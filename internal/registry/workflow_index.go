package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/authoring"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

var (
	ErrWorkflowNotFound = errors.New("graph-native workflow definition not found")
	ErrWorkflowConflict = errors.New("graph-native workflow definition conflict")
	ErrInvalidWorkflow  = errors.New("invalid graph-native workflow definition")
)

// WorkflowRecord is one immutable, exact graph-native registry version. Source
// is the selected workflow source itself; container or publisher digests belong
// in Provenance and never replace Digest.
type WorkflowRecord struct {
	Name                string
	Namespace           string
	Version             string
	Digest              string
	Source              []byte
	SourceFormat        graph.SourceFormat `json:"source_format,omitempty"`
	SourceSchemaID      string             `json:"source_schema_id,omitempty"`
	SourceSchemaVersion string             `json:"source_schema_version,omitempty"`
	Authority           string
	TrustClass          string
	Provenance          graph.Provenance
	PlanDigest          string
	ContractSuiteDigest string
	ContractTestDigest  string
	TestsPassed         bool
	PublisherPrincipal  string
	RegisteredAt        time.Time
	Published           bool
}

// SourceDefinitionID returns the source-local graph identity. Canonical
// records preserve the invariant Name == Namespace + "/" + source ID when a
// namespace is present, and Name == source ID otherwise.
func (r WorkflowRecord) SourceDefinitionID() string {
	if r.Namespace != "" {
		return strings.TrimPrefix(r.Name, r.Namespace+"/")
	}
	return r.Name
}

// WorkflowQuery selects an exact version/digest or the explicitly designated
// current alias. Name is always required so digest lookup cannot cross a
// namespace boundary.
type WorkflowQuery struct {
	Name    string
	Version string
	Digest  string
}

// WorkflowResolution reports whether resolution used a movable current alias.
type WorkflowResolution struct {
	Record  WorkflowRecord
	Movable bool
}

// WorkflowResolver is the graph-native registry port consumed by the Hadron
// definition resolver.
type WorkflowResolver interface {
	ResolveWorkflow(context.Context, WorkflowQuery) (WorkflowResolution, error)
}

// WorkflowIndex is a concurrency-safe graph-native index. An index opened with
// OpenWorkflowIndex durably persists immutable versions, exact pins, aliases,
// and publication state without reusing the legacy blueprint registry model.
type WorkflowIndex struct {
	mu           sync.RWMutex
	versions     map[string]map[string]WorkflowRecord
	current      map[string]string
	pins         map[string]workflowPin
	published    map[string]map[string]string
	path         string
	beforeRename func() error
	afterRename  func() error
}

func NewWorkflowIndex() *WorkflowIndex {
	return &WorkflowIndex{
		versions:  make(map[string]map[string]WorkflowRecord),
		current:   make(map[string]string),
		pins:      make(map[string]workflowPin),
		published: make(map[string]map[string]string),
	}
}

// RegisterWorkflow adds or exactly replays one immutable version. makeCurrent
// explicitly moves the alias without changing any registered version.
func (i *WorkflowIndex) RegisterWorkflow(ctx context.Context, input WorkflowRecord, makeCurrent bool) (WorkflowRecord, error) {
	if ctx == nil {
		return WorkflowRecord{}, fmt.Errorf("%w: context is required", ErrInvalidWorkflow)
	}
	if err := ctx.Err(); err != nil {
		return WorkflowRecord{}, err
	}
	record, err := canonicalWorkflowRecord(input)
	if err != nil {
		return WorkflowRecord{}, err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.versions == nil {
		i.versions = make(map[string]map[string]WorkflowRecord)
	}
	if i.current == nil {
		i.current = make(map[string]string)
	}
	if i.pins == nil {
		i.pins = make(map[string]workflowPin)
	}
	if i.published == nil {
		i.published = make(map[string]map[string]string)
	}
	byVersion := i.versions[record.Name]
	if byVersion == nil {
		byVersion = make(map[string]WorkflowRecord)
		i.versions[record.Name] = byVersion
	}
	if prior, exists := byVersion[record.Version]; exists {
		if !equalWorkflowRegistration(prior, record) {
			return WorkflowRecord{}, fmt.Errorf("%w: %s@%s", ErrWorkflowConflict, record.Name, record.Version)
		}
		priorCurrent, hadCurrent := i.current[record.Name]
		if makeCurrent {
			i.current[record.Name] = record.Version
		}
		committed, persistErr := i.persistLocked()
		if persistErr != nil {
			if !committed && makeCurrent {
				if hadCurrent {
					i.current[record.Name] = priorCurrent
				} else {
					delete(i.current, record.Name)
				}
			}
			return WorkflowRecord{}, persistErr
		}
		return cloneWorkflowRecord(prior), nil
	}
	priorCurrent, hadCurrent := i.current[record.Name]
	byVersion[record.Version] = cloneWorkflowRecord(record)
	if makeCurrent {
		i.current[record.Name] = record.Version
	}
	committed, persistErr := i.persistLocked()
	if persistErr != nil {
		if !committed {
			delete(byVersion, record.Version)
			if len(byVersion) == 0 {
				delete(i.versions, record.Name)
			}
		}
		if !committed && makeCurrent {
			if hadCurrent {
				i.current[record.Name] = priorCurrent
			} else {
				delete(i.current, record.Name)
			}
		}
		return WorkflowRecord{}, persistErr
	}
	return cloneWorkflowRecord(record), nil
}

// RemoveCurrentWorkflowExact removes only the current alias when it still
// identifies the supplied immutable version and source digest. Immutable
// workflow records remain resolvable. An absent alias is an exact replay;
// another current version is a conflict rather than a stale deletion.
func (i *WorkflowIndex) RemoveCurrentWorkflowExact(ctx context.Context, query WorkflowQuery) error {
	query, err := canonicalWorkflowQuery(ctx, query)
	if err != nil {
		return err
	}
	if query.Version == "" || query.Digest == "" {
		return fmt.Errorf("%w: exact current removal requires version and digest", ErrInvalidWorkflow)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	byVersion := i.versions[query.Name]
	record, exists := byVersion[query.Version]
	if !exists {
		return fmt.Errorf("%w: %s@%s", ErrWorkflowNotFound, query.Name, query.Version)
	}
	if record.Digest != query.Digest {
		return fmt.Errorf("%w: version and digest select different source", ErrWorkflowConflict)
	}
	current := i.current[query.Name]
	if current == "" {
		return nil
	}
	if current != query.Version {
		return fmt.Errorf("%w: current workflow changed before removal", ErrWorkflowConflict)
	}
	delete(i.current, query.Name)
	committed, persistErr := i.persistLocked()
	if persistErr != nil {
		if !committed {
			i.current[query.Name] = current
		}
		return persistErr
	}
	return nil
}

func (i *WorkflowIndex) ResolveWorkflow(ctx context.Context, query WorkflowQuery) (WorkflowResolution, error) {
	if ctx == nil {
		return WorkflowResolution{}, fmt.Errorf("%w: context is required", ErrInvalidWorkflow)
	}
	if err := ctx.Err(); err != nil {
		return WorkflowResolution{}, err
	}
	query.Name = strings.TrimSpace(query.Name)
	query.Version = strings.TrimSpace(query.Version)
	query.Digest = strings.TrimSpace(query.Digest)
	if err := validateRegistryName("workflow name", query.Name); err != nil {
		return WorkflowResolution{}, err
	}
	if query.Version != "" && (!utf8.ValidString(query.Version) || containsControl(query.Version)) {
		return WorkflowResolution{}, fmt.Errorf("%w: workflow version is invalid", ErrInvalidWorkflow)
	}
	if query.Digest != "" {
		if err := values.ValidateDigest(query.Digest); err != nil {
			return WorkflowResolution{}, fmt.Errorf("%w: workflow digest: %w", ErrInvalidWorkflow, err)
		}
	}

	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.resolveWorkflowLocked(query)
}

func (i *WorkflowIndex) resolveWorkflowLocked(query WorkflowQuery) (WorkflowResolution, error) {
	byVersion := i.versions[query.Name]
	if byVersion == nil {
		return WorkflowResolution{}, fmt.Errorf("%w: %s", ErrWorkflowNotFound, query.Name)
	}
	movable := query.Version == "" && query.Digest == ""
	version := query.Version
	if movable {
		version = i.current[query.Name]
		if version == "" {
			return WorkflowResolution{}, fmt.Errorf("%w: %s has no current version", ErrWorkflowNotFound, query.Name)
		}
	}
	if version != "" {
		record, exists := byVersion[version]
		if !exists {
			return WorkflowResolution{}, fmt.Errorf("%w: %s@%s", ErrWorkflowNotFound, query.Name, version)
		}
		if query.Digest != "" && record.Digest != query.Digest {
			return WorkflowResolution{}, fmt.Errorf("%w: version and digest select different source", ErrWorkflowConflict)
		}
		return WorkflowResolution{Record: i.recordForRead(record), Movable: movable}, nil
	}

	matches := make([]WorkflowRecord, 0, 1)
	for _, record := range byVersion {
		if record.Digest == query.Digest {
			matches = append(matches, record)
		}
	}
	if len(matches) == 0 {
		return WorkflowResolution{}, fmt.Errorf("%w: %s@%s", ErrWorkflowNotFound, query.Name, query.Digest)
	}
	sort.Slice(matches, func(left, right int) bool { return matches[left].Version < matches[right].Version })
	if len(matches) != 1 {
		return WorkflowResolution{}, fmt.Errorf("%w: digest is registered under multiple versions", ErrWorkflowConflict)
	}
	return WorkflowResolution{Record: i.recordForRead(matches[0])}, nil
}

func (i *WorkflowIndex) recordForRead(input WorkflowRecord) WorkflowRecord {
	result := cloneWorkflowRecord(input)
	result.Published = i.published[input.Name][input.Version] == input.Digest
	return result
}

func canonicalWorkflowRecord(input WorkflowRecord) (WorkflowRecord, error) {
	input.Name = strings.TrimSpace(input.Name)
	input.Version = strings.TrimSpace(input.Version)
	input.Digest = strings.TrimSpace(input.Digest)
	input.Authority = strings.TrimSpace(input.Authority)
	input.TrustClass = strings.TrimSpace(input.TrustClass)
	input.Namespace = strings.TrimSpace(input.Namespace)
	input.PublisherPrincipal = strings.TrimSpace(input.PublisherPrincipal)
	input.SourceSchemaID = strings.TrimSpace(input.SourceSchemaID)
	input.SourceSchemaVersion = strings.TrimSpace(input.SourceSchemaVersion)
	if input.SourceFormat == "" && input.SourceSchemaID == "" && input.SourceSchemaVersion == "" {
		// Catalogs written before W07-T11 contain only graph-native workflow
		// source. The default is format migration, never content inference.
		input.SourceFormat = graph.SourceWorkflow
		input.SourceSchemaID = authoring.WorkflowSourceSchemaID
		input.SourceSchemaVersion = authoring.WorkflowSourceSchemaVersion
	}
	wantSchemaID, wantSchemaVersion, supported := authoring.SourceSchemaFor(input.SourceFormat)
	if !supported || input.SourceSchemaID != wantSchemaID || input.SourceSchemaVersion != wantSchemaVersion {
		return WorkflowRecord{}, fmt.Errorf("%w: workflow source format/schema is unsupported", ErrInvalidWorkflow)
	}
	if input.Published {
		return WorkflowRecord{}, fmt.Errorf("%w: publication is operational catalog state", ErrInvalidWorkflow)
	}
	if err := validateRegistryName("workflow name", input.Name); err != nil {
		return WorkflowRecord{}, err
	}
	for _, field := range []struct{ name, value string }{
		{"workflow version", input.Version},
		{"workflow authority", input.Authority},
		{"workflow trust class", input.TrustClass},
		{"workflow publisher principal", input.PublisherPrincipal},
	} {
		if (field.name != "workflow publisher principal" || field.value != "") &&
			(strings.TrimSpace(field.value) == "" || !utf8.ValidString(field.value) || containsControl(field.value)) {
			return WorkflowRecord{}, fmt.Errorf("%w: %s is required and must not contain control characters", ErrInvalidWorkflow, field.name)
		}
	}
	if input.Namespace != "" {
		if err := validateRegistryName("workflow namespace", input.Namespace); err != nil {
			return WorkflowRecord{}, err
		}
		if !strings.HasPrefix(input.Name, input.Namespace+"/") {
			return WorkflowRecord{}, fmt.Errorf("%w: workflow name is outside its namespace", ErrInvalidWorkflow)
		}
	}
	if err := graph.ValidateID(input.SourceDefinitionID()); err != nil {
		return WorkflowRecord{}, fmt.Errorf("%w: workflow name must be its namespace plus one source-local graph ID: %w", ErrInvalidWorkflow, err)
	}
	if input.PlanDigest != "" {
		if err := values.ValidateDigest(input.PlanDigest); err != nil {
			return WorkflowRecord{}, fmt.Errorf("%w: workflow plan digest: %w", ErrInvalidWorkflow, err)
		}
	}
	if input.ContractTestDigest != "" {
		if err := values.ValidateDigest(input.ContractTestDigest); err != nil {
			return WorkflowRecord{}, fmt.Errorf("%w: workflow contract-test digest: %w", ErrInvalidWorkflow, err)
		}
	}
	if input.ContractSuiteDigest != "" {
		if err := values.ValidateDigest(input.ContractSuiteDigest); err != nil {
			return WorkflowRecord{}, fmt.Errorf("%w: workflow contract-suite digest: %w", ErrInvalidWorkflow, err)
		}
	}
	if input.TestsPassed && (input.ContractTestDigest == "" || input.ContractSuiteDigest == "") {
		return WorkflowRecord{}, fmt.Errorf("%w: passed tests require exact suite and result digests", ErrInvalidWorkflow)
	}
	if !input.RegisteredAt.IsZero() {
		input.RegisteredAt = input.RegisteredAt.UTC()
	}
	if len(input.Source) == 0 {
		return WorkflowRecord{}, fmt.Errorf("%w: workflow source is required", ErrInvalidWorkflow)
	}
	if !utf8.Valid(input.Source) {
		return WorkflowRecord{}, fmt.Errorf("%w: workflow source must contain valid UTF-8", ErrInvalidWorkflow)
	}
	digest := values.SHA256Digest(input.Source)
	if input.Digest == "" {
		input.Digest = digest
	} else if input.Digest != digest {
		return WorkflowRecord{}, fmt.Errorf("%w: workflow digest does not match exact source bytes", ErrWorkflowConflict)
	}
	if input.Provenance.Authority != "" && input.Provenance.Authority != input.Authority {
		return WorkflowRecord{}, fmt.Errorf("%w: provenance authority mismatch", ErrWorkflowConflict)
	}
	if input.Provenance.Digest != "" && input.Provenance.Digest != input.Digest {
		return WorkflowRecord{}, fmt.Errorf("%w: provenance digest must identify selected source bytes", ErrWorkflowConflict)
	}
	for _, field := range []struct{ name, value string }{
		{"provenance origin", input.Provenance.Origin},
		{"provenance locator", input.Provenance.Locator},
		{"provenance revision", input.Provenance.Revision},
	} {
		if (field.name != "provenance revision" && strings.TrimSpace(field.value) == "") ||
			!utf8.ValidString(field.value) || containsControl(field.value) {
			return WorkflowRecord{}, fmt.Errorf("%w: %s is required and must not contain control characters", ErrInvalidWorkflow, field.name)
		}
	}
	if input.Provenance.Revision != "" && input.Provenance.Revision != input.Version {
		return WorkflowRecord{}, fmt.Errorf("%w: provenance revision mismatch", ErrWorkflowConflict)
	}
	input.Provenance.Authority = input.Authority
	input.Provenance.Digest = input.Digest
	input.Provenance.Revision = input.Version
	if input.Provenance.Origin == "" {
		input.Provenance.Origin = "hadron-registry"
	}
	if input.Provenance.Metadata != nil {
		if _, err := values.DigestInline(map[string]any(input.Provenance.Metadata)); err != nil {
			return WorkflowRecord{}, fmt.Errorf("%w: provenance metadata: %w", ErrInvalidWorkflow, err)
		}
	}
	for index, parent := range input.Provenance.Parents {
		for _, field := range []struct{ name, value string }{
			{"authority", parent.Authority}, {"locator", parent.Locator}, {"digest", parent.Digest},
		} {
			if !utf8.ValidString(field.value) || containsControl(field.value) {
				return WorkflowRecord{}, fmt.Errorf("%w: provenance parent[%d] %s is invalid", ErrInvalidWorkflow, index, field.name)
			}
		}
		if parent.Digest != "" {
			if err := values.ValidateDigest(parent.Digest); err != nil {
				return WorkflowRecord{}, fmt.Errorf("%w: provenance parent[%d] digest: %w", ErrInvalidWorkflow, index, err)
			}
		}
	}
	input.Source = bytes.Clone(input.Source)
	cloned, err := cloneWorkflowRecordChecked(input)
	if err != nil {
		return WorkflowRecord{}, fmt.Errorf("%w: provenance: %w", ErrInvalidWorkflow, err)
	}
	return cloned, nil
}

func validateRegistryName(name, value string) error {
	if value == "" || !utf8.ValidString(value) || containsControl(value) || strings.Contains(value, "\\") || strings.HasPrefix(value, "/") {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidWorkflow, name)
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidWorkflow, name)
		}
	}
	return nil
}

// ValidateWorkflowName validates the canonical slash-delimited identity used
// by graph-native catalog queries without reading catalog state.
func ValidateWorkflowName(value string) error {
	return validateRegistryName("workflow name", strings.TrimSpace(value))
}

// ValidateWorkflowNamespace validates the canonical slash-delimited namespace
// used by graph-native catalog searches without reading catalog state.
func ValidateWorkflowNamespace(value string) error {
	return validateRegistryName("workflow namespace", strings.TrimSpace(value))
}

func containsControl(value string) bool {
	return strings.IndexFunc(value, unicode.IsControl) >= 0
}

func cloneWorkflowRecord(input WorkflowRecord) WorkflowRecord {
	cloned, _ := cloneWorkflowRecordChecked(input)
	return cloned
}

func cloneWorkflowRecordChecked(input WorkflowRecord) (WorkflowRecord, error) {
	encoded, err := json.Marshal(input.Provenance)
	if err != nil {
		return WorkflowRecord{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var provenance graph.Provenance
	if err := decoder.Decode(&provenance); err != nil {
		return WorkflowRecord{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return WorkflowRecord{}, errors.New("provenance contains trailing JSON")
	}
	input.Source = bytes.Clone(input.Source)
	input.Provenance = provenance
	return input, nil
}

func equalWorkflowRecord(left, right WorkflowRecord) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

// equalWorkflowRegistration excludes only the catalog-assigned first
// registration timestamp. Exact service retries preserve that original fact;
// every source, provenance, trust, qualification, and publisher field remains
// part of immutable replay identity.
func equalWorkflowRegistration(left, right WorkflowRecord) bool {
	left.RegisteredAt = time.Time{}
	right.RegisteredAt = time.Time{}
	return equalWorkflowRecord(left, right)
}

var _ WorkflowResolver = (*WorkflowIndex)(nil)
