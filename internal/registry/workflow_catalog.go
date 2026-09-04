package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/hollis-labs/go-workflow/values"
)

const maxWorkflowCatalogBytes = 64 << 20

type workflowCatalogSnapshot struct {
	Records   []WorkflowRecord             `json:"records"`
	Current   map[string]string            `json:"current,omitempty"`
	Pins      map[string]workflowPin       `json:"pins,omitempty"`
	Published map[string]map[string]string `json:"published,omitempty"`
}

type workflowPin struct {
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

// OpenWorkflowIndex opens or creates an atomically replaced, file-backed
// graph-native catalog. The catalog is an immutable discovery/provenance index;
// movable current aliases never transfer source authority.
func OpenWorkflowIndex(path string) (*WorkflowIndex, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: catalog path is required", ErrInvalidWorkflow)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve catalog path: %w", ErrInvalidWorkflow, err)
	}
	parent := filepath.Dir(absolute)
	info, err := os.Stat(parent)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("%w: catalog parent must be an existing directory", ErrInvalidWorkflow)
	}
	index := NewWorkflowIndex()
	index.path = absolute
	info, err = os.Lstat(absolute)
	if errors.Is(err, os.ErrNotExist) {
		index.mu.Lock()
		_, err = index.persistLocked()
		index.mu.Unlock()
		return index, err
	}
	if err != nil {
		return nil, fmt.Errorf("%w: inspect catalog: %w", ErrInvalidWorkflow, err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxWorkflowCatalogBytes || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%w: catalog must be a bounded private regular file", ErrInvalidWorkflow)
	}
	file, err := os.Open(absolute) // #nosec G304 -- the caller explicitly selects the catalog.
	if err != nil {
		return nil, fmt.Errorf("%w: read catalog: %w", ErrInvalidWorkflow, err)
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() > maxWorkflowCatalogBytes {
		return nil, fmt.Errorf("%w: catalog changed or is not a bounded regular file", ErrInvalidWorkflow)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxWorkflowCatalogBytes+1))
	if err != nil || len(data) > maxWorkflowCatalogBytes {
		return nil, fmt.Errorf("%w: read bounded catalog", ErrInvalidWorkflow)
	}
	if err := index.restore(data); err != nil {
		return nil, err
	}
	return index, nil
}

// PinWorkflow binds direct exposure to one exact immutable source digest.
func (i *WorkflowIndex) PinWorkflow(ctx context.Context, query WorkflowQuery) (WorkflowRecord, error) {
	query, err := canonicalWorkflowQuery(ctx, query)
	if err != nil {
		return WorkflowRecord{}, err
	}
	if query.Version == "" && query.Digest == "" {
		return WorkflowRecord{}, fmt.Errorf("%w: pinning requires an immutable version or digest", ErrInvalidWorkflow)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	resolution, err := i.resolveWorkflowLocked(query)
	if err != nil {
		return WorkflowRecord{}, err
	}
	if !qualifiedWorkflow(resolution.Record) {
		return WorkflowRecord{}, fmt.Errorf("%w: workflow has not passed validation and configured contract tests", ErrInvalidWorkflow)
	}
	prior, hadPrior := i.pins[query.Name]
	i.pins[query.Name] = workflowPin{Version: resolution.Record.Version, Digest: resolution.Record.Digest}
	committed, persistErr := i.persistLocked()
	if persistErr != nil {
		if !committed && hadPrior {
			i.pins[query.Name] = prior
		} else if !committed {
			delete(i.pins, query.Name)
		}
		return WorkflowRecord{}, persistErr
	}
	return cloneWorkflowRecord(resolution.Record), nil
}

func (i *WorkflowIndex) UnpinWorkflow(ctx context.Context, name string) error {
	query, err := canonicalWorkflowQuery(ctx, WorkflowQuery{Name: name})
	if err != nil {
		return err
	}
	resolution, err := i.ResolvePinnedWorkflow(ctx, query.Name)
	if err != nil {
		if errors.Is(err, ErrWorkflowNotFound) {
			return nil
		}
		return err
	}
	return i.UnpinWorkflowExact(ctx, WorkflowQuery{Name: query.Name, Version: resolution.Record.Version, Digest: resolution.Record.Digest})
}

// UnpinWorkflowExact removes only the exact pinned immutable definition. It
// closes the authorization-to-mutation race for callers that authorize the
// resolved version and digest before removing the operational pin.
func (i *WorkflowIndex) UnpinWorkflowExact(ctx context.Context, query WorkflowQuery) error {
	query, err := canonicalWorkflowQuery(ctx, query)
	if err != nil {
		return err
	}
	if query.Version == "" || query.Digest == "" {
		return fmt.Errorf("%w: exact unpin requires version and digest", ErrInvalidWorkflow)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	prior, exists := i.pins[query.Name]
	if !exists {
		return nil
	}
	if prior.Version != query.Version || prior.Digest != query.Digest {
		return fmt.Errorf("%w: pinned workflow changed before unpin", ErrWorkflowConflict)
	}
	delete(i.pins, query.Name)
	committed, persistErr := i.persistLocked()
	if persistErr != nil {
		if !committed {
			i.pins[query.Name] = prior
		}
		return persistErr
	}
	return nil
}

// ResolvePinnedWorkflow resolves only the exact digest selected for direct
// exposure. It never falls back to a current alias.
func (i *WorkflowIndex) ResolvePinnedWorkflow(ctx context.Context, name string) (WorkflowResolution, error) {
	query, err := canonicalWorkflowQuery(ctx, WorkflowQuery{Name: name})
	if err != nil {
		return WorkflowResolution{}, err
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	pin, exists := i.pins[query.Name]
	if !exists {
		return WorkflowResolution{}, fmt.Errorf("%w: %s is not pinned", ErrWorkflowNotFound, query.Name)
	}
	return i.resolveWorkflowLocked(WorkflowQuery{Name: query.Name, Version: pin.Version, Digest: pin.Digest})
}

func (i *WorkflowIndex) PublishWorkflow(ctx context.Context, query WorkflowQuery) (WorkflowRecord, error) {
	query, err := canonicalWorkflowQuery(ctx, query)
	if err != nil {
		return WorkflowRecord{}, err
	}
	if query.Version == "" && query.Digest == "" {
		return WorkflowRecord{}, fmt.Errorf("%w: publication requires an immutable version or digest", ErrInvalidWorkflow)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	resolution, err := i.resolveWorkflowLocked(query)
	if err != nil {
		return WorkflowRecord{}, err
	}
	pin := i.pins[query.Name]
	if !qualifiedWorkflow(resolution.Record) || pin.Version != resolution.Record.Version || pin.Digest != resolution.Record.Digest {
		return WorkflowRecord{}, fmt.Errorf("%w: publication requires passing qualification and an exact pin", ErrInvalidWorkflow)
	}
	byVersion := i.published[query.Name]
	if byVersion == nil {
		byVersion = make(map[string]string)
		i.published[query.Name] = byVersion
	}
	prior, hadPrior := byVersion[resolution.Record.Version]
	byVersion[resolution.Record.Version] = resolution.Record.Digest
	committed, persistErr := i.persistLocked()
	if persistErr != nil {
		if !committed && hadPrior {
			byVersion[resolution.Record.Version] = prior
		} else if !committed {
			delete(byVersion, resolution.Record.Version)
		}
		return WorkflowRecord{}, persistErr
	}
	resolution.Record.Published = true
	return cloneWorkflowRecord(resolution.Record), nil
}

func (i *WorkflowIndex) InspectWorkflow(ctx context.Context, query WorkflowQuery) (WorkflowRecord, error) {
	resolution, err := i.ResolveWorkflow(ctx, query)
	return resolution.Record, err
}

func (i *WorkflowIndex) SearchWorkflows(ctx context.Context, namespace, text string) ([]WorkflowRecord, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalidWorkflow)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	namespace, text = strings.TrimSpace(namespace), strings.ToLower(strings.TrimSpace(text))
	if namespace != "" {
		if err := validateRegistryName("workflow namespace", namespace); err != nil {
			return nil, err
		}
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	var result []WorkflowRecord
	for name, versions := range i.versions {
		for _, record := range versions {
			if namespace != "" && record.Namespace != namespace {
				continue
			}
			if text != "" && !strings.Contains(strings.ToLower(name+" "+record.Version+" "+record.Provenance.Origin), text) {
				continue
			}
			result = append(result, i.recordForRead(record))
		}
	}
	sort.Slice(result, func(a, b int) bool {
		if result[a].Name == result[b].Name {
			return result[a].Version < result[b].Version
		}
		return result[a].Name < result[b].Name
	})
	return result, nil
}

func qualifiedWorkflow(record WorkflowRecord) bool {
	return record.PlanDigest != "" && record.TestsPassed && record.ContractSuiteDigest != "" && record.ContractTestDigest != "" &&
		record.PublisherPrincipal != "" && !record.RegisteredAt.IsZero()
}

func canonicalWorkflowQuery(ctx context.Context, query WorkflowQuery) (WorkflowQuery, error) {
	if ctx == nil {
		return WorkflowQuery{}, fmt.Errorf("%w: context is required", ErrInvalidWorkflow)
	}
	if err := ctx.Err(); err != nil {
		return WorkflowQuery{}, err
	}
	query.Name, query.Version, query.Digest = strings.TrimSpace(query.Name), strings.TrimSpace(query.Version), strings.TrimSpace(query.Digest)
	if err := validateRegistryName("workflow name", query.Name); err != nil {
		return WorkflowQuery{}, err
	}
	if query.Version != "" && (!utf8.ValidString(query.Version) || containsControl(query.Version)) {
		return WorkflowQuery{}, fmt.Errorf("%w: workflow version is invalid", ErrInvalidWorkflow)
	}
	if query.Digest != "" {
		if err := values.ValidateDigest(query.Digest); err != nil {
			return WorkflowQuery{}, fmt.Errorf("%w: workflow digest: %w", ErrInvalidWorkflow, err)
		}
	}
	return query, nil
}

func (i *WorkflowIndex) restore(data []byte) error {
	if len(data) == 0 || len(data) > maxWorkflowCatalogBytes {
		return fmt.Errorf("%w: catalog size is invalid", ErrInvalidWorkflow)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var snapshot workflowCatalogSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return fmt.Errorf("%w: decode catalog: %w", ErrInvalidWorkflow, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: catalog has trailing JSON", ErrInvalidWorkflow)
	}
	versions := make(map[string]map[string]WorkflowRecord)
	for _, input := range snapshot.Records {
		record, err := canonicalWorkflowRecord(input)
		if err != nil {
			return err
		}
		byVersion := versions[record.Name]
		if byVersion == nil {
			byVersion = make(map[string]WorkflowRecord)
			versions[record.Name] = byVersion
		}
		if _, exists := byVersion[record.Version]; exists {
			return fmt.Errorf("%w: duplicate catalog version", ErrWorkflowConflict)
		}
		byVersion[record.Version] = record
	}
	for name, version := range snapshot.Current {
		if versions[name][version].Name == "" {
			return fmt.Errorf("%w: current alias is unresolved", ErrInvalidWorkflow)
		}
	}
	for name, pin := range snapshot.Pins {
		record := versions[name][pin.Version]
		if record.Name == "" || record.Digest != pin.Digest {
			return fmt.Errorf("%w: pin is unresolved", ErrInvalidWorkflow)
		}
	}
	for name, published := range snapshot.Published {
		for version, digest := range published {
			record := versions[name][version]
			if record.Name == "" || record.Digest != digest || !qualifiedWorkflow(record) {
				return fmt.Errorf("%w: publication is unresolved or unqualified", ErrInvalidWorkflow)
			}
		}
	}
	i.versions = versions
	i.current = cloneStringMap(snapshot.Current)
	i.pins = clonePins(snapshot.Pins)
	i.published = clonePublished(snapshot.Published)
	if i.current == nil {
		i.current = make(map[string]string)
	}
	if i.pins == nil {
		i.pins = make(map[string]workflowPin)
	}
	if i.published == nil {
		i.published = make(map[string]map[string]string)
	}
	return nil
}

// persistLocked returns committed=true once atomic rename succeeds. Callers
// must never roll in-memory state back after that point, even when directory
// sync or a test-injected post-commit hook reports a recoverable warning.
func (i *WorkflowIndex) persistLocked() (committed bool, err error) {
	if i.path == "" {
		return true, nil
	}
	records := make([]WorkflowRecord, 0)
	for _, versions := range i.versions {
		for _, record := range versions {
			records = append(records, cloneWorkflowRecord(record))
		}
	}
	sort.Slice(records, func(a, b int) bool {
		if records[a].Name == records[b].Name {
			return records[a].Version < records[b].Version
		}
		return records[a].Name < records[b].Name
	})
	encoded, err := json.Marshal(workflowCatalogSnapshot{
		Records: records, Current: cloneStringMap(i.current), Pins: clonePins(i.pins), Published: clonePublished(i.published),
	})
	if err != nil || len(encoded) > maxWorkflowCatalogBytes {
		return false, fmt.Errorf("%w: encode bounded catalog", ErrInvalidWorkflow)
	}
	dir := filepath.Dir(i.path)
	temporary, err := os.CreateTemp(dir, ".workflow-catalog-*")
	if err != nil {
		return false, fmt.Errorf("persist workflow catalog: %w", err)
	}
	tempName := temporary.Name()
	defer func() { _ = os.Remove(tempName) }()
	err = temporary.Chmod(0o600)
	if err == nil {
		_, err = temporary.Write(encoded)
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		if i.beforeRename != nil {
			err = i.beforeRename()
		}
	}
	if err == nil {
		err = os.Rename(tempName, i.path)
		committed = err == nil
	}
	if err != nil {
		return false, fmt.Errorf("persist workflow catalog: %w", err)
	}
	directory, openErr := os.Open(dir) // #nosec G304 -- dir is derived from the explicitly configured catalog path.
	if openErr == nil {
		openErr = directory.Sync()
		_ = directory.Close()
	}
	if openErr != nil {
		return true, fmt.Errorf("sync workflow catalog directory: %w", openErr)
	}
	if i.afterRename != nil {
		if hookErr := i.afterRename(); hookErr != nil {
			return true, fmt.Errorf("workflow catalog committed with post-commit error: %w", hookErr)
		}
	}
	return true, nil
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func clonePins(input map[string]workflowPin) map[string]workflowPin {
	if input == nil {
		return nil
	}
	result := make(map[string]workflowPin, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func clonePublished(input map[string]map[string]string) map[string]map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]map[string]string, len(input))
	for name, versions := range input {
		result[name] = cloneStringMap(versions)
	}
	return result
}
