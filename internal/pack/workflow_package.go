package pack

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
)

const maxBuiltWorkflowPackageBytes = 64 << 20

// WorkflowPackageManifest records the immutable identities needed to inspect
// a workflow package without making the package authoritative for source.
type WorkflowPackageManifest struct {
	SchemaVersion       string           `json:"schema_version"`
	Name                string           `json:"name"`
	Namespace           string           `json:"namespace"`
	Version             string           `json:"version"`
	SourceDigest        string           `json:"source_digest"`
	PlanDigest          string           `json:"plan_digest"`
	ContractSuiteDigest string           `json:"contract_suite_digest"`
	ContractTestDigest  string           `json:"contract_test_digest"`
	Authority           string           `json:"authority"`
	TrustClass          string           `json:"trust_class"`
	PublisherPrincipal  string           `json:"publisher_principal"`
	RegisteredAt        time.Time        `json:"registered_at"`
	Provenance          graph.Provenance `json:"provenance"`
}

// WorkflowPackageRequest supplies already validated immutable source, the
// editable canonical suite, and its signed execution report.
type WorkflowPackageRequest struct {
	Manifest WorkflowPackageManifest
	Source   []byte
	Suite    []byte
	Report   []byte
}

// WorkflowPackage is a deterministic tar.gz artifact. Equal requests produce
// byte-identical packages and digests; no wall-clock timestamp enters headers.
type WorkflowPackage struct {
	Bytes  []byte
	Digest string
}

// BuildWorkflowPackage creates a bounded graph-native workflow package.
func BuildWorkflowPackage(request WorkflowPackageRequest) (WorkflowPackage, error) {
	manifest, source, suite, report, err := canonicalPackageRequest(request)
	if err != nil {
		return WorkflowPackage{}, err
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return WorkflowPackage{}, fmt.Errorf("%w: encode manifest: %w", ErrInvalidWorkflowArchive, err)
	}
	entries := []struct {
		name string
		data []byte
	}{
		{name: "contract-report.json", data: report},
		{name: "contract-suite.json", data: suite},
		{name: "manifest.json", data: manifestJSON},
		{name: "workflow.yaml", data: source},
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

	var output bytes.Buffer
	gzipWriter, err := gzip.NewWriterLevel(&output, gzip.BestCompression)
	if err != nil {
		return WorkflowPackage{}, err
	}
	gzipWriter.Header = gzip.Header{OS: 255}
	tarWriter := tar.NewWriter(gzipWriter)
	for _, entry := range entries {
		header := &tar.Header{
			Name: entry.name, Mode: 0o600, Size: int64(len(entry.data)),
			ModTime: time.Unix(0, 0).UTC(), AccessTime: time.Time{}, ChangeTime: time.Time{},
			Typeflag: tar.TypeReg, Format: tar.FormatUSTAR,
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return WorkflowPackage{}, fmt.Errorf("%w: write %s header: %w", ErrInvalidWorkflowArchive, entry.name, err)
		}
		if _, err := tarWriter.Write(entry.data); err != nil {
			return WorkflowPackage{}, fmt.Errorf("%w: write %s: %w", ErrInvalidWorkflowArchive, entry.name, err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		return WorkflowPackage{}, fmt.Errorf("%w: close tar: %w", ErrInvalidWorkflowArchive, err)
	}
	if err := gzipWriter.Close(); err != nil {
		return WorkflowPackage{}, fmt.Errorf("%w: close gzip: %w", ErrInvalidWorkflowArchive, err)
	}
	if output.Len() > maxBuiltWorkflowPackageBytes {
		return WorkflowPackage{}, fmt.Errorf("%w: built package exceeds %d bytes", ErrInvalidWorkflowArchive, maxBuiltWorkflowPackageBytes)
	}
	contents := bytes.Clone(output.Bytes())
	return WorkflowPackage{Bytes: contents, Digest: values.SHA256Digest(contents)}, nil
}

func canonicalPackageRequest(request WorkflowPackageRequest) (WorkflowPackageManifest, []byte, []byte, []byte, error) {
	manifest := request.Manifest
	manifest.SchemaVersion = strings.TrimSpace(manifest.SchemaVersion)
	if manifest.SchemaVersion == "" {
		manifest.SchemaVersion = "hadron.workflow-package/v1"
	}
	for _, field := range []struct{ name, value string }{
		{"name", manifest.Name}, {"namespace", manifest.Namespace}, {"version", manifest.Version},
		{"authority", manifest.Authority}, {"trust class", manifest.TrustClass},
		{"publisher principal", manifest.PublisherPrincipal},
	} {
		if strings.TrimSpace(field.value) == "" {
			return WorkflowPackageManifest{}, nil, nil, nil, fmt.Errorf("%w: manifest %s is required", ErrInvalidWorkflowArchive, field.name)
		}
	}
	if manifest.RegisteredAt.IsZero() {
		return WorkflowPackageManifest{}, nil, nil, nil, fmt.Errorf("%w: manifest registration time is required", ErrInvalidWorkflowArchive)
	}
	manifest.RegisteredAt = manifest.RegisteredAt.UTC()
	if len(request.Source) == 0 || len(request.Suite) == 0 || len(request.Report) == 0 ||
		len(request.Source)+len(request.Suite)+len(request.Report) > maxBuiltWorkflowPackageBytes {
		return WorkflowPackageManifest{}, nil, nil, nil, fmt.Errorf("%w: bounded source, suite, and report are required", ErrInvalidWorkflowArchive)
	}
	if manifest.SourceDigest != values.SHA256Digest(request.Source) {
		return WorkflowPackageManifest{}, nil, nil, nil, fmt.Errorf("%w: source digest mismatch", ErrInvalidWorkflowArchive)
	}
	for _, digest := range []struct{ name, value string }{
		{"plan", manifest.PlanDigest}, {"contract-suite", manifest.ContractSuiteDigest}, {"contract-test", manifest.ContractTestDigest},
	} {
		if err := values.ValidateDigest(digest.value); err != nil {
			return WorkflowPackageManifest{}, nil, nil, nil, fmt.Errorf("%w: %s digest: %w", ErrInvalidWorkflowArchive, digest.name, err)
		}
	}
	if values.SHA256Digest(request.Suite) != manifest.ContractSuiteDigest {
		return WorkflowPackageManifest{}, nil, nil, nil, fmt.Errorf("%w: contract suite digest mismatch", ErrInvalidWorkflowArchive)
	}
	for name, data := range map[string][]byte{"contract suite": request.Suite, "contract report": request.Report} {
		if err := requireSingleJSONObject(data); err != nil {
			return WorkflowPackageManifest{}, nil, nil, nil, fmt.Errorf("%w: %s: %w", ErrInvalidWorkflowArchive, name, err)
		}
	}
	var suiteIdentity struct {
		SchemaVersion string `json:"schema_version"`
	}
	if err := decodeSingleJSON(request.Suite, &suiteIdentity); err != nil || suiteIdentity.SchemaVersion != "hadron.workflow-contract/v1" {
		return WorkflowPackageManifest{}, nil, nil, nil, fmt.Errorf("%w: contract suite identity is invalid", ErrInvalidWorkflowArchive)
	}
	var reportIdentity struct {
		SchemaVersion string `json:"schema_version"`
		PlanDigest    string `json:"plan_digest"`
		SuiteDigest   string `json:"suite_digest"`
		Digest        string `json:"digest"`
		Passed        bool   `json:"passed"`
		Attestation   string `json:"attestation"`
	}
	if err := decodeSingleJSON(request.Report, &reportIdentity); err != nil ||
		reportIdentity.SchemaVersion != suiteIdentity.SchemaVersion || !reportIdentity.Passed ||
		reportIdentity.PlanDigest != manifest.PlanDigest || reportIdentity.SuiteDigest != manifest.ContractSuiteDigest ||
		reportIdentity.Digest != manifest.ContractTestDigest || strings.TrimSpace(reportIdentity.Attestation) == "" {
		return WorkflowPackageManifest{}, nil, nil, nil, fmt.Errorf("%w: contract report identity is invalid", ErrInvalidWorkflowArchive)
	}
	return manifest, bytes.Clone(request.Source), bytes.Clone(request.Suite), bytes.Clone(request.Report), nil
}

func requireSingleJSONObject(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	if value == nil {
		return errors.New("JSON object is required")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON document")
		}
		return err
	}
	return nil
}

func decodeSingleJSON(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON document")
		}
		return err
	}
	return nil
}
