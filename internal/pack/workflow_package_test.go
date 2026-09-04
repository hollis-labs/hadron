package pack

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
)

func TestBuildWorkflowPackageIsDeterministicAndReadable(t *testing.T) {
	source := []byte("workflow:\n  name: package-test\n  version: v1\nsteps: []\n")
	suite := []byte(`{"cases":[],"schema_version":"hadron.workflow-contract/v1"}`)
	suiteDigest := values.SHA256Digest(suite)
	reportDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	report := []byte(fmt.Sprintf(`{"attestation":"test","digest":%q,"passed":true,"plan_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"hadron.workflow-contract/v1","suite_digest":%q}`, reportDigest, suiteDigest))
	request := WorkflowPackageRequest{
		Source: source,
		Suite:  suite,
		Report: report,
		Manifest: WorkflowPackageManifest{
			Name: "team/package-test", Namespace: "team", Version: "v1",
			SourceDigest:        values.SHA256Digest(source),
			PlanDigest:          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ContractSuiteDigest: suiteDigest,
			ContractTestDigest:  reportDigest,
			Authority:           "project", TrustClass: "project-owned", PublisherPrincipal: "principal:test",
			RegisteredAt: time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC),
			Provenance:   graph.Provenance{Authority: "project", Origin: "workflow-source", Locator: "/project/workflow.yaml"},
		},
	}
	first, err := BuildWorkflowPackage(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildWorkflowPackage(request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes, second.Bytes) || first.Digest != second.Digest {
		t.Fatal("equal package requests were not byte-stable")
	}
	selected, err := ReadWorkflowSource(first.Bytes, "workflow.yaml", WorkflowArchiveLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(selected.Source, source) || selected.SourceDigest != request.Manifest.SourceDigest {
		t.Fatalf("selected source = %#v", selected)
	}
	first.Bytes[0] ^= 0xff
	if bytes.Equal(first.Bytes, second.Bytes) {
		t.Fatal("package result aliases prior result bytes")
	}
}

func TestBuildWorkflowPackageRejectsDigestAndJSONDrift(t *testing.T) {
	base := WorkflowPackageRequest{
		Source: []byte("workflow:\n  name: invalid\n"), Suite: []byte(`{"cases":[]}`), Report: []byte(`{"passed":true}`),
		Manifest: WorkflowPackageManifest{
			Name: "team/invalid", Namespace: "team", Version: "v1",
			SourceDigest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			PlanDigest:          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ContractSuiteDigest: values.SHA256Digest([]byte(`{"cases":[]}`)),
			ContractTestDigest:  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Authority:           "project", TrustClass: "project-owned", PublisherPrincipal: "principal:test", RegisteredAt: time.Now(),
		},
	}
	if _, err := BuildWorkflowPackage(base); err == nil {
		t.Fatal("BuildWorkflowPackage accepted source digest drift")
	}
	base.Manifest.SourceDigest = values.SHA256Digest(base.Source)
	base.Report = []byte(`{"passed":true} {}`)
	if _, err := BuildWorkflowPackage(base); err == nil {
		t.Fatal("BuildWorkflowPackage accepted trailing JSON")
	}
}
