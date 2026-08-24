package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/values"
)

func TestStoreStreamsPutOpenStatAndPreservesMetadata(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	authorizer := &recordingAuthorizer{}
	store := newTestStore(t, root, authorizer, nil)
	content := bytes.Repeat([]byte("streamed-artifact-"), 140_000)
	request := testPutRequest(values.RetentionRun, values.RedactionSecret, "run-sensitive")
	wantSize := int64(len(content))
	request.ExpectedSize = &wantSize
	request.ExpectedDigest = values.SHA256Digest(content)
	request.MaxBytes = wantSize + 1

	metadata, err := store.Put(t.Context(), request, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if metadata.Ref.Digest != request.ExpectedDigest || metadata.Ref.SizeBytes != wantSize ||
		metadata.Ref.Producer != request.Metadata.Producer ||
		metadata.Ref.Redaction != request.Metadata.Redaction ||
		metadata.Ref.Retention != request.Metadata.Retention || metadata.Owner != request.Owner {
		t.Fatalf("metadata changed: %#v", metadata)
	}
	if strings.Contains(metadata.Ref.URI, request.Owner.ID) || strings.Contains(metadata.Ref.URI, root) {
		t.Fatalf("URI leaks owner/path: %q", metadata.Ref.URI)
	}

	stat, err := store.Stat(t.Context(), request.Access, metadata.Ref)
	if err != nil || !reflect.DeepEqual(stat, metadata) {
		t.Fatalf("Stat=%#v err=%v, want %#v", stat, err, metadata)
	}
	reader, err := store.Open(t.Context(), request.Access, metadata.Ref)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, content) || !reader.Verified() {
		t.Fatalf("ReadAll bytes=%d err=%v verified=%t", len(got), err, reader.Verified())
	}
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatalf("second Close: %v", closeErr)
	}

	locator, err := parseLocalRef(metadata.Ref)
	if err != nil {
		t.Fatal(err)
	}
	directory := store.artifactDirectory(locator)
	assertMode(t, directory, 0o700)
	assertMode(t, filepath.Join(directory, payloadName), 0o600)
	assertMode(t, filepath.Join(directory, manifestName), 0o600)

	replayed, err := store.Put(t.Context(), request, bytes.NewReader(content))
	if err != nil || replayed.Ref != metadata.Ref {
		t.Fatalf("idempotent Put=%#v err=%v", replayed, err)
	}
	changed := request
	changed.Metadata.Redaction = values.RedactionPrivate
	changedMetadata, err := store.Put(t.Context(), changed, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if changedMetadata.Ref.URI == metadata.Ref.URI {
		t.Fatal("classification change aliased immutable artifact identity")
	}

	operations := authorizer.operations()
	if countOperation(operations, values.ArtifactOperationPut) != 3 ||
		countOperation(operations, values.ArtifactOperationStat) != 2 ||
		countOperation(operations, values.ArtifactOperationOpen) != 2 {
		t.Fatalf("authorization sequence=%v", operations)
	}
}

func TestStoreRejectsSizeAndDigestMismatchWithoutPartialArtifact(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store := newTestStore(t, root, &recordingAuthorizer{}, nil)
	content := []byte("content beyond cap")

	sizeRequest := testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-size")
	sizeRequest.MaxBytes = int64(len(content) - 1)
	if _, err := store.Put(t.Context(), sizeRequest, bytes.NewReader(content)); !errors.Is(err, values.ErrArtifactSizeLimit) {
		t.Fatalf("size error=%v", err)
	}
	digestRequest := testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-digest")
	digestRequest.ExpectedDigest = values.SHA256Digest([]byte("different"))
	if _, err := store.Put(t.Context(), digestRequest, bytes.NewReader(content)); !errors.Is(err, values.ErrArtifactDigest) {
		t.Fatalf("digest error=%v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "staging"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging entries=%v err=%v", entries, err)
	}
}

func TestPutCancellationDuringBlockedReadRemovesStaging(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store := newTestStore(t, root, &recordingAuthorizer{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	reader := &blockingReader{entered: make(chan struct{}), release: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := store.Put(ctx, testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-cancel"), reader)
		result <- err
	}()
	<-reader.entered
	cancel()
	close(reader.release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Put error=%v", err)
	}
	entries, err := os.ReadDir(filepath.Join(store.root, "staging"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("canceled Put staging=%v err=%v", entries, err)
	}
}

func TestPutSnapshotsExpectedSizeBeforeReading(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, t.TempDir(), &recordingAuthorizer{}, nil)
	content := []byte("snapshot")
	expected := int64(len(content))
	request := testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-snapshot")
	request.ExpectedSize = &expected
	reader := &expectedSizeMutatingReader{reader: bytes.NewReader(content), expected: &expected}
	metadata, err := store.Put(t.Context(), request, reader)
	if err != nil {
		t.Fatalf("Put after caller pointer mutation: %v", err)
	}
	if metadata.Ref.SizeBytes != int64(len(content)) || expected == int64(len(content)) {
		t.Fatalf("metadata size=%d caller expected=%d", metadata.Ref.SizeBytes, expected)
	}
}

func TestStoreOpenDetectsSameSizeTamperingAndExpiry(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, t.TempDir(), &recordingAuthorizer{}, nil)
	request := testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-tamper")
	request.ExpiresAt = request.CreatedAt.Add(time.Hour)
	content := []byte("original")
	metadata, err := store.Put(t.Context(), request, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	locator, _ := parseLocalRef(metadata.Ref)
	payload := filepath.Join(store.artifactDirectory(locator), payloadName)
	if err := os.WriteFile(payload, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(t.Context(), request.Access, metadata.Ref); !errors.Is(err, values.ErrArtifactDigest) {
		t.Fatalf("tamper was not rejected before bytes were released: %v", err)
	}

	expiredAccess := request.Access
	expiredAccess.At = request.ExpiresAt
	if _, err := store.Stat(t.Context(), expiredAccess, metadata.Ref); !errors.Is(err, values.ErrArtifactExpired) {
		t.Fatalf("expired Stat error=%v", err)
	}
	if _, err := store.Open(t.Context(), expiredAccess, metadata.Ref); !errors.Is(err, values.ErrArtifactExpired) {
		t.Fatalf("expired Open error=%v", err)
	}
}

func TestPreAuthorizationDenialIsNotExistenceOracle(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	allowed := newTestStore(t, root, &recordingAuthorizer{}, nil)
	request := testPutRequest(values.RetentionRun, values.RedactionSecret, "run-auth")
	existing, err := allowed.Put(t.Context(), request, strings.NewReader("secret material"))
	if err != nil {
		t.Fatal(err)
	}

	deniedAuthorizer := &recordingAuthorizer{deny: errors.New("policy detail for /private/root")}
	delegate := &fakeExternalStore{metadata: externalMetadata([]byte("external"), values.RedactionSecret)}
	denied := newTestStore(t, root, deniedAuthorizer, map[string]values.ArtifactStore{"vault": delegate})
	missing := existing.Ref
	missing.URI = strings.TrimSuffix(missing.URI, strings.Split(missing.URI, "/")[5]) + strings.Repeat("a", 64)
	for _, ref := range []values.ArtifactRef{existing.Ref, missing} {
		_, err := denied.Open(t.Context(), request.Access, ref)
		if !errors.Is(err, values.ErrArtifactUnauthorized) || err.Error() != "artifact open failed: unauthorized" {
			t.Fatalf("local denial=%v", err)
		}
	}
	external := delegate.metadata.Ref
	unknown := external
	unknown.Store = "unknown"
	for _, ref := range []values.ArtifactRef{external, unknown} {
		_, err := denied.Open(t.Context(), request.Access, ref)
		if !errors.Is(err, values.ErrArtifactUnauthorized) || err.Error() != "artifact open failed: unauthorized" {
			t.Fatalf("external denial=%v", err)
		}
	}
	if delegate.openCalls != 0 || delegate.statCalls != 0 {
		t.Fatalf("delegate observed denied lookup: %#v", delegate)
	}
}

func TestAuthorizerReceivesDefensiveReferenceAndOwnerCopies(t *testing.T) {
	t.Parallel()
	authorizer := values.ArtifactAuthorizerFunc(func(_ context.Context, request values.ArtifactAuthorization) error {
		if request.Ref != nil {
			request.Ref.URI = "artifact://hadron-local/run/changed/changed"
		}
		if request.Owner != nil {
			request.Owner.ID = "changed-owner"
		}
		return nil
	})
	store := newTestStore(t, t.TempDir(), authorizer, nil)
	request := testPutRequest(values.RetentionRun, values.RedactionPrivate, "original-owner")
	metadata, err := store.Put(t.Context(), request, strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Owner != request.Owner || strings.Contains(metadata.Ref.URI, hashOwner("changed-owner")) {
		t.Fatalf("authorizer mutated metadata: %#v", metadata)
	}
	if _, err := store.Stat(t.Context(), request.Access, metadata.Ref); err != nil {
		t.Fatalf("authorizer mutated resolved ref: %v", err)
	}
}

func TestCanonicalPathAndSymlinkChecks(t *testing.T) {
	t.Parallel()
	authorizer := &recordingAuthorizer{}
	store := newTestStore(t, t.TempDir(), authorizer, nil)
	request := testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-path")
	metadata, err := store.Put(t.Context(), request, strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	baseCalls := len(authorizer.operations())
	malformed := []string{
		strings.Replace(metadata.Ref.URI, "/run/", "/../", 1),
		metadata.Ref.URI + "?token=secret",
		strings.Replace(metadata.Ref.URI, "hadron-local", "HADRON-LOCAL", 1),
		strings.Replace(metadata.Ref.URI, "/run/", "/%72un/", 1),
	}
	for _, uri := range malformed {
		ref := metadata.Ref
		ref.URI = uri
		if _, statErr := store.Stat(t.Context(), request.Access, ref); !errors.Is(statErr, values.ErrArtifactInvalid) {
			t.Fatalf("URI %q error=%v", uri, statErr)
		}
	}
	if got := len(authorizer.operations()); got != baseCalls {
		t.Fatalf("malformed refs reached authorization: calls=%d want=%d", got, baseCalls)
	}

	locator, _ := parseLocalRef(metadata.Ref)
	payload := filepath.Join(store.artifactDirectory(locator), payloadName)
	realPayload := payload + ".real"
	if renameErr := os.Rename(payload, realPayload); renameErr != nil {
		t.Fatal(renameErr)
	}
	if symlinkErr := os.Symlink(realPayload, payload); symlinkErr != nil {
		t.Fatal(symlinkErr)
	}
	_, err = store.Stat(t.Context(), request.Access, metadata.Ref)
	if !errors.Is(err, values.ErrArtifactInvalid) || strings.Contains(err.Error(), store.root) {
		t.Fatalf("symlink error=%v", err)
	}
}

func TestPostConstructionRootChainReplacementFailsClosed(t *testing.T) {
	t.Parallel()
	for _, chain := range []string{"objects", "staging"} {
		chain := chain
		t.Run(chain, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			store := newTestStore(t, root, &recordingAuthorizer{}, nil)
			original := filepath.Join(root, chain)
			moved := original + ".moved"
			if err := os.Rename(original, moved); err != nil {
				t.Fatal(err)
			}
			redirect := t.TempDir()
			if err := os.Symlink(redirect, original); err != nil {
				t.Fatal(err)
			}
			request := testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-root-swap")
			_, err := store.Put(t.Context(), request, strings.NewReader("content"))
			if !errors.Is(err, values.ErrArtifactInvalid) || strings.Contains(err.Error(), root) {
				t.Fatalf("replacement error=%v", err)
			}
			entries, readErr := os.ReadDir(redirect)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("redirect target was touched: entries=%v err=%v", entries, readErr)
			}
		})
	}
}

func TestNewRejectsSymlinkRoot(t *testing.T) {
	t.Parallel()
	actual := t.TempDir()
	link := filepath.Join(t.TempDir(), "artifact-root")
	if err := os.Symlink(actual, link); err != nil {
		t.Fatal(err)
	}
	if _, err := New(link, &recordingAuthorizer{}, nil); !errors.Is(err, values.ErrArtifactInvalid) {
		t.Fatalf("New symlink root error=%v", err)
	}
}

func TestNewClassifiesMissingRootAndAuthorizer(t *testing.T) {
	t.Parallel()
	if _, err := New("  ", &recordingAuthorizer{}, nil); !errors.Is(err, values.ErrArtifactInvalid) || errors.Is(err, values.ErrArtifactAuthority) {
		t.Fatalf("blank root error=%v", err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(root, nil, nil); !errors.Is(err, values.ErrArtifactAuthority) {
		t.Fatalf("nil authorizer error=%v", err)
	}
}

func TestNewRejectsIntermediateSymlinkComponentBeforeCreatingRoot(t *testing.T) {
	t.Parallel()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	realDirectory := filepath.Join(base, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(realDirectory, link); err != nil {
		t.Fatal(err)
	}
	configured := filepath.Join(link, "nested", "artifacts")
	if _, err := New(configured, &recordingAuthorizer{}, nil); !errors.Is(err, values.ErrArtifactInvalid) {
		t.Fatalf("New intermediate symlink error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(realDirectory, "nested")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("New wrote through intermediate symlink: %v", err)
	}
}

func TestPublicOperationsRejectNilAndCanceledContextsBeforeAuthorization(t *testing.T) {
	t.Parallel()
	authorizer := &recordingAuthorizer{}
	store := newTestStore(t, t.TempDir(), authorizer, nil)
	request := testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-context")
	metadata, err := store.Put(t.Context(), request, strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	baselineCalls := len(authorizer.operations())
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	contexts := []struct {
		name string
		ctx  context.Context
		want error
	}{
		{name: "nil", ctx: nil, want: values.ErrArtifactInvalid},
		{name: "canceled", ctx: canceled, want: context.Canceled},
	}
	for _, test := range contexts {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.Put(test.ctx, request, strings.NewReader("payload")); !errors.Is(err, test.want) {
				t.Errorf("Put error=%v", err)
			}
			if _, err := store.Open(test.ctx, request.Access, metadata.Ref); !errors.Is(err, test.want) {
				t.Errorf("Open error=%v", err)
			}
			if _, err := store.Stat(test.ctx, request.Access, metadata.Ref); !errors.Is(err, test.want) {
				t.Errorf("Stat error=%v", err)
			}
			if _, err := store.Delete(test.ctx, values.ArtifactDeleteRequest{Access: request.Access, Ref: metadata.Ref}); !errors.Is(err, test.want) {
				t.Errorf("Delete error=%v", err)
			}
			if _, err := store.Cleanup(test.ctx, values.ArtifactCleanupRequest{Access: request.Access, Kind: values.ArtifactCleanupNone}); !errors.Is(err, test.want) {
				t.Errorf("Cleanup error=%v", err)
			}
		})
	}
	if got := len(authorizer.operations()); got != baselineCalls {
		t.Fatalf("invalid contexts reached authorization: calls=%d want=%d", got, baselineCalls)
	}
}

func TestManifestSymlinkIsRejected(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, t.TempDir(), &recordingAuthorizer{}, nil)
	request := testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-manifest")
	metadata, err := store.Put(t.Context(), request, strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	locator, _ := parseLocalRef(metadata.Ref)
	manifest := filepath.Join(store.artifactDirectory(locator), manifestName)
	realManifest := manifest + ".real"
	if err := os.Rename(manifest, realManifest); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realManifest, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stat(t.Context(), request.Access, metadata.Ref); !errors.Is(err, values.ErrArtifactInvalid) {
		t.Fatalf("manifest symlink error=%v", err)
	}
}

func TestCleanupRunProjectExpiryPartialsNoneAndExternal(t *testing.T) {
	t.Parallel()
	external := &fakeExternalStore{metadata: externalMetadata([]byte("outside"), values.RedactionSecret)}
	store := newTestStore(t, t.TempDir(), &recordingAuthorizer{}, map[string]values.ArtifactStore{"vault": external})
	runRequest := testPutRequest(values.RetentionRun, values.RedactionSecret, "run-clean")
	runMetadata, err := store.Put(t.Context(), runRequest, strings.NewReader("secret"))
	if err != nil {
		t.Fatal(err)
	}
	projectRequest := testPutRequest(values.RetentionProject, values.RedactionPrivate, "project-clean")
	projectMetadata, err := store.Put(t.Context(), projectRequest, strings.NewReader("project"))
	if err != nil {
		t.Fatal(err)
	}

	runCleanup := values.ArtifactCleanupRequest{
		Access: runRequest.Access, Kind: values.ArtifactCleanupRun, Owner: runRequest.Owner,
	}
	result, err := store.Cleanup(t.Context(), runCleanup)
	if err != nil || result.Outcome != values.ArtifactCleanupDeleted || result.DeletedCount != 1 || result.Validate() != nil {
		t.Fatalf("run cleanup=%#v err=%v", result, err)
	}
	if strings.Contains(mustJSON(t, result), runMetadata.Ref.Digest) {
		t.Fatal("secret cleanup result exposed digest")
	}
	repeated, err := store.Cleanup(t.Context(), runCleanup)
	if err != nil || repeated.Outcome != values.ArtifactCleanupAlreadyAbsent || repeated.Validate() != nil {
		t.Fatalf("repeat cleanup=%#v err=%v", repeated, err)
	}
	if _, statErr := store.Stat(t.Context(), projectRequest.Access, projectMetadata.Ref); statErr != nil {
		t.Fatalf("run cleanup deleted project artifact: %v", statErr)
	}

	none, err := store.Cleanup(t.Context(), values.ArtifactCleanupRequest{
		Access: runRequest.Access, Kind: values.ArtifactCleanupNone,
	})
	if err != nil || none.Outcome != values.ArtifactCleanupNotStored || none.Validate() != nil {
		t.Fatalf("none cleanup=%#v err=%v", none, err)
	}
	externalRef := external.metadata.Ref
	preserved, err := store.Cleanup(t.Context(), values.ArtifactCleanupRequest{
		Access: runRequest.Access, Kind: values.ArtifactCleanupExternal, Ref: &externalRef,
	})
	if err != nil || preserved.Outcome != values.ArtifactCleanupPreservedExternal || preserved.Validate() != nil {
		t.Fatalf("external cleanup=%#v err=%v", preserved, err)
	}
	deleted, err := store.Delete(t.Context(), values.ArtifactDeleteRequest{Access: runRequest.Access, Ref: externalRef})
	if err != nil || deleted.Outcome != values.ArtifactCleanupPreservedExternal || external.deleteCalls != 0 {
		t.Fatalf("external delete=%#v err=%v delegate deletes=%d", deleted, err, external.deleteCalls)
	}

	expiringRequest := testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-expiring")
	expiringRequest.ExpiresAt = expiringRequest.CreatedAt.Add(time.Minute)
	if _, putErr := store.Put(t.Context(), expiringRequest, strings.NewReader("expiring")); putErr != nil {
		t.Fatal(putErr)
	}
	expired, err := store.Cleanup(t.Context(), values.ArtifactCleanupRequest{
		Access: expiringRequest.Access, Kind: values.ArtifactCleanupExpired, Before: expiringRequest.ExpiresAt,
	})
	if err != nil || expired.Outcome != values.ArtifactCleanupDeleted || expired.DeletedCount != 1 {
		t.Fatalf("expiry cleanup=%#v err=%v", expired, err)
	}

	partial := filepath.Join(store.root, "staging", "partial-crash")
	if mkdirErr := os.Mkdir(partial, 0o700); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	old := runRequest.CreatedAt.Add(-time.Hour)
	if chtimesErr := os.Chtimes(partial, old, old); chtimesErr != nil {
		t.Fatal(chtimesErr)
	}
	partials, err := store.Cleanup(t.Context(), values.ArtifactCleanupRequest{
		Access: runRequest.Access, Kind: values.ArtifactCleanupPartials, Before: runRequest.CreatedAt,
	})
	if err != nil || partials.Outcome != values.ArtifactCleanupDeleted || partials.DeletedCount != 1 {
		t.Fatalf("partial cleanup=%#v err=%v", partials, err)
	}
}

func TestCleanupCancellationBeforeRemovalLeavesArtifact(t *testing.T) {
	t.Parallel()
	var cancel context.CancelFunc
	authorizer := values.ArtifactAuthorizerFunc(func(_ context.Context, request values.ArtifactAuthorization) error {
		if request.Operation == values.ArtifactOperationCleanup && request.Ref != nil && cancel != nil {
			cancel()
		}
		return nil
	})
	store := newTestStore(t, t.TempDir(), authorizer, nil)
	request := testPutRequest(values.RetentionRun, values.RedactionPrivate, "run-cleanup-cancel")
	metadata, err := store.Put(t.Context(), request, strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancelContext := context.WithCancel(context.Background())
	cancel = cancelContext
	_, err = store.Cleanup(ctx, values.ArtifactCleanupRequest{
		Access: request.Access, Kind: values.ArtifactCleanupRun, Owner: request.Owner,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup cancellation error=%v", err)
	}
	cancel = nil
	if _, err := store.Stat(t.Context(), request.Access, metadata.Ref); err != nil {
		t.Fatalf("canceled cleanup removed artifact: %v", err)
	}
}

func TestExternalPassthroughRequiresRouterAndDelegateAuthorization(t *testing.T) {
	t.Parallel()
	content := []byte("external content")
	delegate := &fakeExternalStore{metadata: externalMetadata(content, values.RedactionPrivate), content: content}
	routerAuthorizer := &recordingAuthorizer{}
	store := newTestStore(t, t.TempDir(), routerAuthorizer, map[string]values.ArtifactStore{"vault": delegate})
	access := testAccess()

	metadata, err := store.Stat(t.Context(), access, delegate.metadata.Ref)
	if err != nil || metadata.Ref != delegate.metadata.Ref || delegate.statCalls != 1 || delegate.authCalls != 1 {
		t.Fatalf("Stat=%#v err=%v delegate=%#v", metadata, err, delegate)
	}
	reader, err := store.Open(t.Context(), access, delegate.metadata.Ref)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("external read=%q err=%v", got, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if delegate.openCalls != 1 || delegate.authCalls != 2 {
		t.Fatalf("delegate calls=%#v", delegate)
	}
	if countOperation(routerAuthorizer.operations(), values.ArtifactOperationStat) != 2 ||
		countOperation(routerAuthorizer.operations(), values.ArtifactOperationOpen) != 2 {
		t.Fatalf("router authorization=%v", routerAuthorizer.operations())
	}
}

func TestExternalDelegateErrorsAreRenderedSafely(t *testing.T) {
	t.Parallel()
	raw := errors.New("external failure secret://vault/credential at /private/root")
	delegate := &fakeExternalStore{
		metadata: externalMetadata([]byte("external"), values.RedactionSecret),
		openErr:  raw,
	}
	store := newTestStore(t, t.TempDir(), &recordingAuthorizer{}, map[string]values.ArtifactStore{"vault": delegate})
	_, err := store.Open(t.Context(), testAccess(), delegate.metadata.Ref)
	if !errors.Is(err, raw) || !errors.Is(err, values.ErrArtifactInvalid) {
		t.Fatalf("wrapped delegate error=%v", err)
	}
	for _, forbidden := range []string{"secret://", "/private/root", delegate.metadata.Ref.URI} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("delegate error leaked %q: %v", forbidden, err)
		}
	}
}

func TestConcurrentIdenticalPutConverges(t *testing.T) {
	store := newTestStore(t, t.TempDir(), &recordingAuthorizer{}, nil)
	request := testPutRequest(values.RetentionProject, values.RedactionPrivate, "project-concurrent")
	content := bytes.Repeat([]byte("concurrent"), 10_000)
	const workers = 12
	refs := make(chan values.ArtifactRef, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			metadata, err := store.Put(t.Context(), request, bytes.NewReader(content))
			if err != nil {
				errs <- err
				return
			}
			refs <- metadata.Ref
		}()
	}
	group.Wait()
	close(refs)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Put: %v", err)
	}
	var first values.ArtifactRef
	for ref := range refs {
		if first.Store == "" {
			first = ref
		} else if ref != first {
			t.Fatalf("refs differ: %#v != %#v", ref, first)
		}
	}
}

func TestLocalPutRejectsNoneAndExternalRetention(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, t.TempDir(), &recordingAuthorizer{}, nil)
	for _, retention := range []values.RetentionClass{values.RetentionNone, values.RetentionExternal} {
		request := testPutRequest(retention, values.RedactionPrivate, "owner")
		if _, err := store.Put(t.Context(), request, strings.NewReader("content")); !errors.Is(err, values.ErrArtifactRetention) {
			t.Fatalf("retention %q error=%v", retention, err)
		}
	}
}

type recordingAuthorizer struct {
	mu    sync.Mutex
	deny  error
	calls []values.ArtifactAuthorization
}

func (a *recordingAuthorizer) AuthorizeArtifact(_ context.Context, request values.ArtifactAuthorization) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, request)
	return a.deny
}

func (a *recordingAuthorizer) operations() []values.ArtifactOperation {
	a.mu.Lock()
	defer a.mu.Unlock()
	operations := make([]values.ArtifactOperation, len(a.calls))
	for index, call := range a.calls {
		operations[index] = call.Operation
	}
	return operations
}

func countOperation(operations []values.ArtifactOperation, operation values.ArtifactOperation) int {
	count := 0
	for _, candidate := range operations {
		if candidate == operation {
			count++
		}
	}
	return count
}

type fakeExternalStore struct {
	metadata    values.ArtifactMetadata
	content     []byte
	authCalls   int
	openCalls   int
	statCalls   int
	deleteCalls int
	openErr     error
}

func (s *fakeExternalStore) authorize(operation values.ArtifactOperation, access values.ArtifactAccess, ref values.ArtifactRef) error {
	s.authCalls++
	if err := access.Validate(operation); err != nil {
		return err
	}
	if ref != s.metadata.Ref {
		return values.NewArtifactError(operation, values.ArtifactFailureNotFound, &ref, values.ErrArtifactNotFound)
	}
	return nil
}

func (s *fakeExternalStore) Put(context.Context, values.ArtifactPutRequest, io.Reader) (values.ArtifactMetadata, error) {
	return values.ArtifactMetadata{}, values.ErrArtifactRetention
}
func (s *fakeExternalStore) Open(_ context.Context, access values.ArtifactAccess, ref values.ArtifactRef) (values.ArtifactReadCloser, error) {
	s.openCalls++
	if err := s.authorize(values.ArtifactOperationOpen, access, ref); err != nil {
		return nil, err
	}
	if s.openErr != nil {
		return nil, s.openErr
	}
	return values.NewVerifyingArtifactReader(s.metadata, io.NopCloser(bytes.NewReader(s.content)))
}
func (s *fakeExternalStore) Stat(_ context.Context, access values.ArtifactAccess, ref values.ArtifactRef) (values.ArtifactMetadata, error) {
	s.statCalls++
	if err := s.authorize(values.ArtifactOperationStat, access, ref); err != nil {
		return values.ArtifactMetadata{}, err
	}
	return s.metadata, nil
}
func (s *fakeExternalStore) Delete(context.Context, values.ArtifactDeleteRequest) (values.ArtifactCleanupResult, error) {
	s.deleteCalls++
	return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupDeleted, DeletedCount: 1}, nil
}
func (s *fakeExternalStore) Cleanup(context.Context, values.ArtifactCleanupRequest) (values.ArtifactCleanupResult, error) {
	return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupDeleted, DeletedCount: 1}, nil
}

func newTestStore(t *testing.T, root string, authorizer values.ArtifactAuthorizer, external map[string]values.ArtifactStore) *Store {
	t.Helper()
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("canonicalize test root: %v", err)
	}
	store, err := New(canonicalRoot, authorizer, external)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

type blockingReader struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	served  bool
}

func (r *blockingReader) Read(buffer []byte) (int, error) {
	if r.served {
		return 0, io.EOF
	}
	r.once.Do(func() { close(r.entered) })
	<-r.release
	r.served = true
	copy(buffer, "blocked data")
	return len("blocked data"), nil
}

type expectedSizeMutatingReader struct {
	reader   io.Reader
	expected *int64
	once     sync.Once
}

func (r *expectedSizeMutatingReader) Read(buffer []byte) (int, error) {
	r.once.Do(func() { *r.expected++ })
	return r.reader.Read(buffer)
}

func testAccess() values.ArtifactAccess {
	return values.ArtifactAccess{
		Principal: "worker-1", RunID: "run-1", ProjectID: "project-1",
		At: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
	}
}

func testPutRequest(retention values.RetentionClass, redaction values.RedactionClass, ownerID string) values.ArtifactPutRequest {
	scope := values.ArtifactOwnerRun
	if retention == values.RetentionProject || retention == values.RetentionExternal {
		scope = values.ArtifactOwnerProject
	}
	return values.ArtifactPutRequest{
		Store: LocalAuthority,
		Owner: values.ArtifactOwner{Scope: scope, ID: ownerID},
		Metadata: values.Metadata{
			Producer:  values.Producer{Kind: "node_output", Reference: "invocation-1", Output: "result"},
			MediaType: "application/octet-stream", Redaction: redaction, Retention: retention,
		},
		MaxBytes: 8 << 20, CreatedAt: testAccess().At, Access: testAccess(),
	}
}

func externalMetadata(content []byte, redaction values.RedactionClass) values.ArtifactMetadata {
	ref := values.ArtifactRef{
		Store: "vault", URI: "s3://Bucket/Project%20Blob?versionId=A", Digest: values.SHA256Digest(content),
		MediaType: "application/octet-stream", SizeBytes: int64(len(content)),
		Producer:  values.Producer{Kind: "external", Reference: "vault-object-1"},
		Redaction: redaction, Retention: values.RetentionExternal,
	}
	return values.ArtifactMetadata{
		Ref: ref, Owner: values.ArtifactOwner{Scope: values.ArtifactOwnerProject, ID: "project-1"}, CreatedAt: testAccess().At,
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode=%o want=%o", filepath.Base(path), got, want)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

var _ values.ArtifactStore = (*fakeExternalStore)(nil)
