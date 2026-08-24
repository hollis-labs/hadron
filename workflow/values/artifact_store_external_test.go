package values_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/values"
)

func TestCaptureValueInlineAndOpaquePromotion(t *testing.T) {
	t.Parallel()
	store := &captureStore{}
	request := captureRequest(values.RedactionPrivate, values.RetentionRun)

	inline, err := values.CaptureValue(t.Context(), store, request,
		strings.NewReader(`{"number":9007199254740993}`),
		values.CapturePolicy{Mode: values.CaptureJSON, InlineLimit: 128})
	if err != nil {
		t.Fatalf("CaptureValue inline: %v", err)
	}
	if inline.Type != values.TypeObject || store.puts != 0 {
		t.Fatalf("inline capture = %#v, puts=%d", inline, store.puts)
	}
	number := inline.Inline.(map[string]any)["number"]
	if number != json.Number("9007199254740993") {
		t.Fatalf("number = %#v (%T)", number, number)
	}

	opaque := []byte(`{"not":"fully parsed because it exceeds the cap"}`)
	promoted, err := values.CaptureValue(t.Context(), store, request, bytes.NewReader(opaque),
		values.CapturePolicy{Mode: values.CaptureJSON, InlineLimit: 8})
	if err != nil {
		t.Fatalf("CaptureValue promoted: %v", err)
	}
	if promoted.Type != values.TypeArtifact || promoted.Artifact == nil || !bytes.Equal(store.content, opaque) {
		t.Fatalf("promoted capture = %#v, bytes=%q", promoted, store.content)
	}
	if promoted.Artifact.Redaction != request.Metadata.Redaction ||
		promoted.Artifact.Retention != request.Metadata.Retention ||
		promoted.Artifact.Producer != request.Metadata.Producer {
		t.Fatalf("promoted metadata changed: %#v", promoted.Artifact)
	}
}

func TestCaptureValueRejectsMalformedSmallJSONAndInvalidPolicy(t *testing.T) {
	t.Parallel()
	store := &captureStore{}
	request := captureRequest(values.RedactionPrivate, values.RetentionRun)
	_, err := values.CaptureValue(t.Context(), store, request, strings.NewReader(`{"unterminated"`),
		values.CapturePolicy{Mode: values.CaptureJSON, InlineLimit: 128})
	if !errors.Is(err, values.ErrArtifactInvalid) || store.puts != 0 {
		t.Fatalf("malformed error=%v puts=%d", err, store.puts)
	}
	for _, limit := range []int64{0, -1, values.MaximumInlineLimit + 1} {
		_, err := values.CaptureValue(t.Context(), store, request, strings.NewReader("value"),
			values.CapturePolicy{Mode: values.CaptureText, InlineLimit: limit})
		if !errors.Is(err, values.ErrArtifactSizeLimit) {
			t.Fatalf("limit %d error=%v", limit, err)
		}
	}
	request.MaxBytes = math.MaxInt64
	if err := request.Validate(); !errors.Is(err, values.ErrArtifactInvalid) {
		t.Fatalf("MaxInt64 stream limit error=%v", err)
	}
}

func TestCaptureValueAppliesStreamLimitAndExpectedDigestInline(t *testing.T) {
	t.Parallel()
	store := &captureStore{}
	request := captureRequest(values.RedactionPrivate, values.RetentionRun)
	request.MaxBytes = 4
	if _, err := values.CaptureValue(t.Context(), store, request, strings.NewReader("value"),
		values.CapturePolicy{Mode: values.CaptureText, InlineLimit: 16}); !errors.Is(err, values.ErrArtifactSizeLimit) {
		t.Fatalf("inline stream limit error=%v", err)
	}
	request.MaxBytes = 16
	request.ExpectedDigest = values.SHA256Digest([]byte("different"))
	if _, err := values.CaptureValue(t.Context(), store, request, strings.NewReader("value"),
		values.CapturePolicy{Mode: values.CaptureText, InlineLimit: 16}); !errors.Is(err, values.ErrArtifactDigest) {
		t.Fatalf("inline expected digest error=%v", err)
	}
	if store.puts != 0 {
		t.Fatalf("invalid inline capture wrote %d artifacts", store.puts)
	}
}

func TestCaptureValueAllowsSmallRetentionNoneButCannotPromoteIt(t *testing.T) {
	t.Parallel()
	store := &captureStore{}
	request := captureRequest(values.RedactionPrivate, values.RetentionNone)
	value, err := values.CaptureValue(t.Context(), store, request, strings.NewReader("small"),
		values.CapturePolicy{Mode: values.CaptureText, InlineLimit: 8})
	if err != nil || value.Type != values.TypeString || value.Retention != values.RetentionNone {
		t.Fatalf("ephemeral capture=%#v err=%v", value, err)
	}
	_, err = values.CaptureValue(t.Context(), store, request, strings.NewReader("too large"),
		values.CapturePolicy{Mode: values.CaptureText, InlineLimit: 4})
	if !errors.Is(err, values.ErrArtifactRetention) {
		t.Fatalf("ephemeral promotion error=%v", err)
	}
}

func TestCaptureValueAlwaysPromotesSecretAndPreservesMetadata(t *testing.T) {
	t.Parallel()
	store := &captureStore{}
	request := captureRequest(values.RedactionSecret, values.RetentionProject)
	request.Owner = values.ArtifactOwner{Scope: values.ArtifactOwnerProject, ID: "project-1"}
	secret := []byte(`{"credential":"material"}`)
	value, err := values.CaptureValue(t.Context(), store, request, bytes.NewReader(secret),
		values.CapturePolicy{Mode: values.CaptureJSON, InlineLimit: values.DefaultInlineLimit})
	if err != nil {
		t.Fatalf("CaptureValue secret: %v", err)
	}
	if value.Type != values.TypeArtifact || store.puts != 1 || !bytes.Equal(store.content, secret) {
		t.Fatalf("secret capture=%#v puts=%d", value, store.puts)
	}
	if value.Artifact.Producer != request.Metadata.Producer ||
		value.Artifact.Redaction != values.RedactionSecret ||
		value.Artifact.Retention != values.RetentionProject {
		t.Fatalf("secret metadata changed: %#v", value.Artifact)
	}
}

func TestCaptureValueBuffersOnlyCapPlusOne(t *testing.T) {
	t.Parallel()
	store := &captureStore{}
	probe := &readProbe{remaining: 4096}
	_, err := values.CaptureValue(t.Context(), store, captureRequest(values.RedactionPrivate, values.RetentionRun), probe,
		values.CapturePolicy{Mode: values.CaptureText, InlineLimit: 32})
	if err != nil {
		t.Fatalf("CaptureValue: %v", err)
	}
	if probe.firstRead > 33 {
		t.Fatalf("decision buffer requested %d bytes, want <=33", probe.firstRead)
	}
	if len(store.content) != 4096 {
		t.Fatalf("stored bytes=%d", len(store.content))
	}
}

func TestCaptureValueObservesCancellationDuringPrefixRead(t *testing.T) {
	t.Parallel()
	store := &captureStore{}
	ctx, cancel := context.WithCancel(context.Background())
	reader := &captureBlockingReader{entered: make(chan struct{}), release: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := values.CaptureValue(ctx, store, captureRequest(values.RedactionPrivate, values.RetentionRun), reader,
			values.CapturePolicy{Mode: values.CaptureText, InlineLimit: 128})
		result <- err
	}()
	<-reader.entered
	cancel()
	close(reader.release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("CaptureValue cancellation error=%v", err)
	}
	if store.puts != 0 {
		t.Fatalf("canceled capture wrote %d artifacts", store.puts)
	}
}

func TestCaptureValueSnapshotsExpectedSizeBeforeReading(t *testing.T) {
	t.Parallel()
	store := &captureStore{}
	expected := int64(len("value"))
	request := captureRequest(values.RedactionPrivate, values.RetentionRun)
	request.ExpectedSize = &expected
	reader := &captureExpectedMutatingReader{reader: strings.NewReader("value"), expected: &expected}
	value, err := values.CaptureValue(t.Context(), store, request, reader,
		values.CapturePolicy{Mode: values.CaptureText, InlineLimit: 16})
	if err != nil || value.Inline != "value" || expected == int64(len("value")) {
		t.Fatalf("capture=%#v expected=%d err=%v", value, expected, err)
	}
}

func TestVerifyingArtifactReaderEOFAndIdempotentClose(t *testing.T) {
	t.Parallel()
	content := []byte("verified content")
	metadata := externalMetadata(content, values.RedactionPrivate)
	reader, err := values.NewVerifyingArtifactReader(metadata, io.NopCloser(bytes.NewReader(content)))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, content) || !reader.Verified() {
		t.Fatalf("ReadAll=%q err=%v verified=%t", got, err, reader.Verified())
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestVerifyingArtifactReaderMismatchAndEarlyClose(t *testing.T) {
	t.Parallel()
	metadata := externalMetadata([]byte("expected"), values.RedactionSecret)
	mismatch, err := values.NewVerifyingArtifactReader(metadata, io.NopCloser(strings.NewReader("tampered")))
	if err != nil {
		t.Fatal(err)
	}
	if _, readErr := io.ReadAll(mismatch); !errors.Is(readErr, values.ErrArtifactDigest) {
		t.Fatalf("mismatch error=%v", readErr)
	}

	early, err := values.NewVerifyingArtifactReader(metadata, io.NopCloser(strings.NewReader("expected")))
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	if _, err := early.Read(buffer); err != nil {
		t.Fatal(err)
	}
	first := early.Close()
	second := early.Close()
	if !errors.Is(first, values.ErrArtifactDigest) || !errors.Is(first, values.ErrArtifactUnverified) || first.Error() != second.Error() {
		t.Fatalf("early close first=%v second=%v", first, second)
	}
}

func TestArtifactErrorIsStructuredSearchableAndRefSafe(t *testing.T) {
	t.Parallel()
	ref := externalMetadata([]byte("secret"), values.RedactionSecret).Ref
	raw := errors.New("open /private/root: secret://authority/credential")
	err := values.NewArtifactError(values.ArtifactOperation("attacker secret"), values.ArtifactFailure("attacker detail"), &ref, raw)
	if !errors.Is(err, values.ErrArtifactInvalid) || !errors.Is(err, raw) {
		t.Fatalf("errors.Is failed: %v", err)
	}
	for _, forbidden := range []string{"/private/root", "secret://", ref.URI, "attacker"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("Error leaked %q: %s", forbidden, err)
		}
	}
	diagnostic := err.Diagnostic()
	if validationErr := diagnostic.Validate(); validationErr != nil {
		t.Fatalf("diagnostic invalid: %v", validationErr)
	}
	encoded, _ := json.Marshal(diagnostic)
	for _, forbidden := range []string{"/private/root", "secret://", ref.URI, "attacker"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("diagnostic leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestArtifactFailureDiagnosticsUseStableCodes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		failure values.ArtifactFailure
		cause   error
		code    string
	}{
		{values.ArtifactFailureAuthority, values.ErrArtifactAuthority, "HADR-ARTIFACT-002"},
		{values.ArtifactFailureUnauthorized, values.ErrArtifactUnauthorized, "HADR-ARTIFACT-003"},
		{values.ArtifactFailureDigest, values.ErrArtifactDigest, "HADR-ARTIFACT-005"},
		{values.ArtifactFailureExpired, values.ErrArtifactExpired, "HADR-ARTIFACT-006"},
	}
	for _, test := range tests {
		test := test
		t.Run(string(test.failure), func(t *testing.T) {
			t.Parallel()
			err := values.NewArtifactError(values.ArtifactOperationOpen, test.failure, nil, test.cause)
			diagnostic := err.Diagnostic()
			if string(diagnostic.Code) != test.code || diagnostic.Validate() != nil || !errors.Is(err, test.cause) {
				t.Fatalf("diagnostic=%#v error=%v", diagnostic, err)
			}
		})
	}
}

func TestArtifactCleanupResultClosedVocabulary(t *testing.T) {
	t.Parallel()
	valid := []values.ArtifactCleanupResult{
		{Outcome: values.ArtifactCleanupDeleted, DeletedCount: 2},
		{Outcome: values.ArtifactCleanupAlreadyAbsent},
		{Outcome: values.ArtifactCleanupNotStored},
		{Outcome: values.ArtifactCleanupPreservedExternal},
	}
	for _, result := range valid {
		if err := result.Validate(); err != nil {
			t.Fatalf("valid result %#v: %v", result, err)
		}
	}
	invalid := []values.ArtifactCleanupResult{
		{},
		{Outcome: "removed", DeletedCount: 1},
		{Outcome: values.ArtifactCleanupDeleted},
		{Outcome: values.ArtifactCleanupAlreadyAbsent, DeletedCount: 1},
	}
	for _, result := range invalid {
		if err := result.Validate(); !errors.Is(err, values.ErrArtifactInvalid) {
			t.Fatalf("invalid result %#v error=%v", result, err)
		}
	}
}

func TestArtifactOwnerFailuresPreserveCallingOperation(t *testing.T) {
	t.Parallel()
	metadata := externalMetadata([]byte("content"), values.RedactionPrivate)
	metadata.Owner.ID = ""
	var artifactErr *values.ArtifactError
	if err := metadata.Validate(); !errors.As(err, &artifactErr) || artifactErr.Operation() != values.ArtifactOperationStat {
		t.Fatalf("metadata error=%v artifact_error=%#v", err, artifactErr)
	}
	cleanup := values.ArtifactCleanupRequest{
		Access: captureRequest(values.RedactionPrivate, values.RetentionRun).Access,
		Kind:   values.ArtifactCleanupRun, Owner: values.ArtifactOwner{Scope: values.ArtifactOwnerRun},
	}
	artifactErr = nil
	if err := cleanup.Validate(); !errors.As(err, &artifactErr) || artifactErr.Operation() != values.ArtifactOperationCleanup {
		t.Fatalf("cleanup error=%v artifact_error=%#v", err, artifactErr)
	}
}

func TestArtifactOwnerAndAccessRejectUnstableTransportIdentities(t *testing.T) {
	t.Parallel()
	invalid := []string{string([]byte{0xff}), "owner\nidentity", "owner\tidentity", " owner", "owner "}
	for _, identity := range invalid {
		owner := values.ArtifactOwner{Scope: values.ArtifactOwnerRun, ID: identity}
		if err := owner.Validate(); !errors.Is(err, values.ErrArtifactInvalid) {
			t.Errorf("owner %q error=%v", identity, err)
		}
	}
	base := captureRequest(values.RedactionPrivate, values.RetentionRun).Access
	accesses := []values.ArtifactAccess{
		{Principal: string([]byte{0xff}), At: base.At},
		{Principal: "worker\nidentity", At: base.At},
		{Principal: base.Principal, RunID: "run\tidentity", At: base.At},
		{Principal: base.Principal, ProjectID: string([]byte{0xfe}), At: base.At},
	}
	for _, access := range accesses {
		if err := access.Validate(values.ArtifactOperationOpen); !errors.Is(err, values.ErrArtifactUnauthorized) {
			t.Errorf("access %#v error=%v", access, err)
		}
	}
}

type captureStore struct {
	puts    int
	content []byte
}

func (s *captureStore) Put(_ context.Context, request values.ArtifactPutRequest, source io.Reader) (values.ArtifactMetadata, error) {
	if err := request.Validate(); err != nil {
		return values.ArtifactMetadata{}, err
	}
	content, err := io.ReadAll(source)
	if err != nil {
		return values.ArtifactMetadata{}, err
	}
	s.puts++
	s.content = content
	ref := values.ArtifactRef{
		Store: request.Store, URI: "artifact://local/run/blob", Digest: values.SHA256Digest(content),
		MediaType: request.Metadata.MediaType, SizeBytes: int64(len(content)), Producer: request.Metadata.Producer,
		Redaction: request.Metadata.Redaction, Retention: request.Metadata.Retention,
	}
	return values.ArtifactMetadata{Ref: ref, Owner: request.Owner, CreatedAt: request.CreatedAt, ExpiresAt: request.ExpiresAt}, nil
}

func (*captureStore) Open(context.Context, values.ArtifactAccess, values.ArtifactRef) (values.ArtifactReadCloser, error) {
	return nil, errors.New("unused")
}
func (*captureStore) Stat(context.Context, values.ArtifactAccess, values.ArtifactRef) (values.ArtifactMetadata, error) {
	return values.ArtifactMetadata{}, errors.New("unused")
}
func (*captureStore) Delete(context.Context, values.ArtifactDeleteRequest) (values.ArtifactCleanupResult, error) {
	return values.ArtifactCleanupResult{}, errors.New("unused")
}
func (*captureStore) Cleanup(context.Context, values.ArtifactCleanupRequest) (values.ArtifactCleanupResult, error) {
	return values.ArtifactCleanupResult{}, errors.New("unused")
}

func captureRequest(redaction values.RedactionClass, retention values.RetentionClass) values.ArtifactPutRequest {
	return values.ArtifactPutRequest{
		Store: "local", Owner: values.ArtifactOwner{Scope: values.ArtifactOwnerRun, ID: "run-1"},
		Metadata: values.Metadata{
			Producer:  values.Producer{Kind: "node_output", Reference: "invocation-1", Output: "stdout"},
			MediaType: "application/json", Redaction: redaction, Retention: retention,
		},
		MaxBytes: 2 << 20, CreatedAt: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
		Access: values.ArtifactAccess{Principal: "worker-1", RunID: "run-1", ProjectID: "project-1", At: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)},
	}
}

func externalMetadata(content []byte, redaction values.RedactionClass) values.ArtifactMetadata {
	ref := values.ArtifactRef{
		Store: "external", URI: "artifact://external/vault/blob", Digest: values.SHA256Digest(content),
		MediaType: "application/octet-stream", SizeBytes: int64(len(content)),
		Producer:  values.Producer{Kind: "external", Reference: "producer-1"},
		Redaction: redaction, Retention: values.RetentionExternal,
	}
	return values.ArtifactMetadata{
		Ref: ref, Owner: values.ArtifactOwner{Scope: values.ArtifactOwnerProject, ID: "project-1"},
		CreatedAt: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
	}
}

type readProbe struct {
	remaining int
	firstRead int
	reads     int
}

type captureBlockingReader struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	served  bool
}

func (r *captureBlockingReader) Read(buffer []byte) (int, error) {
	if r.served {
		return 0, io.EOF
	}
	r.once.Do(func() { close(r.entered) })
	<-r.release
	r.served = true
	copy(buffer, "value")
	return len("value"), nil
}

type captureExpectedMutatingReader struct {
	reader   io.Reader
	expected *int64
	once     sync.Once
}

func (r *captureExpectedMutatingReader) Read(buffer []byte) (int, error) {
	r.once.Do(func() { *r.expected++ })
	return r.reader.Read(buffer)
}

func (r *readProbe) Read(buffer []byte) (int, error) {
	r.reads++
	if r.reads == 1 {
		r.firstRead = len(buffer)
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(buffer)
	if n > r.remaining {
		n = r.remaining
	}
	for index := 0; index < n; index++ {
		buffer[index] = 'x'
	}
	r.remaining -= n
	return n, nil
}

var _ values.ArtifactStore = (*captureStore)(nil)
