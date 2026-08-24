package emit_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/adapters/emit"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

var publishedAt = time.Date(2026, time.August, 24, 20, 0, 0, 0, time.UTC)

func TestRegisterEmitMetadataAndDescription(t *testing.T) {
	registry := stepkind.NewRegistry()
	publisher := newDurablePublisher()
	kind, err := emit.Register(registry, emit.Options{Policy: allowPolicy(), Publisher: publisher})
	if err != nil || kind == nil {
		t.Fatalf("Register = %#v, %v", kind, err)
	}
	_, spec, err := stepkind.Resolve(registry, emit.KindName, emit.KindVersion)
	if err != nil || !reflect.DeepEqual(spec.Effects, graph.EffectSet{graph.EffectMutate}) ||
		spec.Idempotency != graph.IdempotencyKeyed || spec.RetrySafety != stepkind.RetryRequiresIdempotency ||
		spec.Memoization != stepkind.MemoizationDisabled || spec.EmbeddedModeSupported || spec.CanSuspend {
		t.Fatalf("emit spec = %#v, %v", spec, err)
	}
	description, err := kind.DescribeConfig(t.Context(), validConfig())
	if err != nil || description.Destination.Reference != "project/releases" ||
		!reflect.DeepEqual(description.RequiredCapabilities, []string{emit.CapabilityPublish}) {
		t.Fatalf("description = %#v, %v", description, err)
	}
	description.Destination.Attributes["tenant"] = "mutated"
	repeated, _ := kind.DescribeConfig(t.Context(), validConfig())
	if repeated.Destination.Attributes["tenant"] != "project-a" {
		t.Fatalf("description retained caller mutation: %#v", repeated)
	}
}

func TestEmitPublishesTypedEnvelopeWithExactReplayAndConflict(t *testing.T) {
	publisher := newDurablePublisher()
	var authorizations []emit.AuthorizationRequest
	var observations []emit.Observation
	kind, err := emit.New(emit.Options{
		Policy: emit.PolicyFunc(func(_ context.Context, request emit.AuthorizationRequest) error {
			authorizations = append(authorizations, request)
			return nil
		}),
		Publisher: publisher,
		Observer: emit.ObserverFunc(func(_ context.Context, observation emit.Observation) error {
			observations = append(observations, observation)
			observation.EventType = "host-mutated"
			return errors.New("ignored observer failure")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := inline(t, map[string]any{"count": json.Number("9007199254740993"), "status": "ready"}, values.RedactionPrivate)
	prepared := invocation(payload, "emit-key-1")
	first, err := kind.Execute(t.Context(), prepared)
	if err != nil || first.Outcome != stepkind.StepCompleted {
		t.Fatalf("first Execute = %#v, %v", first, err)
	}
	second, err := kind.Execute(t.Context(), prepared)
	if err != nil || second.Outcome != stepkind.StepCompleted {
		t.Fatalf("replay Execute = %#v, %v", second, err)
	}
	firstEnvelope := first.Outputs["envelope"].Inline.(map[string]any)
	secondEnvelope := second.Outputs["envelope"].Inline.(map[string]any)
	if firstEnvelope["id"] != secondEnvelope["id"] || firstEnvelope["replayed"] != false || secondEnvelope["replayed"] != true ||
		firstEnvelope["payload_digest"] != payload.Digest || first.Outputs["envelope"].Redaction != values.RedactionPrivate {
		t.Fatalf("publication outputs = %#v / %#v", firstEnvelope, secondEnvelope)
	}
	if publisher.applied != 1 || publisher.calls != 2 || len(authorizations) != 2 || authorizations[0].PayloadDigest != payload.Digest {
		t.Fatalf("publisher/authorization = %#v / %#v", publisher, authorizations)
	}
	if len(observations) != 4 || observations[0].Phase != emit.ObservationAuthorized || observations[1].Phase != emit.ObservationPublished ||
		observations[2].Phase != emit.ObservationAuthorized || observations[3].Outcome != emit.PublicationReplayed ||
		observations[0].EventType != "release.ready" || observations[0].DestinationDigest == "" || observations[0].PayloadDigest != payload.Digest {
		t.Fatalf("sanitized observations = %#v", observations)
	}
	encodedObservations, _ := json.Marshal(observations)
	if strings.Contains(string(encodedObservations), "project/releases") || strings.Contains(string(encodedObservations), "release:42") || strings.Contains(string(encodedObservations), "project-a") || strings.Contains(string(encodedObservations), "9007199254740993") {
		t.Fatalf("operational observations leaked route or payload: %s", encodedObservations)
	}
	received := publisher.last()
	count := received.Payload.Inline.(map[string]any)["count"]
	if number, ok := count.(json.Number); !ok || number.String() != "9007199254740993" {
		t.Fatalf("exact payload number = %#v", count)
	}

	changed := invocation(inline(t, map[string]any{"count": json.Number("9007199254740994")}, values.RedactionPrivate), "emit-key-1")
	result, err := kind.Execute(t.Context(), changed)
	var execution *stepkind.ExecutionError
	if !reflect.ValueOf(result).IsZero() || !errors.As(err, &execution) || execution.Code != emit.CodePublishFailed ||
		execution.Classification != stepkind.RetryPermanent || !errors.Is(err, emit.ErrPublicationConflict) {
		t.Fatalf("conflicting replay = %#v, %T %v", result, err, err)
	}
}

func TestEmitPolicyPrecedesPublisherAndErrorsAreRedacted(t *testing.T) {
	publisher := newDurablePublisher()
	observerCalls := 0
	const secret = "secret://project/emit#token"
	kind, err := emit.New(emit.Options{
		Policy:    emit.PolicyFunc(func(context.Context, emit.AuthorizationRequest) error { return errors.New("denied " + secret) }),
		Publisher: publisher,
		Observer:  emit.ObserverFunc(func(context.Context, emit.Observation) error { observerCalls++; return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := kind.Execute(t.Context(), invocation(inline(t, "payload", values.RedactionPrivate), "emit-denied"))
	var execution *stepkind.ExecutionError
	if !reflect.ValueOf(result).IsZero() || !errors.As(err, &execution) || execution.Code != emit.CodePolicyDenied || strings.Contains(err.Error(), secret) || publisher.calls != 0 || observerCalls != 0 {
		t.Fatalf("denied Execute = %#v, %T %v; publisher=%#v", result, err, err, publisher)
	}
}

func TestEmitRetryRecoverySecretRefAndDefensiveOwnership(t *testing.T) {
	publisher := newDurablePublisher()
	publisher.failNext = true
	kind, _ := emit.New(emit.Options{Policy: allowPolicy(), Publisher: publisher})
	secretRef, err := values.ParseSecretRef("secret://project/emit#token")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := values.NewSecretRef(secretRef, values.Metadata{
		Producer:  values.Producer{Kind: "input", Reference: "run/input", Output: "payload"},
		MediaType: "application/json", Redaction: values.RedactionSecret, Retention: values.RetentionProject,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared := invocation(payload, "emit-recovery")
	if _, executeErr := kind.Execute(t.Context(), prepared); stepkind.ClassifyError(executeErr) != stepkind.Retryable || strings.Contains(executeErr.Error(), string(secretRef)) {
		t.Fatalf("transient publication = %v", executeErr)
	}

	// A new executor instance models process recovery; publisher-owned durable
	// idempotency remains the source of truth.
	restarted, _ := emit.New(emit.Options{Policy: allowPolicy(), Publisher: publisher})
	completed, err := restarted.Execute(t.Context(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(completed.Outputs)
	if err != nil || strings.Contains(string(encoded), string(secretRef)) || completed.Outputs["envelope"].Redaction != values.RedactionPrivate {
		t.Fatalf("secret publication output = %s, %v", encoded, err)
	}
	stored := publisher.last()
	if stored.Payload.SecretRef == nil || *stored.Payload.SecretRef != secretRef || stored.Payload.Redaction != values.RedactionSecret {
		t.Fatalf("secret envelope lost classification: %#v", stored.Payload)
	}

	attributes := publisher.resultAttributes
	attributes["region"] = "mutated-after-return"
	receipt := completed.Outputs["envelope"].Inline.(map[string]any)["attributes"].(map[string]any)
	if receipt["region"] != "test" {
		t.Fatalf("publisher retained output ownership: %#v", receipt)
	}
}

func TestEmitFailsClosedForBoundsMalformedResultsAndCancellation(t *testing.T) {
	var nilPolicy *emitPolicyStub
	if _, err := emit.New(emit.Options{Policy: nilPolicy, Publisher: newDurablePublisher()}); err == nil {
		t.Fatal("typed-nil policy accepted")
	}
	var nilPublisher *emitPublisherStub
	if _, err := emit.New(emit.Options{Policy: allowPolicy(), Publisher: nilPublisher}); err == nil {
		t.Fatal("typed-nil publisher accepted")
	}

	standard, _ := emit.New(emit.Options{Policy: allowPolicy(), Publisher: newDurablePublisher()})
	invalid := []graph.Config{
		withConfig("unknown", true),
		withConfig("event_type", strings.Repeat("x", 129)),
		withConfig("payload_input", "bad input"),
		withConfig("correlation", strings.Repeat("c", 4097)),
		withDestinationAttributes(33),
		withDestinationReference("https://example.invalid/events"),
		withDestinationReference("project/events?credential=value"),
		withDestinationReference("user@project/events"),
		withDestinationReference("project/events#fragment"),
		withDestinationAttribute("authorization", "safe"),
		withDestinationAttribute("region", "bearer credential"),
	}
	for index, config := range invalid {
		if findings := standard.ValidateConfig(t.Context(), config); len(findings) != 1 || !strings.Contains(findings[0].Message, "config") {
			t.Fatalf("invalid config %d diagnostics = %#v", index, findings)
		}
	}
	first := standard.ValidateConfig(t.Context(), withConfig("unknown", true))
	for range 20 {
		if repeated := standard.ValidateConfig(t.Context(), withConfig("unknown", true)); !reflect.DeepEqual(first, repeated) {
			t.Fatalf("nondeterministic diagnostics: %#v / %#v", first, repeated)
		}
	}
	if _, err := standard.Execute(t.Context(), invocation(inline(t, "payload", values.RedactionPrivate), "")); err == nil {
		t.Fatal("missing runtime idempotency key accepted")
	}

	wrongID := emit.PublisherFunc(func(_ context.Context, envelope emit.Envelope) (emit.PublicationResult, error) {
		return emit.PublicationResult{EnvelopeID: envelope.ID + "x", PublicationID: "publication", Outcome: emit.PublicationApplied, PublishedAt: publishedAt}, nil
	})
	malformed, _ := emit.New(emit.Options{Policy: allowPolicy(), Publisher: wrongID})
	if _, err := malformed.Execute(t.Context(), invocation(inline(t, "payload", values.RedactionPrivate), "wrong-id")); err == nil || !strings.Contains(err.Error(), emit.CodeInvalidResult) {
		t.Fatalf("mismatched envelope receipt = %v", err)
	}
	overAttributes := emit.PublisherFunc(func(_ context.Context, envelope emit.Envelope) (emit.PublicationResult, error) {
		attributes := map[string]string{}
		for index := range 33 {
			attributes["item-"+string(rune('a'+index%26))+strings.Repeat("x", index/26)] = "safe"
		}
		return emit.PublicationResult{EnvelopeID: envelope.ID, PublicationID: "publication", Outcome: emit.PublicationApplied, PublishedAt: publishedAt, Attributes: attributes}, nil
	})
	malformed, _ = emit.New(emit.Options{Policy: allowPolicy(), Publisher: overAttributes})
	if _, err := malformed.Execute(t.Context(), invocation(inline(t, "payload", values.RedactionPrivate), "over-attrs")); err == nil {
		t.Fatal("oversized publisher receipt accepted")
	}
	unsafeReceipt := emit.PublisherFunc(func(_ context.Context, envelope emit.Envelope) (emit.PublicationResult, error) {
		return emit.PublicationResult{EnvelopeID: envelope.ID, PublicationID: "publication", Outcome: emit.PublicationApplied, PublishedAt: publishedAt, Attributes: map[string]string{"endpoint": "https://example.invalid/receipt"}}, nil
	})
	malformed, _ = emit.New(emit.Options{Policy: allowPolicy(), Publisher: unsafeReceipt})
	if _, err := malformed.Execute(t.Context(), invocation(inline(t, "payload", values.RedactionPrivate), "unsafe-receipt")); err == nil {
		t.Fatal("unsafe publisher receipt metadata accepted")
	}

	ctx, cancel := context.WithCancel(context.Background())
	latePolicy, _ := emit.New(emit.Options{
		Policy:    emit.PolicyFunc(func(context.Context, emit.AuthorizationRequest) error { cancel(); return nil }),
		Publisher: newDurablePublisher(),
	})
	if _, err := latePolicy.Execute(ctx, invocation(inline(t, "payload", values.RedactionPrivate), "cancel-policy")); !errors.Is(err, context.Canceled) {
		t.Fatalf("late policy cancellation = %v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	latePublisher, _ := emit.New(emit.Options{
		Policy: allowPolicy(), Publisher: emit.PublisherFunc(func(_ context.Context, envelope emit.Envelope) (emit.PublicationResult, error) {
			cancel()
			return emit.PublicationResult{EnvelopeID: envelope.ID, PublicationID: "publication", Outcome: emit.PublicationApplied, PublishedAt: publishedAt}, nil
		}),
	})
	if _, err := latePublisher.Execute(ctx, invocation(inline(t, "payload", values.RedactionPrivate), "cancel-publisher")); !errors.Is(err, context.Canceled) {
		t.Fatalf("late publisher cancellation = %v", err)
	}
}

func TestEmitConcurrentReplayIsDeterministic(t *testing.T) {
	publisher := newDurablePublisher()
	kind, _ := emit.New(emit.Options{Policy: allowPolicy(), Publisher: publisher})
	prepared := invocation(inline(t, map[string]any{"safe": true}, values.RedactionPrivate), "concurrent-key")
	const workers = 24
	ids := make(chan string, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := kind.Execute(context.Background(), prepared)
			if err != nil {
				ids <- "error:" + err.Error()
				return
			}
			ids <- result.Outputs["envelope"].Inline.(map[string]any)["id"].(string)
		}()
	}
	group.Wait()
	close(ids)
	var expected string
	for id := range ids {
		if expected == "" {
			expected = id
		}
		if id != expected {
			t.Fatalf("concurrent envelope IDs = %q / %q", expected, id)
		}
	}
	if publisher.applied != 1 || publisher.calls != workers {
		t.Fatalf("concurrent publisher = calls %d, applied %d", publisher.calls, publisher.applied)
	}
}

func TestEmitDirectTransientSentinelAndEmptyReceiptMetadata(t *testing.T) {
	transient, err := emit.New(emit.Options{
		Policy: allowPolicy(),
		Publisher: emit.PublisherFunc(func(context.Context, emit.Envelope) (emit.PublicationResult, error) {
			return emit.PublicationResult{}, emit.ErrPublicationTransient
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, executeErr := transient.Execute(t.Context(), invocation(inline(t, "payload", values.RedactionPrivate), "direct-transient")); stepkind.ClassifyError(executeErr) != stepkind.Retryable {
		t.Fatalf("direct transient classification = %v", executeErr)
	}

	withoutMetadata, err := emit.New(emit.Options{
		Policy: allowPolicy(),
		Publisher: emit.PublisherFunc(func(_ context.Context, envelope emit.Envelope) (emit.PublicationResult, error) {
			return emit.PublicationResult{EnvelopeID: envelope.ID, PublicationID: "publication", Outcome: emit.PublicationApplied, PublishedAt: publishedAt}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	completed, executeErr := withoutMetadata.Execute(t.Context(), invocation(inline(t, "payload", values.RedactionPrivate), "empty-metadata"))
	if executeErr != nil {
		t.Fatal(executeErr)
	}
	attributes := completed.Outputs["envelope"].Inline.(map[string]any)["attributes"]
	if reflected := reflect.ValueOf(attributes); reflected.Kind() != reflect.Map || reflected.IsNil() || reflected.Len() != 0 {
		t.Fatalf("empty receipt attributes = %#v", attributes)
	}
}

type durablePublisher struct {
	mu               sync.Mutex
	records          map[string]emit.Envelope
	results          map[string]emit.PublicationResult
	calls            int
	applied          int
	failNext         bool
	resultAttributes map[string]string
}

func newDurablePublisher() *durablePublisher {
	return &durablePublisher{records: map[string]emit.Envelope{}, results: map[string]emit.PublicationResult{}, resultAttributes: map[string]string{"region": "test"}}
}

func (p *durablePublisher) Publish(_ context.Context, envelope emit.Envelope) (emit.PublicationResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.failNext {
		p.failNext = false
		return emit.PublicationResult{}, &emit.PublicationError{Failure: emit.ErrPublicationTransient, Cause: errors.New("raw transport detail")}
	}
	if recorded, found := p.records[envelope.IdempotencyKey]; found {
		if !reflect.DeepEqual(recorded, envelope) {
			return emit.PublicationResult{}, &emit.PublicationError{Failure: emit.ErrPublicationConflict, Cause: errors.New("raw conflicting payload")}
		}
		result := p.results[envelope.IdempotencyKey]
		result.Outcome = emit.PublicationReplayed
		result.Attributes = cloneStrings(p.resultAttributes)
		return result, nil
	}
	p.applied++
	p.records[envelope.IdempotencyKey] = envelope
	result := emit.PublicationResult{
		EnvelopeID: envelope.ID, PublicationID: "publication-" + envelope.ID,
		Outcome: emit.PublicationApplied, PublishedAt: publishedAt, Attributes: cloneStrings(p.resultAttributes),
	}
	p.results[envelope.IdempotencyKey] = result
	return result, nil
}

func (p *durablePublisher) last() emit.Envelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, envelope := range p.records {
		return envelope
	}
	return emit.Envelope{}
}

type emitPolicyStub struct{}

func (*emitPolicyStub) AuthorizeEmit(context.Context, emit.AuthorizationRequest) error { return nil }

type emitPublisherStub struct{}

func (*emitPublisherStub) Publish(context.Context, emit.Envelope) (emit.PublicationResult, error) {
	return emit.PublicationResult{}, nil
}

func allowPolicy() emit.Policy {
	return emit.PolicyFunc(func(context.Context, emit.AuthorizationRequest) error { return nil })
}

func validConfig() graph.Config {
	return graph.Config{
		"destination": map[string]any{"kind": "event-bus", "reference": "project/releases", "attributes": map[string]any{"tenant": "project-a"}},
		"event_type":  "release.ready", "payload_input": "payload", "correlation": "release:42",
	}
}

func withDestinationReference(reference string) graph.Config {
	result := validConfig()
	result["destination"].(map[string]any)["reference"] = reference
	return result
}

func withDestinationAttribute(key, value string) graph.Config {
	result := validConfig()
	result["destination"].(map[string]any)["attributes"] = map[string]any{key: value}
	return result
}

func invocation(payload values.Value, idempotencyKey string) stepkind.PreparedInvocation {
	return stepkind.PreparedInvocation{Invocation: stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-emit", NodeID: "publish-event", Iteration: "item-1", Attempt: 2},
		Config:   validConfig(), Inputs: values.ValueSet{"payload": payload}, IdempotencyKey: idempotencyKey,
	}}
}

func inline(t *testing.T, input any, redaction values.RedactionClass) values.Value {
	t.Helper()
	value, err := values.NewInline(input, values.Metadata{
		Producer:  values.Producer{Kind: "input", Reference: "run/input", Output: "payload"},
		MediaType: "application/json", Redaction: redaction, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func withConfig(key string, value any) graph.Config {
	result := validConfig()
	result[key] = value
	return result
}

func withDestinationAttributes(count int) graph.Config {
	result := validConfig()
	attributes := map[string]any{}
	for index := range count {
		attributes["attr-"+strings.Repeat("x", index+1)] = "value"
	}
	result["destination"].(map[string]any)["attributes"] = attributes
	return result
}

func cloneStrings(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
