package waitadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

const maximumWait = 365 * 24 * time.Hour

type baseExecutor struct {
	authority AuthorityResolver
	callbacks CallbackIssuer
	now       func() time.Time
}

func newBase(options Options) (baseExecutor, error) {
	if nilInterface(options.Authority) {
		return baseExecutor{}, fmt.Errorf("wait authority resolver is required")
	}
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return baseExecutor{authority: options.Authority, callbacks: options.Callbacks, now: now}, nil
}

func (e baseExecutor) authorize(ctx context.Context, identity stepkind.InvocationIdentity, source Source, correlation string) (workflowwait.ResponderAuthority, error) {
	request := AuthorityRequest{Identity: identity, Source: cloneSource(source), Correlation: correlation}
	authority, err := e.authority.ResolveWaitAuthority(ctx, request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return workflowwait.ResponderAuthority{}, contextErr
		}
		return workflowwait.ResponderAuthority{}, executionError(CodeAuthorityFailed, "wait responder authority resolution failed", stepkind.RetryPermanent, err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return workflowwait.ResponderAuthority{}, contextErr
	}
	authority.Attributes = cloneStringMap(authority.Attributes)
	if source.Kind == SourceEvent {
		if authority.Attributes == nil {
			authority.Attributes = map[string]string{}
		}
		authority.Attributes["wait_source"] = string(SourceEvent)
		authority.Attributes["event_type"] = source.Reference
		if eventSource := source.Attributes["source"]; eventSource != "" {
			authority.Attributes["event_source"] = eventSource
		}
	}
	if err := authority.Validate(); err != nil {
		return workflowwait.ResponderAuthority{}, executionError(CodeAuthorityFailed, "wait responder authority is invalid", stepkind.RetryPermanent, err)
	}
	return authority, nil
}

func openRecord(kind workflowwait.Kind, wake workflowwait.WakeSource, correlation string, deadline time.Time, schema graph.Schema, authority workflowwait.ResponderAuthority) (workflowwait.Record, error) {
	resumeSchema, err := workflowwait.NewSchemaRef(schema)
	if err != nil {
		return workflowwait.Record{}, err
	}
	record := workflowwait.Record{
		Kind: kind, Correlation: correlation, Deadline: deadline,
		ResumeSchema: resumeSchema, Visibility: workflowwait.VisibilityPrivate,
		Authority: authority, WakeSource: wake, Status: workflowwait.StatusOpen,
	}
	if err := record.Validate(); err != nil {
		return workflowwait.Record{}, err
	}
	return record, nil
}

func waitID(identity stepkind.InvocationIdentity, name, correlation string) string {
	seed := strings.Join([]string{identity.RunID, identity.NodeID, identity.Iteration, fmt.Sprint(identity.Attempt), name, correlation}, "\x00")
	digest := strings.TrimPrefix(values.SHA256Digest([]byte(seed)), "sha256:")
	return "wait-" + digest[:32]
}

func deadline(now func() time.Time, timeout time.Duration) (time.Time, error) {
	current := now()
	if current.IsZero() {
		return time.Time{}, fmt.Errorf("wait clock returned zero time")
	}
	return current.UTC().Add(timeout), nil
}

func validateCallbackCredential(credential CallbackCredential, path string) (string, string, error) {
	digest, err := workflowwait.DigestToken(credential.Token)
	if err != nil {
		return "", "", err
	}
	parsed, err := url.Parse(credential.URL)
	if err != nil || parsed == nil || !parsed.IsAbs() || parsed.Host == "" || parsed.Scheme != "https" && parsed.Scheme != "http" {
		return "", "", fmt.Errorf("callback URL must be absolute HTTP(S)")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.EscapedPath() != path {
		return "", "", fmt.Errorf("callback URL must use the requested path without userinfo, query, or fragment")
	}
	if strings.Contains(parsed.Host, credential.Token) || strings.Contains(parsed.EscapedPath(), credential.Token) {
		return "", "", fmt.Errorf("callback URL must not contain the resume token")
	}
	return credential.URL, digest, nil
}

func continuationPayload(invocation stepkind.Invocation, name string, expectedKind workflowwait.Kind, expectedWake workflowwait.WakeSource, correlation string, schema graph.Schema) (values.Value, workflowwait.Resolution, error) {
	continuation := invocation.Continuation
	if continuation == nil {
		return values.Value{}, workflowwait.Resolution{}, fmt.Errorf("wait continuation is required")
	}
	if err := continuation.Validate(); err != nil {
		return values.Value{}, workflowwait.Resolution{}, err
	}
	expectedID := waitID(invocation.Identity, name, correlation)
	if continuation.ID != expectedID || continuation.Record.Kind != expectedKind || continuation.Record.WakeSource != expectedWake || continuation.Record.Correlation != correlation {
		return values.Value{}, workflowwait.Resolution{}, fmt.Errorf("wait continuation does not match invocation")
	}
	ref, err := workflowwait.NewSchemaRef(schema)
	if err != nil || ref.Digest != continuation.Record.ResumeSchema.Digest {
		return values.Value{}, workflowwait.Resolution{}, fmt.Errorf("wait continuation resume schema does not match config")
	}
	payload, ok := continuation.Values["resume"]
	if !ok || continuation.Record.Resolution == nil {
		return values.Value{}, workflowwait.Resolution{}, fmt.Errorf("wait continuation is missing resume payload or provenance")
	}
	cloned, err := cloneValue(payload)
	if err != nil {
		return values.Value{}, workflowwait.Resolution{}, err
	}
	if err := validateResumeEnvelope(cloned); err != nil {
		return values.Value{}, workflowwait.Resolution{}, err
	}
	if err := values.ValidateValueSchema(continuation.Record.ResumeSchema.Schema, cloned); err != nil {
		return values.Value{}, workflowwait.Resolution{}, fmt.Errorf("wait continuation payload schema: %w", err)
	}
	return cloned, *continuation.Record.Resolution, nil
}

func completionOutputs(identity stepkind.InvocationIdentity, kind string, payloadName string, payload values.Value, record workflowwait.Record, source SourceKind) (values.ValueSet, error) {
	outputs := values.ValueSet{}
	if payloadName != "" {
		cloned, err := cloneValue(payload)
		if err != nil {
			return nil, err
		}
		outputs[payloadName] = cloned
	}
	resolution := record.Resolution
	if resolution == nil {
		return nil, fmt.Errorf("resumed wait is missing resolution")
	}
	resume := map[string]any{
		"wait_id":     waitID(identity, kind, record.Correlation),
		"status":      "resumed",
		"source":      string(source),
		"correlation": record.Correlation,
		"resolved_at": resolution.ResolvedAt.UTC().Format(time.RFC3339Nano),
		"responder": map[string]any{
			"kind": resolution.Responder.Kind, "reference": resolution.Responder.Reference,
			"attributes": cloneStringMap(resolution.Responder.Attributes),
		},
	}
	var err error
	outputs["resume"], err = values.NewInline(resume, outputMetadata(identity, kind, "resume"))
	if err != nil {
		return nil, err
	}
	outputs["timed_out"], err = values.NewInline(false, outputMetadata(identity, kind, "timed_out"))
	if err != nil {
		return nil, err
	}
	return outputs, nil
}

func outputMetadata(identity stepkind.InvocationIdentity, kind, output string) values.Metadata {
	reference := identity.RunID + "/" + identity.NodeID
	if identity.Iteration != "" {
		reference += "/" + identity.Iteration
	}
	reference += fmt.Sprintf("/attempt-%d", identity.Attempt)
	return values.Metadata{
		Producer:  values.Producer{Kind: kind, Reference: reference, Output: output},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}
}

func cloneValue(value values.Value) (values.Value, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return values.Value{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned values.Value
	if err := decoder.Decode(&cloned); err != nil {
		return values.Value{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return values.Value{}, fmt.Errorf("value clone contains trailing JSON")
	}
	return cloned, cloned.Validate()
}

func validateResumeEnvelope(value values.Value) error {
	if value.Redaction != values.RedactionPrivate && value.Redaction != values.RedactionSecret {
		return fmt.Errorf("resume payload must be private or secret")
	}
	if value.Retention != values.RetentionRun && value.Retention != values.RetentionProject && value.Retention != values.RetentionExternal {
		return fmt.Errorf("resume payload must have run, project, or external retention")
	}
	return nil
}

func cloneConfig(config graph.Config) (map[string]any, error) {
	if config == nil {
		return nil, fmt.Errorf("config must be an object")
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned map[string]any
	if err := decoder.Decode(&cloned); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config contains trailing JSON")
	}
	return cloned, nil
}

func parseDuration(value any, path string, required bool, findings *[]diagnostic.Diagnostic) time.Duration {
	if value == nil {
		if required {
			*findings = append(*findings, configFinding(path, "is required"))
		}
		return 0
	}
	text, ok := value.(string)
	if !ok {
		*findings = append(*findings, configFinding(path, "must be a duration string"))
		return 0
	}
	parsed, err := time.ParseDuration(text)
	if err != nil || parsed <= 0 || parsed > maximumWait {
		*findings = append(*findings, configFinding(path, "must be a positive duration no greater than 8760h"))
		return 0
	}
	return parsed
}

func parseSchema(value any, path string, findings *[]diagnostic.Diagnostic) graph.Schema {
	if value == nil {
		return graph.Schema{}
	}
	schema, ok := value.(map[string]any)
	if !ok {
		*findings = append(*findings, configFinding(path, "must be a local JSON Schema object"))
		return graph.Schema{}
	}
	if err := values.ValidateSchema(graph.Schema(schema)); err != nil {
		*findings = append(*findings, configFinding(path, "must be a valid local JSON Schema"))
		return graph.Schema{}
	}
	return graph.Schema(schema)
}

func requiredString(value any, path string, findings *[]diagnostic.Diagnostic) string {
	text, ok := value.(string)
	if !ok || !stableText(text, 4096) {
		*findings = append(*findings, configFinding(path, "must be non-empty stable UTF-8"))
		return ""
	}
	return text
}

func optionalString(value any, path string, findings *[]diagnostic.Diagnostic) string {
	if value == nil {
		return ""
	}
	return requiredString(value, path, findings)
}

func validateFields(object map[string]any, allowed map[string]struct{}, prefix string, findings *[]diagnostic.Diagnostic) {
	for _, key := range sortedKeys(object) {
		if _, ok := allowed[key]; !ok {
			*findings = append(*findings, configFinding(prefix+key, "is not supported"))
		}
	}
}

func configFinding(path, message string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{Severity: diagnostic.SeverityError, Code: stepkind.CodeInvalidConfig, Message: path + " " + message}
}

func sortFindings(findings []diagnostic.Diagnostic) {
	sort.SliceStable(findings, func(i, j int) bool { return findings[i].Message < findings[j].Message })
}

func hasErrors(findings []diagnostic.Diagnostic) bool { return len(findings) != 0 }

func executionError(code, message string, classification stepkind.RetryClassification, cause error) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: classification, Cause: cause}
}

func invalidInvocation(cause error) error {
	return executionError(CodeInvalidInvocation, "wait invocation is invalid", stepkind.RetryPermanent, cause)
}

func contextOr(ctx context.Context, code, message string, classification stepkind.RetryClassification, cause error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return executionError(code, message, classification, cause)
}

func stableText(value string, maximum int) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) || len(value) > maximum {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validateRootPath(path string) error {
	if !stableText(path, 2048) || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || strings.Contains(path, "{{") || strings.Contains(path, "}}") || strings.Contains(path, "\\") {
		return fmt.Errorf("must be a static root-relative path")
	}
	parsed, err := url.ParseRequestURI(path)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.EscapedPath() != path {
		return fmt.Errorf("must be a canonical root-relative path without query or fragment")
	}
	for _, segment := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("must not contain empty or traversal segments")
		}
	}
	return nil
}

func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func cloneStringMap(entries map[string]string) map[string]string {
	if entries == nil {
		return nil
	}
	cloned := make(map[string]string, len(entries))
	for key, value := range entries {
		cloned[key] = value
	}
	return cloned
}

func cloneSource(source Source) Source {
	copySource := source
	copySource.Attributes = cloneStringMap(source.Attributes)
	return copySource
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
