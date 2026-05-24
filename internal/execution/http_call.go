package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

const maxHTTPCallBodyBytes = 65536

func (r *runExecution) executeHTTPCallStep(ctx context.Context, section string, step blueprint.Step) error {
	if step.HTTPCall == nil {
		return fmt.Errorf("step %q has no http_call", step.Name)
	}
	call := step.HTTPCall
	method := strings.ToUpper(strings.TrimSpace(call.Method))
	if method == "" {
		method = http.MethodGet
	}
	if err := validateLocalHTTPURL(call.URL); err != nil {
		return fmt.Errorf("http_call safety check: %w", err)
	}

	timeout := call.TimeoutSeconds
	if timeout == 0 {
		timeout = step.TimeoutSeconds
	}
	if timeout == 0 && r.manager.settings != nil {
		timeout = r.manager.settings.GetDefaultTimeout()
	}

	stepCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		stepCtx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}

	body, contentType, err := httpCallRequestBody(call)
	if err != nil {
		return err
	}

	if r.dryRun {
		r.emit(section, step.Name, "dry_run", fmt.Sprintf("[dry-run] would call %s %s", method, call.URL))
		return nil
	}

	start := time.Now()
	r.emit(section, step.Name, "http_call_start", fmt.Sprintf("%s %s", method, call.URL))

	req, err := http.NewRequestWithContext(stepCtx, method, call.URL, body)
	if err != nil {
		return fmt.Errorf("http_call request: %w", err)
	}
	for k, v := range call.Headers {
		req.Header.Set(k, v)
	}
	if contentType != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.emit(section, step.Name, "http_call_error", err.Error())
		return fmt.Errorf("http_call: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPCallBodyBytes+1))
	if err != nil {
		r.emit(section, step.Name, "http_call_error", err.Error())
		return fmt.Errorf("http_call read response: %w", err)
	}
	bodyText := string(responseBody)
	truncated := false
	if len(responseBody) > maxHTTPCallBodyBytes {
		bodyText = string(responseBody[:maxHTTPCallBodyBytes])
		truncated = true
	}
	bodyJSON := compactJSON(bodyText)

	result := map[string]any{
		"status_code": resp.StatusCode,
		"duration_ms": time.Since(start).Milliseconds(),
		"body":        bodyText,
		"body_json":   bodyJSON,
		"truncated":   truncated,
	}
	resultJSON, _ := json.Marshal(result)
	r.emit(section, step.Name, "http_call_response", string(resultJSON))
	r.emit(section, step.Name, "log", fmt.Sprintf("::set-output status_code=%d", resp.StatusCode))
	r.emit(section, step.Name, "log", fmt.Sprintf("::set-output body=%s", sanitizeSetOutputValue(bodyText)))
	if bodyJSON != "" {
		r.emit(section, step.Name, "log", fmt.Sprintf("::set-output body_json=%s", bodyJSON))
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("http_call returned status %d", resp.StatusCode)
		r.emit(section, step.Name, "http_call_error", err.Error())
		return err
	}
	return nil
}

func httpCallRequestBody(call *blueprint.HTTPCall) (io.Reader, string, error) {
	if len(call.BodyJSON) > 0 {
		b, err := json.Marshal(call.BodyJSON)
		if err != nil {
			return nil, "", fmt.Errorf("http_call body_json: %w", err)
		}
		return bytes.NewReader(b), "application/json", nil
	}
	if call.Body != "" {
		return strings.NewReader(call.Body), "", nil
	}
	return nil, "", nil
}

func validateLocalHTTPURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("url scheme must be http or https")
	}
	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("url host is required")
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("url host must be local-only")
	}
	if ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("url host must be local-only")
}

func compactJSON(body string) string {
	var v any
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

func sanitizeSetOutputValue(v string) string {
	return strings.ReplaceAll(strings.ReplaceAll(v, "\r", "\\r"), "\n", "\\n")
}
