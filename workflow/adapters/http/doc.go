// Package http implements the registered workflow http@v1 step kind. Network
// resolution, destination authorization, dialing, secrets, artifacts, and
// operational observation remain explicit adapter boundaries.
//
// Configuration is a closed graph.Config object. It requires an absolute
// http(s) "url" and accepts "method", "headers", JSON-compatible "body",
// opaque-reference "auth", "timeout", "max_response_bytes", "inline_limit",
// "expected_status", "expected_content_types", "expected_json_schema",
// "redirects", "idempotency_key", "effects", and "capabilities". Defaults
// are GET, 30 seconds, 8 MiB maximum response, 64 KiB inline capture, 2xx
// status, and no redirects. Kind.Spec returns the machine-readable schema.
//
// Every resolved address of every hop is authorized before one deterministic
// address is pinned for the actual dial. Redirects are followed manually and
// never carry credentials across origins. Secret references are resolved only
// immediately before Transport.RoundTrip; their material is excluded from
// ConfigDescription, observations, outputs, and structured errors.
//
// Completed results contain status, normalized headers, a string or artifact
// body, optional inline body_json, and sanitized final-request metadata. Large
// JSON never appears in body_json after promotion to an artifact.
package http
