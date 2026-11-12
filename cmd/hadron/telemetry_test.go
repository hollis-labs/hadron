package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

// A caller's traceparent must reach the handler's context, so the runtime's
// spans join the caller's trace (HTTP gets this from withTracing; startTelemetry
// installs the propagator that makes the header meaningful).
func TestIncomingTraceparentReachesTheHandler(t *testing.T) {
	startTelemetry("test")()
	const traceID = "0102030405060708090a0b0c0d0e0f10"
	var got trace.SpanContext
	h := withTracing(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = trace.SpanContextFromContext(r.Context())
	}))
	req := httptest.NewRequest(http.MethodPost, "/api/workflows/validate", nil)
	req.Header.Set("traceparent", "00-"+traceID+"-1112131415161718-01")
	h.ServeHTTP(httptest.NewRecorder(), req)
	if !got.IsValid() || got.TraceID().String() != traceID {
		t.Fatalf("handler span context = %v, want trace %s", got, traceID)
	}
}
