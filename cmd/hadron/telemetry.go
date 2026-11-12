package main

import (
	"context"
	"log"
	"net/http"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel"
	otelpropagation "go.opentelemetry.io/otel/propagation"

	feotel "github.com/hollis-labs/libs/core/util/otel"
	"github.com/hollis-labs/libs/core/util/otel/propagation"
)

// startTelemetry installs the W3C trace-context propagator (so an incoming
// traceparent is honoured whether or not export is on) and, when
// HADRON_OTEL is truthy, starts the OpenTelemetry exporter. The returned
// function shuts it down. Substrate has no OTel module yet; when it does, this
// moves there.
func startTelemetry(version string) func() {
	otel.SetTextMapPropagator(otelpropagation.NewCompositeTextMapPropagator(
		otelpropagation.TraceContext{}, otelpropagation.Baggage{}))
	if !feotel.EnabledFromEnv("HADRON_OTEL") {
		return func() {}
	}
	return feotel.InitOrWarn(context.Background(), log.Printf, 5*time.Second,
		feotel.WithServiceName("hadron"),
		feotel.WithServiceVersion(version),
		feotel.WithServiceNamespace("hollis"),
		feotel.WithEnvironment(feotel.EnvironmentFromEnv("HADRON_ENV", "dev")),
	)
}

// withTracing continues the caller's trace: it extracts traceparent from each
// request and runs the handler inside a server span, so the workflow runtime's
// own spans and logs attach to the caller's trace.
func withTracing(next http.Handler) http.Handler { return propagation.HTTPMiddleware(next) }

// version is the module version the binary was built from (devel when built from a checkout).
var version = func() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return "devel"
}()
