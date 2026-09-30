// Package tetherint proves Hadron's Tether-backed agent session host against
// a real Tether daemon (muxd) built from a pinned Tether module version.
//
// The tests live behind the tether_integration build tag and the
// HADRON_TETHER_INTEGRATION=1 environment variable, so neither Hadron's
// `go test ./...` nor this module's plain `go test ./...` builds or starts a
// daemon. Run them with `make test-tether-integration` from the repository
// root. docs/workflows.md describes what they cover.
package tetherint
