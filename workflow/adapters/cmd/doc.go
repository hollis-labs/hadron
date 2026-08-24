// Package cmd implements the extraction-ready cmd@v1 workflow step kind.
//
// Commands are launched directly, never through a shell. The adapter does not
// inherit the host process environment or working directory. A host policy
// resolves and authorizes the executable, working directory, capabilities, and
// sandbox contract immediately before execution. Secret material is resolved
// only for process injection and every observable byte stream is redacted
// before capture, artifact storage, or operational event emission.
//
// The package may import workflow graph, step-kind, diagnostic, and value
// contracts plus the Go standard library. It must not import Hadron application,
// transport, persistence, or provider packages. Config and seam types are
// versioned with cmd@v1; concrete host policy and artifact ownership remain
// adapter-composition concerns.
package cmd
