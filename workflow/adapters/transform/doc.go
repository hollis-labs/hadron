// Package transform implements the extraction-ready, pure expression transform
// step kind.
//
// Transform config is a non-empty object whose normalized keys name outputs and
// whose values are raw workflow expressions. Expressions are evaluated without
// environment access or adapter-defined functions. The default executor exposes
// invocation inputs through the standard inputs root. Hosts that have already
// constructed a visibility-scoped invocation context may inject a ContextProvider
// to expose steps, item, index, and immutable runtime roots.
//
// The package owns no storage, transport, provider, or Hadron application
// integration. Runtime owns declared output-schema validation and persistence.
package transform
