// Package generatedchild implements generated_child@v1, the extraction-safe
// materialization boundary for workflow definitions produced as typed values.
//
// Source and graph values are bounded and processed through the ordinary
// compiler/validator before host authorization and immutable registration.
// The adapter returns only an exact-digest graph.DefinitionRef. A subsequent
// graph-native call node consumes that typed reference through
// graph.CallSpec.DefinitionInput; generated_child never mutates a bound plan or
// starts a child run directly.
package generatedchild
