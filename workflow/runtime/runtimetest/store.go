// Package runtimetest preserves the original test-support import path for the
// public process-lifetime in-memory runtime store.
//
// Deprecated: production and new test code should import
// workflow/runtime/inmemory. This package is a source-compatible alias, not a
// second store implementation.
package runtimetest

import "github.com/hollis-labs/hadron/workflow/runtime/inmemory"

// Store is the process-lifetime in-memory runtime store.
//
// Deprecated: use inmemory.Store.
type Store = inmemory.Store

// NewStore returns an empty process-lifetime in-memory runtime store.
//
// Deprecated: use inmemory.NewStore.
func NewStore() *Store { return inmemory.NewStore() }
