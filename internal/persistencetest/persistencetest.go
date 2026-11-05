// Package persistencetest opens migrated Hadron databases for other packages'
// tests. persistence's own tests use their test-only Open (the package cannot
// import this one without an import cycle).
package persistencetest

import (
	"context"
	"fmt"

	"github.com/hollis-labs/libs/core/store"
	"github.com/hollis-labs/libs/core/store/migrate"

	"github.com/hollis-labs/hadron/internal/persistence"
)

// Open opens a SQLite database at path the way Substrate's database module
// does (libs core/store, then the embedded goose migration) and wraps it. Close
// the returned Store when done.
func Open(path string) (*persistence.Store, error) {
	if path == "" {
		return nil, fmt.Errorf("db path is required")
	}
	ctx := context.Background()
	cfg := store.Config{Driver: store.DriverSQLite, SQLite: store.SQLiteConfig{Path: path, CreateParentDir: true}}
	st, err := store.Open(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	fsys, root := persistence.Migrations()
	if err := migrate.Ensure(ctx, cfg, st, migrate.Source{FS: fsys, Root: root}); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return persistence.NewStoreWithCloser(st.DB(), st.Close), nil
}
