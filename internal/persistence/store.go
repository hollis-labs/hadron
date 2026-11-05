package persistence

import "database/sql"

// Store wraps the application database that Substrate's database module opened
// and migrated. In production it does not open, configure or close the
// database: the module owns that (WAL, busy timeout, single writer, schema
// version). Tests open their own through Open (see open_test.go) and Close it.
type Store struct {
	db     *sql.DB
	closer func() error
}

// NewStore wraps an open, migrated database.
func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// DB returns the wrapped database.
func (s *Store) DB() *sql.DB { return s.db }

// Close releases what the Store itself opened. It does nothing for a Store
// made with NewStore: the database module closes the database it opened.
func (s *Store) Close() error {
	if s == nil || s.closer == nil {
		return nil
	}
	return s.closer()
}

func closeRows(rows *sql.Rows) { _ = rows.Close() }

// NewStoreWithCloser is NewStore for a caller that opened the database itself
// and wants Close to release it (test helpers). Production code uses NewStore.
func NewStoreWithCloser(db *sql.DB, closer func() error) *Store {
	return &Store{db: db, closer: closer}
}
