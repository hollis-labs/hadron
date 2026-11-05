package persistence

import "testing"

// hasColumn reports whether table has a column named column.
func hasColumn(t *testing.T, store *Store, table, column string) bool {
	t.Helper()
	var n int
	if err := store.DB().QueryRow(`SELECT count(*) FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&n); err != nil {
		t.Fatalf("table_info %s: %v", table, err)
	}
	return n > 0
}
