package testdb_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/testdb"
)

func TestCopiesAreIsolatedAndReopenPreservesState(t *testing.T) {
	t.Parallel()
	for i := range 8 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "test.db")
			s, err := testdb.Open(path, persistence.Open)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			// A new copy must not contain another test's writes.
			if _, err = s.DB().Exec(`CREATE TABLE fixture_marker (value TEXT); INSERT INTO fixture_marker VALUES ('isolated')`); err != nil {
				t.Fatal(err)
			}
			var generation string
			if err = s.DB().QueryRow(`SELECT generation FROM store_generation`).Scan(&generation); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := testdb.Open(path, persistence.Open)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			var kept, marker string
			if err = reopened.DB().QueryRow(`SELECT generation FROM store_generation`).Scan(&kept); err != nil {
				t.Fatal(err)
			}
			if kept != generation {
				t.Fatal("reopen changed store generation")
			}
			if err = reopened.DB().QueryRow(`SELECT value FROM fixture_marker`).Scan(&marker); err != nil || marker != "isolated" {
				t.Fatalf("reopen lost data: %v", err)
			}
			other, err := testdb.Open(filepath.Join(t.TempDir(), "other.db"), persistence.Open)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			var different string
			if err = other.DB().QueryRow(`SELECT generation FROM store_generation`).Scan(&different); err != nil {
				t.Fatal(err)
			}
			if different == generation {
				t.Fatal("distinct copies share a generation")
			}
			if _, err = other.DB().Exec(`CREATE TABLE fixture_marker (value TEXT)`); err != nil {
				t.Fatalf("copy leaked test writes: %v", err)
			}
		})
	}
}

func BenchmarkDatabaseSetup(b *testing.B) {
	for _, tc := range []struct {
		name string
		open func(string) (*persistence.Store, error)
	}{
		{"fresh_migrations", persistence.Open},
		{"template", func(path string) (*persistence.Store, error) { return testdb.Open(path, persistence.Open) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s, err := tc.open(filepath.Join(dir, fmt.Sprintf("%d.db", i)))
				if err != nil {
					b.Fatal(err)
				}
				if err = s.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
