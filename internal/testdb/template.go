// Package testdb provides isolated, migrated Hadron databases for tests.
// Production and migration tests must continue to call persistence.Open directly.
package testdb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type store interface {
	DB() *sql.DB
	Close() error
}

var template struct {
	sync.Once
	image []byte
	err   error
}

// Open copies a once-migrated, fully closed database before calling open. Pass
// persistence.Open (or Open from persistence's own tests) as the callback.
// Every copy has its own store generation. Existing paths are opened normally,
// preserving data and generation for reopen and multi-handle tests.
func Open[T store](path string, open func(string) (T, error)) (T, error) {
	var zero T
	if path == "" {
		return open(path)
	}
	if _, err := os.Stat(path); err == nil {
		return open(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return zero, err
	}
	template.Do(func() {
		dir, err := os.MkdirTemp("", "hadron-test-template-")
		if err != nil {
			template.err = err
			return
		}
		defer os.RemoveAll(dir)
		source := filepath.Join(dir, "template.db")
		s, err := open(source)
		if err != nil {
			template.err = err
			return
		}
		// Closing the last connection checkpoints WAL before reading the main file.
		if err = s.Close(); err != nil {
			template.err = err
			return
		}
		template.image, template.err = os.ReadFile(source)
	})
	if template.err != nil {
		return zero, fmt.Errorf("test database template: %w", template.err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return zero, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return zero, err
	}
	_, writeErr := f.Write(template.image)
	if err = errors.Join(writeErr, f.Close()); err != nil {
		return zero, err
	}
	s, err := open(path)
	if err != nil {
		return zero, err
	}
	if _, err = s.DB().Exec(`UPDATE store_generation SET generation = lower(hex(randomblob(16))) WHERE id = 1`); err != nil {
		_ = s.Close()
		return zero, fmt.Errorf("reset test store generation: %w", err)
	}
	return s, nil
}
