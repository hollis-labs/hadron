// Package persistence owns Hadron's durable schema. The schema is one goose
// migration per dialect, embedded here and applied by Substrate's database
// module. The store code that reads and writes it arrives with the engine host
// (see docs/status.md); this package is the part that exists first.
package persistence

import "embed"

//go:embed migrations
var migrations embed.FS

// Migrations is the embedded migration tree: Root "migrations" holds one
// directory per dialect. Hadron is SQLite only.
func Migrations() (fsys embed.FS, root string) { return migrations, "migrations" }
