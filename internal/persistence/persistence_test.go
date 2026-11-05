package persistence_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hollis-labs/substrate/boot"
	"github.com/hollis-labs/substrate/modules/database"
	_ "github.com/hollis-labs/substrate/modules/health"

	"github.com/hollis-labs/hadron/internal/persistence"
)

func open(t *testing.T) (*boot.Running, string) {
	t.Helper()
	fsys, root := persistence.Migrations()
	database.UseMigrationFS(fsys, root)
	path := filepath.Join(t.TempDir(), "hadron.db")
	run, err := boot.Start(context.Background(), "hadron", []string{"health", "database"}, `{"database":{"sqlite":{"path":"`+path+`"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Stop(context.Background()) })
	return run, path
}

func TestTheSquashedSchemaAppliesThroughSubstrate(t *testing.T) {
	run, _ := open(t)
	st := database.From(run.App)
	var n int
	if err := st.QueryRowContext(context.Background(),
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('workflow_runs','workflow_events','workflow_waits','store_generation')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("expected the core workflow tables, found %d of 4", n)
	}
}

// The event log is append-only by construction: a trigger rejects an update or
// delete. That proves the multi-statement triggers survived goose intact.
func TestImmutabilityTriggersSurviveTheMigration(t *testing.T) {
	run, _ := open(t)
	ctx := context.Background()
	st := database.From(run.App)
	rows, err := st.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '%reject%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	for _, want := range []string{"workflow_events_reject_update", "workflow_events_reject_delete"} {
		found := false
		for _, n := range names {
			found = found || n == want
		}
		if !found {
			t.Errorf("trigger %s is missing (have %s)", want, strings.Join(names, ", "))
		}
	}
	// A trigger that is present but does not fire would pass the check above.
	var body string
	if err := st.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='trigger' AND name='workflow_events_reject_update'`).Scan(&body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToUpper(body), "RAISE") {
		t.Errorf("the update trigger has no RAISE: %s", body)
	}
}

func TestMigrationsVerifyOnReopen(t *testing.T) {
	_, path := open(t)
	// A second start against the same file finds the schema at the expected version.
	fsys, root := persistence.Migrations()
	database.UseMigrationFS(fsys, root)
	run, err := boot.Start(context.Background(), "hadron", []string{"health", "database"},
		`{"database":{"schema_mode":"verify","sqlite":{"path":"`+path+`"}}}`)
	if err != nil {
		t.Fatalf("verify mode refused a migrated database: %v", err)
	}
	_ = run.Stop(context.Background())
}
