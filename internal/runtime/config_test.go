package runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hollis-labs/substrate/module"
)

func appWith(block map[string]any) *module.App {
	return &module.App{Name: "hadron", Config: map[string]any{"hadron": block}, Values: map[string]any{}}
}

func TestConfigFromAppDefaultsAndValues(t *testing.T) {
	t.Chdir(t.TempDir())
	c, err := ConfigFromApp(&module.App{Name: "hadron", Config: map[string]any{}, Values: map[string]any{}})
	if err != nil || filepath.Base(c.DataDir) != "hadron-data" || !filepath.IsAbs(c.DataDir) || c.Workers != 3 || c.EventOutbox {
		t.Fatalf("defaults = %+v, %v", c, err)
	}
	dir := t.TempDir()
	c, err = ConfigFromApp(appWith(map[string]any{"data_dir": dir, "workers": float64(5), "event_stream_limit": float64(2), "events": map[string]any{"outbox": true}}))
	if err != nil || c.DataDir != resolved(t, dir) || c.Workers != 5 || c.EventStreamLimit != 2 || !c.EventOutbox {
		t.Fatalf("values = %+v, %v", c, err)
	}
	if got := c.SourceRoot(); got != filepath.Join(resolved(t, dir), "workflows") {
		t.Errorf("source root %q", got)
	}
}

func TestConfigFromAppRefusesWhatItDoesNotUnderstand(t *testing.T) {
	for name, block := range map[string]map[string]any{
		"unknown key":     {"workerz": float64(3)},
		"bad workers":     {"workers": "three"},
		"fractional":      {"workers": 2.5},
		"negative":        {"workers": float64(-1)},
		"bad data dir":    {"data_dir": 7},
		"events not map":  {"events": true},
		"unknown event":   {"events": map[string]any{"hub_relay": true}},
		"outbox not bool": {"events": map[string]any{"outbox": "yes"}},
	} {
		if _, err := ConfigFromApp(appWith(block)); err == nil {
			t.Errorf("%s was accepted", name)
		} else if !strings.Contains(err.Error(), "hadron") {
			t.Errorf("%s: error does not name the setting: %v", name, err)
		}
	}
}

func resolved(t *testing.T, dir string) string {
	t.Helper()
	r, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestDataDirBehindASymlinkIsResolvedNotRefused(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Skip("symlinks unavailable")
	}
	c, err := ConfigFromApp(appWith(map[string]any{"data_dir": filepath.Join(link, "data")}))
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(resolved(t, real), "data"); c.DataDir != want {
		t.Errorf("data dir %q, want %q", c.DataDir, want)
	}
}

func TestAgentSubstratesAreDecodedStrictlyAndValidated(t *testing.T) {
	good := map[string]any{"work": map[string]any{"kind": "tether_session", "tether": map[string]any{"launch": "claude-work", "stop_on_result": false}}}
	c, err := ConfigFromApp(appWith(map[string]any{"data_dir": t.TempDir(), "agent_substrates": good}))
	if err != nil {
		t.Fatal(err)
	}
	if s := c.AgentSubstrates["work"]; s.Tether == nil || s.Tether.Launch != "claude-work" || s.Tether.EffectiveStopOnResult() {
		t.Errorf("substrate = %+v", s)
	}
	for name, block := range map[string]any{
		"unknown key":     map[string]any{"work": map[string]any{"kind": "tether_session", "tether": map[string]any{"launch": "l"}, "provider": "x"}},
		"unknown tether":  map[string]any{"work": map[string]any{"kind": "tether_session", "tether": map[string]any{"launch": "l", "endpont": "x"}}},
		"other kind":      map[string]any{"work": map[string]any{"kind": "go_agent_runtime"}},
		"no tether block": map[string]any{"work": map[string]any{"kind": "tether_session"}},
		"no launch":       map[string]any{"work": map[string]any{"kind": "tether_session", "tether": map[string]any{}}},
		"bad timeout":     map[string]any{"work": map[string]any{"kind": "tether_session", "tether": map[string]any{"launch": "l", "unreachable_timeout": "soon"}}},
		"not a mapping":   []any{"x"},
	} {
		if _, err := ConfigFromApp(appWith(map[string]any{"data_dir": t.TempDir(), "agent_substrates": block})); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}
