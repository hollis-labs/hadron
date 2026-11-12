package runtime

import (
	"strings"
	"testing"

	"github.com/hollis-labs/substrate/module"
)

func appWith(block map[string]any) *module.App {
	return &module.App{Name: "hadron", Config: map[string]any{"hadron": block}, Values: map[string]any{}}
}

func TestConfigFromAppDefaultsAndValues(t *testing.T) {
	c, err := ConfigFromApp(&module.App{Name: "hadron", Config: map[string]any{}, Values: map[string]any{}})
	if err != nil || c.DataDir != "hadron-data" || c.Workers != 3 || c.EventOutbox {
		t.Fatalf("defaults = %+v, %v", c, err)
	}
	c, err = ConfigFromApp(appWith(map[string]any{"data_dir": "/srv/h", "workers": float64(5), "event_stream_limit": float64(2), "events": map[string]any{"outbox": true}}))
	if err != nil || c.DataDir != "/srv/h" || c.Workers != 5 || c.EventStreamLimit != 2 || !c.EventOutbox {
		t.Fatalf("values = %+v, %v", c, err)
	}
	if got := c.SourceRoot(); got != "/srv/h/workflows" {
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
