package runtime

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/hollis-labs/substrate/module"
)

// Config is the runtime's slice of the hadron: block in the Substrate manifest
// config. Hadron validates it at start; there is no settings file.
type Config struct {
	// DataDir holds workflow sources, the catalog, artifacts, the unattended
	// allow-list and the contract attestor key. Default "hadron-data".
	DataDir string
	// Workers is the number of node workers. Default 3.
	Workers int
	// EventStreamLimit caps concurrent event streams per principal. Default 0
	// (the operator's own default).
	EventStreamLimit int
	// EventOutbox turns on the transactional event outbox in the stores.
	EventOutbox bool
}

// defaultWorkflowWorkers applies when workers is unset.
const defaultWorkflowWorkers = 3

func (c Config) withDefaults() Config {
	if c.DataDir == "" {
		c.DataDir = "hadron-data"
	}
	if c.Workers < 1 {
		c.Workers = defaultWorkflowWorkers
	}
	return c
}

// SourceRoot is where workflow definition files live.
func (c Config) SourceRoot() string { return filepath.Join(c.DataDir, "workflows") }

// ConfigFromApp reads and validates the hadron: block of the Substrate config:
//
//	hadron:
//	  data_dir: hadron-data          # workflow sources, catalog, artifacts, allow-list, attestor key
//	  workers: 3                     # node workers
//	  event_stream_limit: 0          # concurrent event streams per principal (0 = default)
//	  events: { outbox: false }      # transactional event outbox in the stores
//
// Unknown keys are errors, so a misspelt setting does not silently do nothing.
func ConfigFromApp(app *module.App) (Config, error) {
	block, _ := app.Config["hadron"].(map[string]any)
	var c Config
	for key, v := range block {
		switch key {
		case "data_dir":
			s, ok := v.(string)
			if !ok {
				return c, fmt.Errorf("hadron.data_dir must be a string")
			}
			c.DataDir = s
		case "workers":
			n, err := wholeNumber("hadron.workers", v)
			if err != nil {
				return c, err
			}
			c.Workers = n
		case "event_stream_limit":
			n, err := wholeNumber("hadron.event_stream_limit", v)
			if err != nil {
				return c, err
			}
			c.EventStreamLimit = n
		case "events":
			events, ok := v.(map[string]any)
			if !ok {
				return c, fmt.Errorf("hadron.events must be a mapping")
			}
			for k, ev := range events {
				if k != "outbox" {
					return c, fmt.Errorf("hadron.events.%s is not a known setting (known: outbox)", k)
				}
				b, ok := ev.(bool)
				if !ok {
					return c, fmt.Errorf("hadron.events.outbox must be true or false")
				}
				c.EventOutbox = b
			}
		default:
			return c, fmt.Errorf("hadron.%s is not a known setting (known: data_dir, workers, event_stream_limit, events)", key)
		}
	}
	if c.Workers < 0 || c.EventStreamLimit < 0 {
		return c, fmt.Errorf("hadron.workers and hadron.event_stream_limit cannot be negative")
	}
	c = c.withDefaults()
	dir, err := resolveDataDir(c.DataDir)
	if err != nil {
		return c, fmt.Errorf("hadron.data_dir %q: %w", c.DataDir, err)
	}
	c.DataDir = dir
	return c, nil
}

// resolveDataDir makes the data directory absolute, creates it owner-only and
// resolves symlinks in its path. The artifact store refuses a root whose path
// has a symlink component, and an unresolved working directory (macOS /var, a
// symlinked home) would otherwise fail deep inside startup.
func resolveDataDir(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

func wholeNumber(name string, v any) (int, error) {
	switch n := v.(type) {
	case float64: // JSON numbers
		if n == float64(int(n)) {
			return int(n), nil
		}
	case int:
		return n, nil
	}
	return 0, fmt.Errorf("%s must be a whole number", name)
}
