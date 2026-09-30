package settings

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDefaultSettingsIncludesInitializedMaps(t *testing.T) {
	s := DefaultSettings()
	if s.MCPServers == nil {
		t.Fatal("expected MCPServers map to be initialized")
	}
	if s.AgentSubstrates == nil {
		t.Fatal("expected AgentSubstrates map to be initialized")
	}
	if s.MessageSubstrates == nil {
		t.Fatal("expected MessageSubstrates map to be initialized")
	}
}

func TestLoadDefaultsMapsWhenMissing(t *testing.T) {
	t.Helper()
	s, err := Load(t.TempDir())
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	if s.MCPServers == nil {
		t.Fatal("expected MCPServers map to be initialized")
	}
	if s.AgentSubstrates == nil {
		t.Fatal("expected AgentSubstrates map to be initialized")
	}
	if s.MessageSubstrates == nil {
		t.Fatal("expected MessageSubstrates map to be initialized")
	}
}

func TestLoadInitializesNewSubstrateMapsForLegacySettings(t *testing.T) {
	dir := t.TempDir()
	legacy := `{
  "blueprint_dir": "/tmp/blueprints",
  "mcp_servers": {
    "torque": {
      "transport": "stdio",
      "command": "/usr/local/bin/torque-mcp"
    }
  },
  "execution": {
    "allowedCommands": [],
    "deniedCommands": [],
    "allowedDirs": [],
    "deniedDirs": [],
    "maxConcurrentJobs": 3,
    "defaultTimeout": 300,
    "workers": 3
  },
  "safety": {
    "requireConfirmation": true,
    "dryRunByDefault": false,
    "blockSudo": false,
    "sandboxMode": false
  },
  "telemetry": {
    "enabled": true,
    "retainDays": 30
  }
}`
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), []byte(legacy), 0o600); err != nil {
		t.Fatalf("write legacy settings: %v", err)
	}

	s, err := Load(dir)
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	if s.AgentSubstrates == nil {
		t.Fatal("expected AgentSubstrates map to be initialized")
	}
	if s.MessageSubstrates == nil {
		t.Fatal("expected MessageSubstrates map to be initialized")
	}
}

func TestSaveAndLoadRoundTripsSubstrateSettings(t *testing.T) {
	dir := t.TempDir()
	want := DefaultSettings()
	want.AgentSubstrates["local_runtime"] = AgentSubstrateSettings{
		Kind:                   "go_agent_runtime",
		Provider:               "claude",
		Runtime:                "streaming-stdio",
		Authority:              "hadron",
		WorkingDirMode:         "blueprint_dir",
		AllowGenericSubprocess: false,
		Boot: AgentBootSettings{
			Profile:          "hadron.default",
			CallbacksProfile: "shared",
			PlantNativeFiles: true,
		},
	}
	want.MessageSubstrates["tether"] = MessageSubstrateSetting{
		Kind:           "tether_http",
		Authority:      "agent-mux",
		BaseURL:        "http://127.0.0.1:7777",
		Headers:        map[string]string{"Authorization": "Bearer test"},
		TimeoutSeconds: 30,
		NotifyWake:     true,
	}

	if err := want.Save(dir); err != nil {
		t.Fatalf("save settings: %v", err)
	}

	got, err := Load(dir)
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}

	if !reflect.DeepEqual(got.AgentSubstrates, want.AgentSubstrates) {
		t.Fatalf("agent substrates mismatch:\n got: %#v\nwant: %#v", got.AgentSubstrates, want.AgentSubstrates)
	}
	if !reflect.DeepEqual(got.MessageSubstrates, want.MessageSubstrates) {
		t.Fatalf("message substrates mismatch:\n got: %#v\nwant: %#v", got.MessageSubstrates, want.MessageSubstrates)
	}
}

func TestValidateRejectsInvalidSubstrateConfig(t *testing.T) {
	s := DefaultSettings()
	s.AgentSubstrates["bad_agent"] = AgentSubstrateSettings{
		Kind:           "go_agent_runtime",
		Provider:       "codex",
		Runtime:        "subprocess",
		WorkingDirMode: "bogus",
	}
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "agent_substrates.bad_agent.working_dir_mode") {
		t.Fatalf("expected working_dir_mode validation error, got %v", err)
	}

	s = DefaultSettings()
	s.MessageSubstrates["bad_remote"] = MessageSubstrateSetting{
		Kind:    "tether_http",
		BaseURL: "://bad",
	}
	err = s.Validate()
	if err == nil || !strings.Contains(err.Error(), "message_substrates.bad_remote.base_url") {
		t.Fatalf("expected base_url validation error, got %v", err)
	}
}

func TestTetherSessionSubstrateRoundTripsAndDefaults(t *testing.T) {
	dir := t.TempDir()
	want := DefaultSettings()
	stop := false
	want.AgentSubstrates["tether"] = AgentSubstrateSettings{
		Kind: AgentSubstrateKindTetherSession,
		Tether: &TetherSubstrateSettings{
			Launch: "claude-default", Launches: map[string]string{"reviewer": "claude-review"},
			ResultOptional: true, StopOnResult: &stop, UnreachableTimeout: "90s",
		},
	}
	if err := want.Save(dir); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir)
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	if !reflect.DeepEqual(got.AgentSubstrates, want.AgentSubstrates) {
		t.Fatalf("agent substrates mismatch:\n got: %#v\nwant: %#v", got.AgentSubstrates, want.AgentSubstrates)
	}
	tether := got.AgentSubstrates["tether"].Tether
	if tether.EffectiveStopOnResult() || tether.EffectiveEndpoint() != DefaultTetherEndpoint {
		t.Fatalf("effective tether settings = %#v", tether)
	}
	if timeout, err := tether.EffectiveUnreachableTimeout(); err != nil || timeout != 90*time.Second {
		t.Fatalf("unreachable timeout = %v, %v", timeout, err)
	}

	defaults := TetherSubstrateSettings{Launch: "x"}
	if !defaults.EffectiveStopOnResult() {
		t.Fatal("stop_on_result must default to true")
	}
	if timeout, err := defaults.EffectiveUnreachableTimeout(); err != nil || timeout != 10*time.Minute {
		t.Fatalf("default unreachable timeout = %v, %v", timeout, err)
	}

	cloned := tether.Clone()
	cloned.Launches["reviewer"] = "mutated"
	*cloned.StopOnResult = true
	if tether.Launches["reviewer"] != "claude-review" || *tether.StopOnResult {
		t.Fatal("Clone shares mutable state")
	}
}

func TestValidateTetherSessionSubstrate(t *testing.T) {
	for name, tc := range map[string]struct {
		substrate AgentSubstrateSettings
		want      string
	}{
		"missing tether block":    {AgentSubstrateSettings{Kind: AgentSubstrateKindTetherSession}, "tether: required"},
		"no launch":               {AgentSubstrateSettings{Kind: AgentSubstrateKindTetherSession, Tether: &TetherSubstrateSettings{}}, "launch or launches is required"},
		"bad timeout":             {AgentSubstrateSettings{Kind: AgentSubstrateKindTetherSession, Tether: &TetherSubstrateSettings{Launch: "l", UnreachableTimeout: "soon"}}, "unreachable_timeout"},
		"non-positive timeout":    {AgentSubstrateSettings{Kind: AgentSubstrateKindTetherSession, Tether: &TetherSubstrateSettings{Launch: "l", UnreachableTimeout: "0s"}}, "unreachable_timeout"},
		"empty mapped launch":     {AgentSubstrateSettings{Kind: AgentSubstrateKindTetherSession, Tether: &TetherSubstrateSettings{Launches: map[string]string{"a": " "}}}, "tether.launches"},
		"tether on other kind":    {AgentSubstrateSettings{Kind: "go_agent_runtime", Provider: "p", Runtime: "r", Tether: &TetherSubstrateSettings{Launch: "l"}}, "only valid for kind"},
		"launches only is enough": {AgentSubstrateSettings{Kind: AgentSubstrateKindTetherSession, Tether: &TetherSubstrateSettings{Launches: map[string]string{"a": "b"}}}, ""},
		"launch only is enough":   {AgentSubstrateSettings{Kind: AgentSubstrateKindTetherSession, Tether: &TetherSubstrateSettings{Launch: "l", UnreachableTimeout: "30s"}}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			s := DefaultSettings()
			s.AgentSubstrates["tether"] = tc.substrate
			err := s.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Validate = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v, want mention of %q", err, tc.want)
			}
		})
	}
}
