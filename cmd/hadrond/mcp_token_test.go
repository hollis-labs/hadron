package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveMCPTokenSources(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(" file-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	emptyFile := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(emptyFile, []byte(" \n"), 0600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, env string
		args      []string
		want      string
		wantErr   bool
	}{
		{name: "requires principal", wantErr: true},
		{name: "legacy flag", args: []string{"--token", " flag-token "}, want: "flag-token"},
		{name: "environment without argv", env: " env-token\n", want: "env-token"},
		{name: "environment overrides flag", env: "env-token", args: []string{"--token", "flag-token"}, want: "env-token"},
		{name: "blank environment falls back", env: " \n", args: []string{"--token", "flag-token"}, want: "flag-token"},
		{name: "file without token argv", args: []string{"--token-file", tokenFile}, want: "file-token"},
		{name: "file overrides environment and flag", env: "env-token", args: []string{"--token-file", tokenFile, "--token", "flag-token"}, want: "file-token"},
		{name: "missing file fails closed", env: "env-token", args: []string{"--token-file", tokenFile + "-missing"}, wantErr: true},
		{name: "empty file fails closed", env: "env-token", args: []string{"--token-file", emptyFile}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HADRON_MCP_TOKEN", tt.env)
			var file, legacy string
			for i := 0; i < len(tt.args); i += 2 {
				if tt.args[i] == "--token-file" {
					file = tt.args[i+1]
				} else {
					legacy = tt.args[i+1]
				}
			}
			got, err := resolveMCPToken(file, legacy)
			if (err != nil) != tt.wantErr {
				t.Fatalf("unexpected error state: %v", err)
			}
			if got != tt.want {
				t.Fatal("unexpected token source")
			}
		})
	}
}

func TestRunMCPInvalidFileDoesNotFallBackAndScrubsEnvironment(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HADRON_MCP_TOKEN", "env-token")
	err := runMCP([]string{"--token-file", filepath.Join(t.TempDir(), "missing"), "--token", "flag-token"})
	if err == nil {
		t.Fatal("missing file must fail before runtime startup")
	}
	if _, ok := os.LookupEnv("HADRON_MCP_TOKEN"); ok {
		t.Fatal("principal token remains in child environment")
	}
}
