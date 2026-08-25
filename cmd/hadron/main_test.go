package main

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestPublicRootCommandsAreGraphNativeOrProcessOperations(t *testing.T) {
	root := buildRootCommand()
	commands := root.Commands()
	got := make([]string, 0, len(commands))
	for _, command := range commands {
		got = append(got, command.Name())
	}
	sort.Strings(got)
	want := []string{"build", "daemon", "version", "workflow", "workspace"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("public root commands = %#v, want %#v", got, want)
	}
	for _, legacy := range []string{"run", "pipeline", "schedule", "trigger", "registry", "validate", "lint", "fmt", "pack", "testgen", "gate", "message"} {
		if found, _, err := root.Find([]string{legacy}); err == nil && found != root {
			t.Fatalf("legacy root command %q remains reachable as %q", legacy, found.CommandPath())
		}
	}

	var rendered bytes.Buffer
	root.SetOut(&rendered)
	root.SetErr(&rendered)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	help := rendered.String()
	for _, command := range want {
		if !strings.Contains(help, "\n  "+command+" ") {
			t.Fatalf("visible help omits %q:\n%s", command, help)
		}
	}
	for _, legacy := range []string{"run", "pipeline", "schedule", "trigger", "registry", "validate", "lint", "fmt", "pack", "testgen", "gate", "message", "completion"} {
		if strings.Contains(help, "\n  "+legacy+" ") {
			t.Fatalf("visible help exposes legacy/default command %q:\n%s", legacy, help)
		}
	}
}
