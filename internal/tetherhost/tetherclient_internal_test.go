package tetherhost

import (
	"os"
	"path/filepath"
	"testing"
)

func TestListenAddressExpandsSocketPaths(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home directory")
	}
	for endpoint, want := range map[string]string{
		"":                        "unix:~/.tether/run/tetherd.sock",
		"~/.tether/run/muxd.sock": "unix:" + filepath.Join(home, ".tether/run/muxd.sock"),
		"/var/run/muxd.sock":      "unix:/var/run/muxd.sock",
		"unix:/var/run/muxd.sock": "unix:/var/run/muxd.sock",
		"tcp:127.0.0.1:7777":      "tcp:127.0.0.1:7777",
		"http://127.0.0.1:7777":   "http://127.0.0.1:7777",
	} {
		if got, err := listenAddress(endpoint); err != nil || got != want {
			t.Fatalf("listenAddress(%q) = %q, %v; want %q", endpoint, got, err, want)
		}
	}
}
