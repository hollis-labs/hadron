package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const defaultDaemonAddress = "127.0.0.1:8095"

// desktopApp is deliberately a lifecycle-only wrapper. It starts or adopts
// hadrond and opens the daemon-served UI; workflow data and operations never
// cross this process boundary.
type desktopApp struct {
	address string
	client  *http.Client
}

func newDesktopApp(address string) (*desktopApp, error) {
	webURL, err := daemonWebURL(address)
	if err != nil {
		return nil, err
	}
	return &desktopApp{address: webURL.Host, client: &http.Client{Timeout: 750 * time.Millisecond}}, nil
}

func (a *desktopApp) run(ctx context.Context, open bool) error {
	webURL, _ := daemonWebURL(a.address)
	var daemon *exec.Cmd
	if !a.healthy(ctx, webURL) {
		binary, err := findDaemonBinary()
		if err != nil {
			return errors.New("hadrond is not running and was not found beside hadron-app or on PATH")
		}
		daemon = exec.CommandContext(ctx, binary, "serve", "--addr", a.address) // #nosec G204 -- binary is resolved beside this executable or from PATH; address is validated by daemonWebURL.
		daemon.Stdout, daemon.Stderr = os.Stdout, os.Stderr
		if err := daemon.Start(); err != nil {
			return fmt.Errorf("start hadrond: %w", err)
		}
		if err := a.waitHealthy(ctx, webURL, 10*time.Second); err != nil {
			_ = daemon.Process.Kill()
			_ = daemon.Wait()
			return err
		}
	}

	if open {
		if err := openBrowser(webURL.String()); err != nil {
			if daemon != nil {
				_ = daemon.Process.Kill()
				_ = daemon.Wait()
			}
			return err
		}
	}
	fmt.Printf("Hadron operator UI: %s\n", webURL.String())
	if daemon == nil {
		return nil
	}
	err := daemon.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func daemonWebURL(address string) (*url.URL, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		address = defaultDaemonAddress
	}
	if strings.Contains(address, "://") {
		return nil, errors.New("daemon address must be host:port, not a URL")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return nil, fmt.Errorf("invalid daemon address %q", address)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, fmt.Errorf("invalid daemon address %q", address)
	}
	return &url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: "/"}, nil
}

func findDaemonBinary() (string, error) {
	executable, _ := os.Executable()
	return resolveDaemonBinary(executable, os.Stat, exec.LookPath)
}

func resolveDaemonBinary(
	executable string,
	stat func(string) (fs.FileInfo, error),
	lookPath func(string) (string, error),
) (string, error) {
	name := "hadrond"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	if executable != "" {
		sibling := filepath.Join(filepath.Dir(executable), name)
		if info, err := stat(sibling); err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return sibling, nil
		}
	}
	return lookPath(name)
}

func (a *desktopApp) healthy(ctx context.Context, webURL *url.URL) bool {
	health := *webURL
	health.Path = "/v1/health"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, health.String(), nil)
	if err != nil {
		return false
	}
	response, err := a.client.Do(request)
	if err != nil {
		return false
	}
	defer func() { _ = response.Body.Close() }()
	return response.StatusCode == http.StatusOK
}

func (a *desktopApp) waitHealthy(ctx context.Context, webURL *url.URL, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if a.healthy(ctx, webURL) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("hadrond did not become healthy within 10 seconds")
		case <-ticker.C:
		}
	}
}

func openBrowser(target string) error {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		command = exec.Command("open", target) // #nosec G204 -- target is a validated local HTTP URL.
	case "windows":
		command = exec.Command("rundll32", "url.dll,FileProtocolHandler", target) // #nosec G204 -- target is a validated local HTTP URL.
	default:
		command = exec.Command("xdg-open", target) // #nosec G204 -- target is a validated local HTTP URL.
	}
	if err := command.Start(); err != nil {
		return fmt.Errorf("open browser: %w", err)
	}
	return nil
}
