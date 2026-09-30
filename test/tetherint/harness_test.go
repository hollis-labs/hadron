//go:build tether_integration

package tetherint

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	tether "github.com/hollis-labs/go-tether-client"
	_ "modernc.org/sqlite" // Read-only child-run lookups in the test's own Hadron database.
)

// Isolation contract (see docs/workflows.md, "Tether integration proof").
//
// Every daemon these tests start is a child process whose environment is
// built from scratch (never inherited wholesale): HOME, TMPDIR and every
// XDG_* directory point inside the test's own temp root, OpenTelemetry export
// is pointed at a closed local port, and muxd gets an explicit --catalog whose
// global.yaml puts the socket, pid file, state DB, workspaces and temp dirs
// inside the same root. Before any process starts, guardPath refuses any path
// outside the root or naming the real user's Tether state. Daemons run in
// their own process group and are killed by group in t.Cleanup; nothing is
// ever killed by name.

const (
	integrationEnv = "HADRON_TETHER_INTEGRATION"
	tetherModule   = "github.com/hollis-labs/tether"
	muxPackage     = tetherModule + "/cmd/mux"
	// liveHadronPort is the default hadrond port; a test daemon never uses it.
	liveHadronPort = 8095

	substrateName = "tether"
	launchID      = "fake-review"
	workflowName  = "agent-review"
	resultMailbox = "msg://agent/hadron/results"
)

var (
	// realHome is the invoking user's home, computed before any child
	// environment is built. It is used only to refuse live paths.
	realHome string
	binDir   string
	muxBin   string
	hadrondB string
	hadronB  string
	// tetherVersion is the pinned Tether module version from this module's
	// go.mod (the `tool` directive).
	tetherVersion string
)

const agentReviewSource = `workflow: {id: agent-review, version: v1}
steps:
  - id: review
    agent_launch:
      substrate: tether
      logical_agent_id: reviewer
      prompt_append: Review the change.
      wait: {timeout: 1h}
outputs:
  verdict:
    type: object
    value: steps.review.outputs.payload.result
`

func TestMain(m *testing.M) {
	if os.Getenv(integrationEnv) != "1" {
		// Tests skip individually; nothing is built or started.
		os.Exit(m.Run())
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		fmt.Fprintln(os.Stderr, "tetherint: cannot resolve the real home directory:", err)
		os.Exit(1)
	}
	realHome = filepath.Clean(home)
	code, err := buildAndRun(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tetherint:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func buildAndRun(m *testing.M) (int, error) {
	dir, err := os.MkdirTemp("", "hti-bin-")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	binDir = dir
	if err := buildBinaries(); err != nil {
		return 0, err
	}
	return m.Run(), nil
}

// buildBinaries builds mux from the pinned Tether version and hadrond/hadron
// from this repository.
//
// mux is built with `go install <pkg>@<version>`, which uses Tether's own
// go.mod and go.sum: the daemon under test has exactly the dependency set
// Tether pins, not one merged with Hadron's by this module's graph. The
// version comes from this module's go.mod, where the `tool` directive pins it.
// hadrond and hadron are built from the repository root so they use Hadron's
// own module graph, as a release would.
func buildBinaries() error {
	moduleDir, err := os.Getwd()
	if err != nil {
		return err
	}
	version, err := goOutput(moduleDir, nil, "list", "-m", "-f", "{{.Version}}", tetherModule)
	if err != nil {
		return fmt.Errorf("resolve pinned tether version: %w", err)
	}
	tetherVersion = strings.TrimSpace(version)
	if tetherVersion == "" {
		return errors.New("pinned tether version is empty")
	}
	if _, err := goOutput(moduleDir, []string{"GOBIN=" + binDir, "GOFLAGS="}, "install", muxPackage+"@"+tetherVersion); err != nil {
		return fmt.Errorf("build mux %s: %w", tetherVersion, err)
	}
	muxBin = filepath.Join(binDir, "mux")
	repoRoot := filepath.Clean(filepath.Join(moduleDir, "..", ".."))
	hadrondB = filepath.Join(binDir, "hadrond")
	hadronB = filepath.Join(binDir, "hadron")
	for output, pkg := range map[string]string{hadrondB: "./cmd/hadrond", hadronB: "./cmd/hadron"} {
		if _, err := goOutput(repoRoot, nil, "build", "-o", output, pkg); err != nil {
			return fmt.Errorf("build %s: %w", pkg, err)
		}
	}
	return nil
}

func goOutput(dir string, extraEnv []string, args ...string) (string, error) {
	command := exec.Command("go", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), extraEnv...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return "", fmt.Errorf("go %s: %w\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return stdout.String(), nil
}

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv(integrationEnv) != "1" {
		t.Skip("set " + integrationEnv + "=1 to run the Tether integration proof")
	}
	if runtime.GOOS == "windows" {
		t.Skip("the fake agent is a POSIX shell script")
	}
}

// knownGapsEnv runs the scenarios that fail against real Tether today.
const knownGapsEnv = "HADRON_TETHER_KNOWN_GAPS"

// knownGap skips a scenario that currently fails against a real muxd because
// of a documented Hadron gap (docs/workflows.md), unless knownGapsEnv=1. The
// scenario still asserts the intended contract, so it passes once the gap is
// closed and the skip is removed.
func knownGap(t *testing.T, gap string) {
	t.Helper()
	requireIntegration(t)
	if os.Getenv(knownGapsEnv) != "1" {
		t.Skipf("known gap: %s (set %s=1 to run it)", gap, knownGapsEnv)
	}
}

// isoEnv is one test's isolated world: a temp root holding a fake HOME, XDG
// dirs, a Tether state root with its catalog, a Hadron data dir and the fake
// agent's control files.
type isoEnv struct {
	t        *testing.T
	root     string
	home     string
	tmp      string
	xdg      map[string]string
	tetherR  string // Tether state root (<root>/.tether)
	catalog  string
	socket   string
	stateDB  string
	agentDir string
	script   string
	hadronD  string // Hadron data dir
	hadronDB string
	hadronL  string
}

func newIsoEnv(t *testing.T) *isoEnv {
	t.Helper()
	requireIntegration(t)
	// Not t.TempDir(): its path embeds the test name and routinely exceeds
	// the ~104-byte sockaddr_un limit for <root>/.tether/run/muxd.sock.
	dir, err := os.MkdirTemp("", "hti")
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() && os.Getenv("HADRON_TETHER_KEEP") == "1" {
			t.Logf("kept temp root %s", root)
			return
		}
		_ = os.RemoveAll(root)
	})
	e := &isoEnv{t: t, root: root}
	e.home = filepath.Join(root, "home")
	e.tmp = filepath.Join(root, "tmp")
	e.xdg = map[string]string{
		"XDG_CONFIG_HOME": filepath.Join(root, "xdg", "config"),
		"XDG_DATA_HOME":   filepath.Join(root, "xdg", "data"),
		"XDG_STATE_HOME":  filepath.Join(root, "xdg", "state"),
		"XDG_CACHE_HOME":  filepath.Join(root, "xdg", "cache"),
		"XDG_RUNTIME_DIR": filepath.Join(root, "xdg", "run"),
	}
	e.tetherR = filepath.Join(root, ".tether")
	e.catalog = filepath.Join(e.tetherR, "catalog")
	e.socket = filepath.Join(e.tetherR, "run", "muxd.sock")
	e.stateDB = filepath.Join(e.tetherR, "state", "tether.db")
	e.agentDir = filepath.Join(root, "agent")
	e.script = filepath.Join(root, "fake-agent.sh")
	e.hadronD = filepath.Join(root, "hadron", "data")
	e.hadronDB = filepath.Join(root, "hadron", "state", "hadron.db")
	e.hadronL = filepath.Join(root, "hadron", "logs")

	e.guardRoot()
	dirs := []string{e.home, e.tmp, e.agentDir, filepath.Join(root, "repo"), e.hadronD, filepath.Join(e.hadronD, "workflows"),
		filepath.Dir(e.hadronDB), e.hadronL}
	for _, sub := range []string{"providers", "projects", "agents", "launches", "boot"} {
		dirs = append(dirs, filepath.Join(e.catalog, sub))
	}
	for _, sub := range []string{"run", "state", "logs", "workspaces", "tmp", "launch-specs"} {
		dirs = append(dirs, filepath.Join(e.tetherR, sub))
	}
	for _, dir := range e.xdg {
		dirs = append(dirs, dir)
	}
	for _, dir := range dirs {
		e.guardPath(dir)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	e.guardPath(e.socket)
	e.guardPath(e.stateDB)
	e.writeCatalog()
	e.writeFakeAgent()
	e.writeHadronConfig()
	return e
}

// liveTetherPaths are the real user's Tether locations no test may touch.
func liveTetherPaths() []string {
	return []string{
		filepath.Join(realHome, ".tether", "run", "muxd.sock"),
		filepath.Join(realHome, ".tether", "catalog"),
		filepath.Join(realHome, ".tether"),
		filepath.Join(realHome, "tether", "state"),
		filepath.Join(realHome, "tether"),
	}
}

func (e *isoEnv) guardRoot() {
	e.t.Helper()
	if realHome == "" {
		e.t.Fatal("real home was not resolved before building the isolated environment")
	}
	if !filepath.IsAbs(e.root) || e.root == realHome || e.root == "/" {
		e.t.Fatalf("unsafe temp root %q", e.root)
	}
	for _, live := range liveTetherPaths() {
		if e.root == live || strings.HasPrefix(e.root+"/", live+"/") {
			e.t.Fatalf("temp root %q is inside live Tether state %q", e.root, live)
		}
	}
}

// guardPath aborts the test unless path is inside the temp root and names
// none of the real user's Tether state.
func (e *isoEnv) guardPath(path string) {
	e.t.Helper()
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) || !strings.HasPrefix(clean, e.root+string(filepath.Separator)) {
		e.t.Fatalf("isolation guard: %q is outside the temp root %q", path, e.root)
	}
	for _, live := range liveTetherPaths() {
		if clean == live || strings.HasPrefix(clean, live+string(filepath.Separator)) {
			e.t.Fatalf("isolation guard: %q names live Tether state %q", path, live)
		}
	}
	// Hardcoded backstop, independent of realHome: the live socket and state
	// shapes may appear only inside this test's root.
	for _, shape := range []string{"/.tether/run/muxd.sock", "/tether/state"} {
		if index := strings.Index(clean, shape); index >= 0 && !strings.HasPrefix(clean[:index]+"/", e.root+"/") {
			e.t.Fatalf("isolation guard: %q contains %q outside the temp root", path, shape)
		}
	}
	if clean == filepath.Join(realHome, ".tether", "run", "muxd.sock") {
		e.t.Fatalf("isolation guard: %q is the live muxd socket", path)
	}
}

// childEnv is the complete environment of every process a test starts.
func (e *isoEnv) childEnv() []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + e.home,
		"TMPDIR=" + e.tmp,
		"LANG=C",
		// Point OpenTelemetry at a closed local port so no test telemetry
		// reaches a live collector.
		"OTEL_SDK_DISABLED=true",
		"OTEL_EXPORTER_OTLP_ENDPOINT=127.0.0.1:9",
	}
	for key, dir := range e.xdg {
		env = append(env, key+"="+dir)
	}
	return env
}

func (e *isoEnv) writeFile(path, content string, mode os.FileMode) {
	e.t.Helper()
	e.guardPath(path)
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		e.t.Fatal(err)
	}
}

// writeCatalog writes a minimal Tether catalog (modeled on Tether's
// internal/setup seed global.yaml) with every path inside the temp root, and
// one launch whose provider command is the fake agent.
//
// The provider is brand "claude" with runtime_kind "streaming-stdio" — the
// same provider/runtime the seeded claude-code provider uses — because it is
// the runtime whose process lifetime is the session lifetime: the child is
// spawned once at launch, its exit ends the session (completed, or failed
// with its exit code), Stop kills its process group, and the composed boot
// prompt (Hadron's PromptAppend included) is passed on its argv. The
// subprocess runtimes are per-turn instead: a session stays "running" after
// the process exits, which cannot model an agent that finishes without a
// reply.
func (e *isoEnv) writeCatalog() {
	e.t.Helper()
	e.writeFile(filepath.Join(e.catalog, "global.yaml"), fmt.Sprintf(`version: 1.0.0
catalog:
  roots:
    projects: projects
    agents: agents
    providers: providers
    launches: launches
    boot: boot
  defaults:
    state_db: %[1]s/state/tether.db
    workspace_root: %[1]s/workspaces
    temp_root: %[1]s/tmp
    launch_specs_root: %[1]s/launch-specs
daemon:
  listen_addr: unix:%[1]s/run/muxd.sock
  pid_file: %[1]s/run/muxd.pid
  shutdown_timeout: 5s
`, e.tetherR), 0o600)
	e.writeFile(filepath.Join(e.catalog, "providers", "fake-agent.yaml"), fmt.Sprintf(`id: fake-agent
type: cli
provider: claude
runtime_kind: streaming-stdio
command: %s
args: []
bootstrap:
  mode: streaming-stdio
env:
  mode: merge
`, e.script), 0o600)
	e.writeFile(filepath.Join(e.catalog, "projects", "hadron-int.yaml"), fmt.Sprintf(`id: hadron-int
name: Hadron integration
repo_root: %s
workspace:
  default_mode: shared
`, filepath.Join(e.root, "repo")), 0o600)
	e.writeFile(filepath.Join(e.catalog, "agents", "fake-reviewer.yaml"), "id: fake-reviewer\n", 0o600)
	e.writeFile(filepath.Join(e.catalog, "launches", launchID+".yaml"), `id: `+launchID+`
project: hadron-int
agent: fake-reviewer
provider: fake-agent
workspace:
  mode: shared
`, 0o600)
}

// writeFakeAgent writes the provider command. muxd runs it with the composed
// boot prompt on its argv; it finds Hadron's result contract there, records
// the start, and acts on the mode in <agent>/mode:
//
//	reply    reply at once, then stay alive until stopped
//	exit     exit 0 without replying
//	block    never reply; stay alive until stopped
//	trigger  wait for <agent>/trigger, reply, then stay alive
//
// It replies with the real `mux messages send` against the test catalog.
func (e *isoEnv) writeFakeAgent() {
	e.t.Helper()
	e.writeFile(e.script, fmt.Sprintf(`#!/bin/sh
# Hadron Tether integration fake agent. Generated per test; every path below
# is inside the test's temp root.
AGENT_DIR='%[1]s'
MUX='%[2]s'
CATALOG='%[3]s'
prompt="$AGENT_DIR/prompt.$$"
printf '%%s\n' "$@" > "$prompt"
thread=$(sed -n 's/^  thread_id: //p' "$prompt" | head -n 1)
nonce=$(sed -n 's/.*"nonce": "\([0-9a-f]\{64\}\)".*/\1/p' "$prompt" | head -n 1)
mode=$(cat "$AGENT_DIR/mode" 2>/dev/null || echo reply)
printf '%%s %%s %%s\n' "$$" "$mode" "$thread" >> "$AGENT_DIR/starts"
if [ -z "$thread" ] || [ -z "$nonce" ]; then
  echo "$$ no-contract" >> "$AGENT_DIR/events"
  exit 3
fi
reply() {
  if "$MUX" --catalog "$CATALOG" messages send --kind response \
      --from msg://agent/local/fake-reviewer --to %[4]s --thread "$thread" \
      --payload-json "{\"nonce\":\"$nonce\",\"result\":{\"ok\":true,\"agent_pid\":$$}}" >> "$AGENT_DIR/send.log" 2>&1; then
    echo "$$ replied" >> "$AGENT_DIR/events"
  else
    echo "$$ reply-failed" >> "$AGENT_DIR/events"
  fi
}
stay() {
  while :; do sleep 1; done
}
case "$mode" in
  reply) reply; stay ;;
  exit) echo "$$ exited" >> "$AGENT_DIR/events"; exit 0 ;;
  block) stay ;;
  trigger)
    while [ ! -f "$AGENT_DIR/trigger" ]; do sleep 0.2; done
    reply
    stay ;;
  *) echo "$$ unknown-mode $mode" >> "$AGENT_DIR/events"; exit 4 ;;
esac
`, e.agentDir, muxBin, e.catalog, resultMailbox), 0o700)
	e.t.Cleanup(e.killFakeAgents)
}

func (e *isoEnv) setAgentMode(mode string) {
	e.t.Helper()
	e.writeFile(filepath.Join(e.agentDir, "mode"), mode+"\n", 0o600)
}

func (e *isoEnv) triggerReply() {
	e.t.Helper()
	e.writeFile(filepath.Join(e.agentDir, "trigger"), "go\n", 0o600)
}

// agentStart is one line of the fake agent's start log.
type agentStart struct {
	PID    int
	Mode   string
	Thread string
}

func (e *isoEnv) agentStarts() []agentStart {
	e.t.Helper()
	var starts []agentStart
	for _, line := range readLines(filepath.Join(e.agentDir, "starts")) {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		start := agentStart{PID: pid, Mode: fields[1]}
		if len(fields) > 2 {
			start.Thread = fields[2]
		}
		starts = append(starts, start)
	}
	return starts
}

func (e *isoEnv) agentEvents() []string { return readLines(filepath.Join(e.agentDir, "events")) }

func readLines(path string) []string {
	file, err := os.Open(path) // #nosec G304 -- test-owned temp file.
	if err != nil {
		return nil
	}
	defer func() { _ = file.Close() }()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// waitAgentStarted waits for the n-th fake agent start.
func (e *isoEnv) waitAgentStarted(n int) agentStart {
	e.t.Helper()
	var starts []agentStart
	eventually(e.t, 60*time.Second, "fake agent start", func() bool {
		starts = e.agentStarts()
		return len(starts) >= n
	})
	return starts[n-1]
}

func (e *isoEnv) waitAgentEvent(suffix string) {
	e.t.Helper()
	eventually(e.t, 60*time.Second, "fake agent event "+suffix, func() bool {
		for _, event := range e.agentEvents() {
			if strings.HasSuffix(event, " "+suffix) {
				return true
			}
		}
		return false
	})
}

// killFakeAgents stops fake agents that outlived muxd (a muxd shutdown does
// not kill session processes). It only signals PIDs this test's fake agent
// recorded, and only while their command line still names this test's script.
func (e *isoEnv) killFakeAgents() {
	for _, start := range e.agentStarts() {
		out, err := exec.Command("ps", "-o", "pgid=,command=", "-p", strconv.Itoa(start.PID)).Output() // #nosec G204 -- fixed ps argv.
		if err != nil {
			continue
		}
		fields := strings.Fields(string(out))
		if len(fields) < 2 || !strings.Contains(string(out), e.script) {
			continue
		}
		if pgid, err := strconv.Atoi(fields[0]); err == nil && pgid == start.PID {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		} else {
			_ = syscall.Kill(start.PID, syscall.SIGKILL)
		}
	}
}

// writeHadronConfig writes hadrond's settings.json (one tether_session
// substrate on the test socket) and the agent workflow.
func (e *isoEnv) writeHadronConfig() {
	e.t.Helper()
	settings := map[string]any{
		"agent_substrates": map[string]any{
			substrateName: map[string]any{
				"kind":   "tether_session",
				"tether": map[string]any{"endpoint": e.socket, "launch": launchID},
			},
		},
	}
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		e.t.Fatal(err)
	}
	e.writeFile(filepath.Join(e.hadronD, "settings.json"), string(data), 0o600)
	e.writeFile(e.workflowPath(), agentReviewSource, 0o600)
}

func (e *isoEnv) workflowPath() string {
	return filepath.Join(e.hadronD, "workflows", workflowName+".workflow.yaml")
}

// daemon is a started child process in its own process group.
type daemon struct {
	name    string
	cmd     *exec.Cmd
	logPath string
	done    chan struct{}
}

func (e *isoEnv) spawn(name string, args ...string) *daemon {
	e.t.Helper()
	logPath := filepath.Join(e.root, fmt.Sprintf("%s-%d.log", name, time.Now().UnixNano()))
	e.guardPath(logPath)
	logFile, err := os.Create(logPath) // #nosec G304 -- path is inside the guarded temp root.
	if err != nil {
		e.t.Fatal(err)
	}
	command := exec.Command(args[0], args[1:]...) // #nosec G204 -- test-built binaries with guarded paths.
	command.Dir = e.root
	command.Env = e.childEnv()
	command.Stdout, command.Stderr = logFile, logFile
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		e.t.Fatalf("start %s: %v", name, err)
	}
	d := &daemon{name: name, cmd: command, logPath: logPath, done: make(chan struct{})}
	go func() {
		_ = command.Wait()
		_ = logFile.Close()
		close(d.done)
	}()
	e.t.Cleanup(func() { d.kill() })
	return d
}

func (d *daemon) exited() bool {
	select {
	case <-d.done:
		return true
	default:
		return false
	}
}

// stop sends SIGTERM to the daemon's process group and waits, escalating to
// SIGKILL after grace.
func (d *daemon) stop(grace time.Duration) {
	if d.exited() {
		return
	}
	_ = syscall.Kill(-d.cmd.Process.Pid, syscall.SIGTERM)
	select {
	case <-d.done:
	case <-time.After(grace):
		d.kill()
	}
}

func (d *daemon) kill() {
	if d.exited() {
		return
	}
	_ = syscall.Kill(-d.cmd.Process.Pid, syscall.SIGKILL)
	<-d.done
}

func (d *daemon) log() string {
	data, _ := os.ReadFile(d.logPath)
	var kept []string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, "traces export") || strings.Contains(line, "internal_logging.go") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// muxd is the test Tether daemon.
type muxd struct {
	env *isoEnv
	d   *daemon
}

func (e *isoEnv) startMuxd() *muxd {
	e.t.Helper()
	m := &muxd{env: e}
	m.start()
	return m
}

func (m *muxd) start() {
	e := m.env
	e.t.Helper()
	e.guardPath(e.catalog)
	e.guardPath(e.socket)
	m.d = e.spawn("muxd", muxBin, "daemon", "run", "--catalog", e.catalog)
	client := e.tetherClient()
	eventually(e.t, 30*time.Second, "muxd health", func() bool {
		if m.d.exited() {
			e.t.Fatalf("muxd exited during startup:\n%s", m.d.log())
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return client.Ping(ctx) == nil
	})
	if _, err := os.Stat(e.socket); err != nil {
		e.t.Fatalf("muxd is healthy but its socket is not at %s: %v", e.socket, err)
	}
}

// stop shuts muxd down gracefully (SIGTERM). muxd waits up to the catalog's
// shutdown_timeout for sessions and then exits without killing them.
func (m *muxd) stop() { m.d.stop(30 * time.Second) }

func (m *muxd) restart() {
	m.stop()
	m.start()
}

func (e *isoEnv) tetherClient() *tether.Client {
	e.t.Helper()
	e.guardPath(e.socket)
	client, err := tether.New("unix:" + e.socket)
	if err != nil {
		e.t.Fatal(err)
	}
	return client
}

func (e *isoEnv) sessions() []tether.Session {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listed, err := e.tetherClient().ListSessions(ctx, tether.ListSessionsOptions{Limit: 100})
	if err != nil {
		e.t.Fatalf("list tether sessions: %v", err)
	}
	return listed.Sessions
}

func (e *isoEnv) session(id string) tether.Session {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	session, err := e.tetherClient().GetSession(ctx, id)
	if err != nil {
		e.t.Fatalf("get tether session %s: %v", id, err)
	}
	return session
}

// onlySession returns the single Tether session, failing unless exactly one
// exists.
func (e *isoEnv) onlySession() tether.Session {
	e.t.Helper()
	sessions := e.sessions()
	if len(sessions) != 1 {
		e.t.Fatalf("tether sessions = %d, want exactly 1: %#v", len(sessions), sessions)
	}
	return sessions[0]
}

func (e *isoEnv) waitSessionState(id string, states ...string) tether.Session {
	e.t.Helper()
	var session tether.Session
	eventually(e.t, 60*time.Second, "tether session "+id+" in "+strings.Join(states, "|"), func() bool {
		session = e.session(id)
		for _, state := range states {
			if session.State == state {
				return true
			}
		}
		return false
	})
	return session
}

// waitStopped waits until a stopped session is terminal in muxd and its
// agent process is gone.
//
// A stop is persisted as state "completed" with the killed process's exit
// code, never "killed": agentkit's session manager records a stopped session
// as done and emits its state change with no reason, so Tether's mapping of a
// "killed" reason to state "killed" (internal/app/sinks.go) never fires.
func (e *isoEnv) waitStopped(id string, pid int) tether.Session {
	e.t.Helper()
	session := e.waitSessionState(id, "completed", "killed", "failed")
	eventually(e.t, 30*time.Second, "agent process "+strconv.Itoa(pid)+" to exit", func() bool {
		return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
	})
	return session
}

// hadrond is the test Hadron daemon, built from this repository.
type hadrond struct {
	env  *isoEnv
	addr string
	d    *daemon
}

func (e *isoEnv) startHadrond() *hadrond {
	e.t.Helper()
	port := freePort(e.t)
	if port == liveHadronPort {
		e.t.Fatalf("refusing the live hadrond port %d", port)
	}
	for _, path := range []string{e.hadronD, e.hadronDB, e.hadronL} {
		e.guardPath(path)
	}
	h := &hadrond{env: e, addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}
	h.d = e.spawn("hadrond", hadrondB, "serve", "-addr", h.addr, "-db", e.hadronDB, "-logs", e.hadronL, "-data", e.hadronD)
	eventually(e.t, 30*time.Second, "hadrond health", func() bool {
		if h.d.exited() {
			e.t.Fatalf("hadrond exited during startup:\n%s", h.d.log())
		}
		response, err := http.Get("http://" + h.addr + "/v1/health") // #nosec G107 -- loopback test daemon.
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	})
	return h
}

// stop shuts hadrond down gracefully (SIGTERM).
func (h *hadrond) stop() { h.d.stop(30 * time.Second) }

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address %T", listener.Addr())
	}
	return addr.Port
}

// cli runs the hadron CLI against this daemon with the isolated environment.
func (h *hadrond) cli(args ...string) (string, error) {
	h.env.t.Helper()
	if strings.HasSuffix(h.addr, ":"+strconv.Itoa(liveHadronPort)) {
		h.env.t.Fatalf("refusing the live hadrond address %s", h.addr)
	}
	command := exec.Command(hadronB, append([]string{"--addr", "http://" + h.addr}, args...)...) // #nosec G204 -- test-built binary.
	command.Dir = h.env.root
	command.Env = h.env.childEnv()
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return stdout.String(), fmt.Errorf("hadron %s: %w: %s", strings.Join(args, " "), err, stderr.String())
	}
	return stdout.String(), nil
}

func (h *hadrond) runWorkflow(runID string) {
	h.env.t.Helper()
	if _, err := h.cli("workflow", "run", h.env.workflowPath(), "--run-id", runID, "--idempotency-key", runID, "--confirm", "--json"); err != nil {
		h.env.t.Fatal(err)
	}
}

func (h *hadrond) cancel(runID string) {
	h.env.t.Helper()
	if _, err := h.cli("workflow", "cancel", runID, "--reason", "integration cancel", "--idempotency-key", "cancel-"+runID, "--json"); err != nil {
		h.env.t.Fatal(err)
	}
}

// inspection is the part of `hadron workflow inspect --json` the tests read.
type inspection struct {
	Run struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	} `json:"run"`
	Nodes []struct {
		ID struct {
			NodeID string `json:"node_id"`
		} `json:"id"`
		Status   string `json:"status"`
		Attempts []struct {
			Number  int    `json:"number"`
			Status  string `json:"status"`
			Failure *struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"failure"`
		} `json:"attempts"`
	} `json:"nodes"`
	Values []struct {
		Roles  []string                   `json:"roles"`
		Values map[string]json.RawMessage `json:"values"`
	} `json:"values"`
}

func (h *hadrond) inspect(runID string) (inspection, error) {
	out, err := h.cli("workflow", "inspect", runID, "--reveal-private", "--json")
	if err != nil {
		return inspection{}, err
	}
	var result inspection
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		return inspection{}, fmt.Errorf("decode inspect: %w\n%s", err, out)
	}
	return result, nil
}

func terminal(status string) bool {
	return status == "succeeded" || status == "failed" || status == "canceled"
}

func (h *hadrond) waitTerminal(runID string, timeout time.Duration) inspection {
	h.env.t.Helper()
	var last inspection
	var lastErr error
	deadline := time.Now().Add(timeout)
	for {
		last, lastErr = h.inspect(runID)
		if lastErr == nil && terminal(last.Run.Status) {
			return last
		}
		if time.Now().After(deadline) {
			h.env.t.Fatalf("run %s not terminal after %s: status=%q err=%v\n%s", runID, timeout, last.Run.Status, lastErr, h.describe(runID))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// node returns a node's status and its latest attempt's failure code.
func (i inspection) node(id string) (status, failure string, ok bool) {
	for _, node := range i.Nodes {
		if node.ID.NodeID != id {
			continue
		}
		if n := len(node.Attempts); n > 0 && node.Attempts[n-1].Failure != nil {
			failure = node.Attempts[n-1].Failure.Code
		}
		return node.Status, failure, true
	}
	return "", "", false
}

// output decodes a run output value's payload.
func (i inspection) output(t *testing.T, name string) any {
	t.Helper()
	for _, set := range i.Values {
		for _, role := range set.Roles {
			if role != "run.outputs" {
				continue
			}
			raw, ok := set.Values[name]
			if !ok {
				continue
			}
			var value struct {
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Fatalf("decode output %s: %v", name, err)
			}
			var payload any
			decoder := json.NewDecoder(bytes.NewReader(value.Payload))
			decoder.UseNumber()
			if err := decoder.Decode(&payload); err != nil {
				t.Fatalf("decode output %s payload %s: %v", name, value.Payload, err)
			}
			return payload
		}
	}
	t.Fatalf("run %s has no output %q", i.Run.ID, name)
	return nil
}

// childRunID returns the agent_launch child run (the one holding the
// agent_session "session" step). Child links are read from the test's own
// Hadron database; the inspect API does not expose them unmasked.
func (h *hadrond) childRunID(runID string) string {
	h.env.t.Helper()
	h.env.guardPath(h.env.hadronDB)
	db, err := sql.Open("sqlite", "file:"+h.env.hadronDB+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)")
	if err != nil {
		h.env.t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var child string
	eventually(h.env.t, 30*time.Second, "child run of "+runID, func() bool {
		return db.QueryRow(`SELECT child_run_id FROM workflow_child_runs WHERE parent_run_id = ?`, runID).Scan(&child) == nil
	})
	return child
}

// sessionStep inspects the child run's agent_session step.
func (h *hadrond) sessionStep(runID string) (status, failure string) {
	h.env.t.Helper()
	child, err := h.inspect(h.childRunID(runID))
	if err != nil {
		h.env.t.Fatal(err)
	}
	status, failure, ok := child.node("session")
	if !ok {
		h.env.t.Fatalf("child run has no session step: %#v", child.Nodes)
	}
	return status, failure
}

// waitSessionPending waits until the agent_session step has launched and
// recorded its external operation (status waiting).
func (h *hadrond) waitSessionPending(runID string) {
	h.env.t.Helper()
	child := h.childRunID(runID)
	eventually(h.env.t, 60*time.Second, "session step pending in "+child, func() bool {
		inspected, err := h.inspect(child)
		if err != nil {
			return false
		}
		status, _, ok := inspected.node("session")
		return ok && status == "waiting"
	})
}

func (h *hadrond) describe(runID string) string {
	var report strings.Builder
	for _, id := range []string{runID, ""} {
		if id == "" {
			db, err := sql.Open("sqlite", "file:"+h.env.hadronDB+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)")
			if err != nil {
				break
			}
			_ = db.QueryRow(`SELECT child_run_id FROM workflow_child_runs WHERE parent_run_id = ?`, runID).Scan(&id)
			_ = db.Close()
			if id == "" {
				break
			}
		}
		inspected, err := h.inspect(id)
		if err != nil {
			fmt.Fprintf(&report, "inspect %s: %v\n", id, err)
			continue
		}
		fmt.Fprintf(&report, "run %s status=%s\n", id, inspected.Run.Status)
		for _, node := range inspected.Nodes {
			status, failure, _ := inspected.node(node.ID.NodeID)
			fmt.Fprintf(&report, "  node %s status=%s failure=%s\n", node.ID.NodeID, status, failure)
		}
	}
	if response, err := http.Get("http://" + h.addr + "/v1/health"); err == nil { // #nosec G107 -- loopback test daemon.
		var body bytes.Buffer
		_, _ = body.ReadFrom(response.Body)
		_ = response.Body.Close()
		fmt.Fprintf(&report, "hadrond health: %d %s\n", response.StatusCode, strings.TrimSpace(body.String()))
	}
	fmt.Fprintf(&report, "--- hadrond log ---\n%s\n", h.d.log())
	return report.String()
}

// eventually polls cond every 100ms until it holds or timeout elapses.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// never asserts cond stays false for the whole window.
func never(t *testing.T, window time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if cond() {
			t.Fatalf("%s happened within %s", what, window)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
