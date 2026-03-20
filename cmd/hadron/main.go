package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/lint"
	"github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	globalAddr string
	httpClient = &http.Client{Timeout: 30 * time.Second}
)

func main() {
	root := &cobra.Command{
		Use:   "hadron",
		Short: "Hadron blueprint automation runner CLI",
		Long:  "hadron is the CLI client for the hadrond daemon.",
	}

	root.PersistentFlags().StringVar(&globalAddr, "addr", "http://"+config.DefaultAddr, "daemon base URL")

	root.AddCommand(
		buildRunCmd(),
		buildValidateCmd(),
		buildBlueprintCmd(),
		buildScheduleCmd(),
		buildPipelineCmd(),
		buildWorkspaceCmd(),
		buildDaemonCmd(),
		buildLintCmd(),
		buildFmtCmd(),
		buildPluginCmd(),
		buildTestGenCmd(),
		buildAgentCardCmd(),
		buildRegistryCmd(),
		buildTriggerCmd(),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// ── run ───────────────────────────────────────────────────────────────────────

func buildRunCmd() *cobra.Command {
	var (
		inputs      []string
		workspaceID string
		dryRun      bool
	)

	cmd := &cobra.Command{
		Use:   "run <blueprint-path>",
		Short: "Enqueue and stream a blueprint run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bpPath := args[0]

			// If the file doesn't exist at the literal path, try resolving
			// against the configured blueprint_dir from settings.
			if !filepath.IsAbs(bpPath) {
				if _, err := os.Stat(bpPath); os.IsNotExist(err) {
					cfg := config.Default()
					sett, loadErr := settings.Load(cfg.DataDir)
					if loadErr == nil && sett.BlueprintDir != "" {
						candidate := filepath.Join(sett.BlueprintDir, bpPath)
						if _, statErr := os.Stat(candidate); statErr == nil {
							bpPath = candidate
						}
					}
				}
			}

			parsedInputs, err := parseInputs(inputs)
			if err != nil {
				return err
			}

			body := map[string]any{
				"blueprint_path": bpPath,
				"workspace_id":   workspaceID,
				"inputs":         parsedInputs,
				"dry_run":        dryRun,
			}

			var result map[string]any
			if err := postJSON(globalAddr+"/v1/runs", body, &result); err != nil {
				return err
			}

			runID, _ := result["id"].(string)
			if runID == "" {
				return fmt.Errorf("unexpected response: missing run id")
			}
			fmt.Printf("run %s queued\n", runID)

			return streamRunEvents(runID, result)
		},
	}

	cmd.Flags().StringArrayVar(&inputs, "input", nil, "input key=value (repeatable)")
	cmd.Flags().StringVar(&workspaceID, "workspace", "default", "workspace ID")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "dry run (no commands executed)")
	return cmd
}

func streamRunEvents(runID string, initialRun map[string]any) error {
	var lastCursor string
	deadline := time.Now().Add(10 * time.Minute)

	for time.Now().Before(deadline) {
		// Fetch run status
		var run map[string]any
		if err := httpGet(globalAddr+"/v1/runs/"+runID, &run); err != nil {
			return err
		}
		status, _ := run["status"].(string)

		// Fetch new events
		url := globalAddr + "/v1/runs/" + runID + "/events?limit=100"
		if lastCursor != "" {
			url += "&cursor=" + lastCursor
		}
		var eventsResp map[string]any
		if err := httpGet(url, &eventsResp); err != nil {
			return err
		}

		items, _ := eventsResp["items"].([]any)
		for _, item := range items {
			ev, _ := item.(map[string]any)
			evType, _ := ev["event_type"].(string)
			msg, _ := ev["message"].(string)
			if msg != "" {
				fmt.Printf("[%s] %s\n", evType, msg)
			} else {
				fmt.Printf("[%s]\n", evType)
			}
		}

		if nc, ok := eventsResp["next_cursor"]; ok && nc != nil {
			if s, ok := nc.(string); ok {
				lastCursor = s
			}
		}

		if status == "success" || status == "done" {
			fmt.Println("run completed successfully")
			return nil
		}
		if status == "failed" {
			errMsg, _ := run["error_message"].(string)
			if errMsg != "" {
				fmt.Fprintf(os.Stderr, "run failed: %s\n", errMsg)
			} else {
				fmt.Fprintln(os.Stderr, "run failed")
			}
			os.Exit(1)
		}

		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for run %s", runID)
}

// ── validate ──────────────────────────────────────────────────────────────────

func buildValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate <blueprint-path>",
		Short: "Validate a blueprint file via the daemon",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read blueprint: %w", err)
			}

			resp, err := httpClient.Post(globalAddr+"/v1/blueprints/validate",
				"application/octet-stream", bytes.NewReader(data))
			if err != nil {
				return fmt.Errorf("request failed: %w", err)
			}
			defer resp.Body.Close()

			var result map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return err
			}

			valid, _ := result["valid"].(bool)
			if valid {
				fmt.Println("valid")
				return nil
			}
			errMsg, _ := result["error"].(string)
			if errMsg == "" {
				errMsg = "invalid blueprint"
			}
			fmt.Fprintln(os.Stderr, "invalid:", errMsg)
			os.Exit(1)
			return nil
		},
	}
}

// ── blueprint ─────────────────────────────────────────────────────────────────

func buildBlueprintCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "blueprint",
		Short: "Blueprint file operations (local)",
	}

	var dir string
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List blueprint files in a directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dir == "" {
				dir = "."
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				return err
			}
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				name := e.Name()
				ext := strings.ToLower(filepath.Ext(name))
				if ext != ".yaml" && ext != ".yml" && ext != ".json" {
					continue
				}
				path := filepath.Join(dir, name)
				_, parseErr := blueprint.ParseFile(path)
				status := "valid"
				if parseErr != nil {
					status = "invalid"
				}
				fmt.Printf("%-50s %s\n", path, status)
			}
			return nil
		},
	}
	listCmd.Flags().StringVar(&dir, "dir", ".", "directory to scan")

	showCmd := &cobra.Command{
		Use:   "show <path>",
		Short: "Print parsed blueprint summary",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bp, err := blueprint.ParseFile(args[0])
			if err != nil {
				return err
			}
			fmt.Printf("name:    %s\n", bp.Spec.Name)
			fmt.Printf("version: %s\n", bp.Version)
			if len(bp.Inputs) > 0 {
				fmt.Println("inputs:")
				for _, inp := range bp.Inputs {
					fmt.Printf("  %s (%s)\n", inp.Name, inp.Type)
				}
			}
			fmt.Printf("sections: %d\n", len(bp.Steps))
			for _, step := range bp.Steps {
				fmt.Printf("  %s: %d steps\n", step.Section, len(step.Steps))
			}
			return nil
		},
	}

	cmd.AddCommand(listCmd, showCmd)
	return cmd
}

// ── schedule ──────────────────────────────────────────────────────────────────

func buildScheduleCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Manage schedules",
	}

	var wsID string

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List schedules",
		RunE: func(cmd *cobra.Command, args []string) error {
			url := globalAddr + "/v1/schedules"
			if wsID != "" {
				url += "?workspace_id=" + wsID
			}
			var result map[string]any
			if err := httpGet(url, &result); err != nil {
				return err
			}
			items, _ := result["items"].([]any)
			if len(items) == 0 {
				fmt.Println("no schedules")
				return nil
			}
			for _, item := range items {
				sc, _ := item.(map[string]any)
				fmt.Printf("%s  %s  %s  enabled=%v\n",
					sc["id"], sc["blueprint_path"], sc["cron_expr"], sc["enabled"])
			}
			return nil
		},
	}
	listCmd.Flags().StringVar(&wsID, "workspace", "", "workspace ID")

	var (
		bpPath   string
		cronExpr string
		name     string
		createWS string
	)
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a schedule",
		RunE: func(cmd *cobra.Command, args []string) error {
			if bpPath == "" {
				return fmt.Errorf("--blueprint is required")
			}
			if cronExpr == "" {
				return fmt.Errorf("--cron is required")
			}
			body := map[string]any{
				"blueprint_path": bpPath,
				"cron_expr":      cronExpr,
				"workspace_id":   createWS,
				"name":           name,
				"enabled":        true,
			}
			var result map[string]any
			if err := postJSON(globalAddr+"/v1/schedules", body, &result); err != nil {
				return err
			}
			fmt.Printf("created schedule %s\n", result["id"])
			return nil
		},
	}
	createCmd.Flags().StringVar(&bpPath, "blueprint", "", "blueprint path (required)")
	createCmd.Flags().StringVar(&cronExpr, "cron", "", "cron expression (required)")
	createCmd.Flags().StringVar(&name, "name", "", "schedule name")
	createCmd.Flags().StringVar(&createWS, "workspace", "default", "workspace ID")

	enableCmd := &cobra.Command{
		Use:   "enable <id>",
		Short: "Enable a schedule",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return patchScheduleEnabled(args[0], true)
		},
	}

	disableCmd := &cobra.Command{
		Use:   "disable <id>",
		Short: "Disable a schedule",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return patchScheduleEnabled(args[0], false)
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a schedule",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req, _ := http.NewRequest(http.MethodDelete, globalAddr+"/v1/schedules/"+args[0], nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				fmt.Println("deleted")
				return nil
			}
			return printAPIError(resp)
		},
	}

	cmd.AddCommand(listCmd, createCmd, enableCmd, disableCmd, deleteCmd)
	return cmd
}

func patchScheduleEnabled(id string, enabled bool) error {
	body := map[string]any{"enabled": enabled}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPatch, globalAddr+"/v1/schedules/"+id, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		fmt.Printf("schedule %s: enabled=%v\n", id, enabled)
		return nil
	}
	return printAPIError(resp)
}

// ── pipeline ──────────────────────────────────────────────────────────────────

func buildPipelineCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pipeline",
		Short: "Manage pipeline runs",
	}

	var wsID string
	runCmd := &cobra.Command{
		Use:   "run <pipeline-path>",
		Short: "Start a pipeline run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body := map[string]any{
				"pipeline_path": args[0],
				"workspace_id":  wsID,
			}
			var result map[string]any
			if err := postJSON(globalAddr+"/v1/pipelines", body, &result); err != nil {
				return err
			}
			fmt.Printf("pipeline run %s queued\n", result["id"])
			return nil
		},
	}
	runCmd.Flags().StringVar(&wsID, "workspace", "default", "workspace ID")

	cmd.AddCommand(runCmd)
	return cmd
}

// ── workspace ─────────────────────────────────────────────────────────────────

func buildWorkspaceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workspace",
		Short: "Manage workspaces",
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List workspaces",
		RunE: func(cmd *cobra.Command, args []string) error {
			var result map[string]any
			if err := httpGet(globalAddr+"/v1/workspaces", &result); err != nil {
				return err
			}
			items, _ := result["items"].([]any)
			if len(items) == 0 {
				fmt.Println("no workspaces")
				return nil
			}
			for _, item := range items {
				ws, _ := item.(map[string]any)
				fmt.Printf("%s  %s\n", ws["id"], ws["name"])
			}
			return nil
		},
	}

	createCmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a workspace",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			body := map[string]any{"id": name, "name": name}
			var result map[string]any
			if err := postJSON(globalAddr+"/v1/workspaces", body, &result); err != nil {
				return err
			}
			fmt.Printf("created workspace %s\n", result["id"])
			return nil
		},
	}

	cmd.AddCommand(listCmd, createCmd)
	return cmd
}

// ── daemon ────────────────────────────────────────────────────────────────────

func buildDaemonCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "daemon",
		Short: "Check daemon status",
		RunE: func(cmd *cobra.Command, args []string) error {
			var result map[string]any
			if err := httpGet(globalAddr+"/v1/health", &result); err != nil {
				fmt.Println("daemon not reachable:", err)
				os.Exit(1)
			}
			fmt.Printf("status: %s  version: %s\n", result["status"], result["version"])
			return nil
		},
	}
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func postJSON(url string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return printAPIError(resp)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func httpGet(url string, out any) error {
	resp, err := httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return printAPIError(resp)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func printAPIError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	var errResp map[string]string
	if json.Unmarshal(body, &errResp) == nil {
		if msg, ok := errResp["error"]; ok {
			return fmt.Errorf("API error (%d): %s", resp.StatusCode, msg)
		}
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

// ── lint ──────────────────────────────────────────────────────────────────────

func buildLintCmd() *cobra.Command {
	var formatJSON bool

	cmd := &cobra.Command{
		Use:   "lint <path|dir>",
		Short: "Lint blueprint and pipeline files for best-practice issues",
		Long:  "Runs best-practice checks beyond schema/parse validation. Checks for unused inputs, missing timeouts, duplicate step names, template syntax errors, and more.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := args[0]
			var files []string

			info, err := os.Stat(target)
			if err != nil {
				return err
			}
			if info.IsDir() {
				err = filepath.WalkDir(target, func(path string, d fs.DirEntry, err error) error {
					if err != nil || d.IsDir() {
						return err
					}
					ext := strings.ToLower(filepath.Ext(path))
					if ext == ".yaml" || ext == ".yml" || ext == ".json" || ext == ".jsonc" {
						files = append(files, path)
					}
					return nil
				})
				if err != nil {
					return err
				}
			} else {
				files = []string{target}
			}

			var allIssues []lint.Issue
			for _, f := range files {
				rawContent, readErr := os.ReadFile(f)
				if readErr != nil {
					allIssues = append(allIssues, lint.Issue{
						File:     f,
						Severity: lint.SeverityError,
						Rule:     "read-error",
						Message:  readErr.Error(),
					})
					continue
				}

				// Try blueprint first, then pipeline.
				bp, bpErr := blueprint.ParseFile(f)
				if bpErr == nil {
					allIssues = append(allIssues, lint.LintBlueprint(bp, f, rawContent)...)
					continue
				}

				spec, pipeErr := pipeline.ParseFile(f)
				if pipeErr == nil {
					allIssues = append(allIssues, lint.LintPipeline(spec, f, rawContent)...)
					continue
				}

				// Neither parsed — report as error.
				allIssues = append(allIssues, lint.Issue{
					File:     f,
					Severity: lint.SeverityError,
					Rule:     "parse-error",
					Message:  bpErr.Error(),
				})
			}

			// Determine exit code: 2 = errors, 1 = warnings only, 0 = clean.
			exitCode := 0
			for _, issue := range allIssues {
				if issue.Severity == lint.SeverityError {
					exitCode = 2
					break
				}
				if issue.Severity == lint.SeverityWarning && exitCode < 1 {
					exitCode = 1
				}
			}

			if formatJSON {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				_ = enc.Encode(allIssues)
			} else {
				if len(allIssues) == 0 {
					fmt.Println("No issues found.")
				}
				for _, issue := range allIssues {
					if issue.Line > 0 {
						fmt.Fprintf(os.Stderr, "%s:%d: %s: [%s] %s\n", issue.File, issue.Line, issue.Severity, issue.Rule, issue.Message)
					} else {
						fmt.Fprintf(os.Stderr, "%s: %s: [%s] %s\n", issue.File, issue.Severity, issue.Rule, issue.Message)
					}
				}
			}

			if exitCode != 0 {
				os.Exit(exitCode)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&formatJSON, "format", false, "output machine-readable JSON")
	// Keep backward compat with old --json flag.
	cmd.Flags().BoolVar(&formatJSON, "json", false, "output machine-readable JSON (alias for --format)")
	return cmd
}

// ── fmt ───────────────────────────────────────────────────────────────────────

func buildFmtCmd() *cobra.Command {
	var (
		writeBack bool
		check     bool
	)

	cmd := &cobra.Command{
		Use:   "fmt <path>",
		Short: "Format a blueprint file to canonical YAML",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]

			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}

			// Apply compat alias normalisation
			normalised := applyCompatAliases(string(raw))

			// Parse and re-serialise to canonical YAML
			var tree any
			if err := yaml.Unmarshal([]byte(normalised), &tree); err != nil {
				return fmt.Errorf("parse %s: %w", path, err)
			}
			canonical, err := yaml.Marshal(tree)
			if err != nil {
				return fmt.Errorf("serialise: %w", err)
			}

			if check {
				if string(canonical) != string(raw) {
					fmt.Fprintf(os.Stderr, "%s would be reformatted\n", path)
					os.Exit(1)
				}
				return nil
			}

			if writeBack {
				return os.WriteFile(path, canonical, 0o644)
			}

			_, err = os.Stdout.Write(canonical)
			return err
		},
	}
	cmd.Flags().BoolVar(&writeBack, "write", false, "write canonical YAML back to file")
	cmd.Flags().BoolVar(&check, "check", false, "exit 1 if file would change (CI mode)")
	return cmd
}

// applyCompatAliases normalises v0.2/v0.3 field aliases to canonical v0.4 names.
func applyCompatAliases(src string) string {
	replacements := [][2]string{
		{"condition:", "if:"},
		{"continueOnError:", "continue_on_error:"},
		{"retryDelay:", "retry_delay_seconds:"},
	}
	for _, r := range replacements {
		src = strings.ReplaceAll(src, r[0], r[1])
	}
	return src
}

func parseInputs(inputs []string) (map[string]any, error) {
	result := map[string]any{}
	for _, s := range inputs {
		idx := strings.IndexByte(s, '=')
		if idx < 0 {
			return nil, fmt.Errorf("invalid input %q: expected key=value", s)
		}
		key := s[:idx]
		val := s[idx+1:]
		result[key] = val
	}
	return result, nil
}
