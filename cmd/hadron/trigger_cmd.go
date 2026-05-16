package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

func buildTriggerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trigger",
		Short: "Manage webhook triggers",
	}

	// ── list ──────────────────────────────────────────────────────────────────
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all webhook triggers",
		RunE: func(cmd *cobra.Command, args []string) error {
			var result map[string]any
			if err := httpGet(globalAddr+"/v1/triggers", &result); err != nil {
				return err
			}
			items, _ := result["items"].([]any)
			if len(items) == 0 {
				fmt.Println("no triggers")
				return nil
			}
			for _, item := range items {
				t, _ := item.(map[string]any)
				fmt.Printf("%s  %s  path=%s  blueprint=%s  enabled=%v  fired=%v\n",
					t["id"], t["name"], t["path"], t["blueprint_path"], t["enabled"], t["fired_count"])
			}
			return nil
		},
	}

	// ── create ────────────────────────────────────────────────────────────────
	var (
		name        string
		path        string
		bpPath      string
		secret      string
		oneShot     bool
		createWS    string
		trigType    string
		watchPaths  string
		watchEvents string
		debounce    int
	)
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a trigger (webhook or file_watch)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if bpPath == "" {
				return fmt.Errorf("--blueprint is required")
			}

			body := map[string]any{
				"name":           name,
				"blueprint_path": bpPath,
				"workspace_id":   createWS,
				"one_shot":       oneShot,
				"type":           trigType,
			}

			switch trigType {
			case "webhook":
				if path == "" {
					return fmt.Errorf("--path is required for webhook triggers")
				}
				body["path"] = path
				if secret != "" {
					body["secret"] = secret
				}
			case "file_watch":
				if watchPaths == "" {
					return fmt.Errorf("--paths is required for file_watch triggers")
				}
				body["path"] = watchPaths
				body["debounce_seconds"] = debounce
				if watchEvents != "" {
					body["extract_inputs"] = map[string]string{"events": watchEvents}
				}
			default:
				return fmt.Errorf("--type must be 'webhook' or 'file_watch'")
			}

			var result map[string]any
			if err := postJSON(globalAddr+"/v1/triggers", body, &result); err != nil {
				return err
			}
			if trigType == "webhook" {
				fmt.Printf("created trigger %s (hook URL: /hooks/%s)\n", result["id"], result["path"])
			} else {
				fmt.Printf("created file_watch trigger %s watching %s\n", result["id"], watchPaths)
			}
			return nil
		},
	}
	createCmd.Flags().StringVar(&trigType, "type", "webhook", "trigger type: 'webhook' or 'file_watch'")
	createCmd.Flags().StringVar(&name, "name", "", "trigger name (required)")
	createCmd.Flags().StringVar(&path, "path", "", "webhook path (for webhook type, e.g. 'my-deploy')")
	createCmd.Flags().StringVar(&bpPath, "blueprint", "", "blueprint path (required)")
	createCmd.Flags().StringVar(&secret, "secret", "", "HMAC-SHA256 secret for signature validation (webhook only)")
	createCmd.Flags().BoolVar(&oneShot, "one-shot", false, "delete trigger after first firing")
	createCmd.Flags().StringVar(&createWS, "workspace", "default", "workspace ID")
	createCmd.Flags().StringVar(&watchPaths, "paths", "", "comma-separated paths to watch (file_watch only)")
	createCmd.Flags().StringVar(&watchEvents, "events", "", "comma-separated events to watch: create,modify,delete,rename (file_watch only)")
	createCmd.Flags().IntVar(&debounce, "debounce", 5, "debounce seconds for file_watch triggers")

	// ── delete ────────────────────────────────────────────────────────────────
	deleteCmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a webhook trigger",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req, _ := http.NewRequest(http.MethodDelete, globalAddr+"/v1/triggers/"+args[0], nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				return err
			}
			defer closeBody(resp.Body)
			if resp.StatusCode == http.StatusNoContent {
				fmt.Println("deleted")
				return nil
			}
			return printAPIError(resp)
		},
	}

	// ── get ───────────────────────────────────────────────────────────────────
	getCmd := &cobra.Command{
		Use:   "get <id>",
		Short: "Get trigger details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var result map[string]any
			if err := httpGet(globalAddr+"/v1/triggers/"+args[0], &result); err != nil {
				return err
			}
			data, _ := json.MarshalIndent(result, "", "  ")
			fmt.Println(string(data))
			return nil
		},
	}

	// ── fire (test) ───────────────────────────────────────────────────────────
	var firePayload string
	fireCmd := &cobra.Command{
		Use:   "fire <hook-path>",
		Short: "Send a test webhook to a trigger",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			hookPath := args[0]
			var bodyReader *bytes.Reader
			if firePayload != "" {
				bodyReader = bytes.NewReader([]byte(firePayload))
			} else {
				bodyReader = bytes.NewReader([]byte("{}"))
			}
			resp, err := httpClient.Post(globalAddr+"/hooks/"+hookPath, "application/json", bodyReader)
			if err != nil {
				return fmt.Errorf("request failed: %w", err)
			}
			defer closeBody(resp.Body)
			if resp.StatusCode >= 400 {
				return printAPIError(resp)
			}
			var result map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return err
			}
			fmt.Printf("webhook accepted: run_id=%s\n", result["run_id"])
			return nil
		},
	}
	fireCmd.Flags().StringVar(&firePayload, "payload", "", "JSON payload to send")

	cmd.AddCommand(listCmd, createCmd, deleteCmd, getCmd, fireCmd)
	return cmd
}
