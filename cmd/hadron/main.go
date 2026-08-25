package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/config"
	"github.com/spf13/cobra"
)

var (
	globalAddr string
	httpClient = &http.Client{Timeout: 30 * time.Second}
	version    = "dev"
	commit     = "unknown"
	buildDate  = "unknown"
)

func closeBody(body io.Closer) { _ = body.Close() }

func main() {
	if err := buildRootCommand().Execute(); err != nil {
		os.Exit(1)
	}
}

func buildRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:               "hadron",
		Short:             "Hadron graph-native workflow CLI",
		Long:              "hadron is the CLI client for the hadrond daemon.",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}
	root.PersistentFlags().StringVar(&globalAddr, "addr", "http://"+config.DefaultAddr, "daemon base URL")
	root.AddCommand(buildOfflineCmd(), buildWorkflowCmd(), buildWorkspaceCmd(), buildDaemonCmd(), buildVersionCmd())
	return root
}

func buildVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use: "version", Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("hadron %s\n", version)
			fmt.Printf("commit: %s\n", commit)
			fmt.Printf("built: %s\n", buildDate)
		},
	}
}

func buildWorkspaceCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "workspace", Short: "Manage workspaces"}
	listCmd := &cobra.Command{
		Use: "list", Short: "List workspaces",
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
				workspace, _ := item.(map[string]any)
				fmt.Printf("%s  %s\n", workspace["id"], workspace["name"])
			}
			return nil
		},
	}
	createCmd := &cobra.Command{
		Use: "create <name>", Short: "Create a workspace", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			var result map[string]any
			if err := postJSON(globalAddr+"/v1/workspaces", map[string]any{"id": name, "name": name}, &result); err != nil {
				return err
			}
			fmt.Printf("created workspace %s\n", result["id"])
			return nil
		},
	}
	cmd.AddCommand(listCmd, createCmd)
	return cmd
}

func buildDaemonCmd() *cobra.Command {
	return &cobra.Command{
		Use: "daemon", Short: "Check daemon status",
		RunE: func(cmd *cobra.Command, args []string) error {
			var result map[string]any
			if err := httpGet(globalAddr+"/v1/health", &result); err != nil {
				return fmt.Errorf("daemon not reachable: %w", err)
			}
			fmt.Printf("status: %s  version: %s\n", result["status"], result["version"])
			return nil
		},
	}
}

func postJSON(url string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer closeBody(resp.Body)
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
	defer closeBody(resp.Body)
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
	var errorResponse map[string]string
	if json.Unmarshal(body, &errorResponse) == nil {
		if message, ok := errorResponse["error"]; ok {
			return fmt.Errorf("API error (%d): %s", resp.StatusCode, message)
		}
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}
