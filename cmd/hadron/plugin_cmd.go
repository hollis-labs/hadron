package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	fplugin "github.com/hollis-labs/fragments-engine/plugin"
	"github.com/hollis-labs/hadron/internal/plugin"
	"github.com/spf13/cobra"
)

const pluginGitOrg = "hollis-labs"

func buildPluginCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "Manage Hadron plugins",
	}

	cmd.AddCommand(
		buildPluginListCmd(),
		buildPluginInstallCmd(),
		buildPluginUninstallCmd(),
		buildPluginDisableCmd(),
		buildPluginEnableCmd(),
	)

	return cmd
}

func resolvePluginsDir() string {
	if d := os.Getenv("HADRON_PLUGINS_DIR"); d != "" {
		return d
	}
	return "./plugins"
}

func buildPluginListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List installed plugins",
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := resolvePluginsDir()
			entries, err := os.ReadDir(dir)
			if err != nil {
				if os.IsNotExist(err) {
					fmt.Println("No plugins installed.")
					return nil
				}
				return fmt.Errorf("read plugins dir: %w", err)
			}

			found := false
			fmt.Printf("%-25s %-10s %-10s %s\n", "PLUGIN", "VERSION", "STATUS", "DESCRIPTION")
			fmt.Println(strings.Repeat("-", 80))

			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}

				name := entry.Name()
				status := plugin.PluginStatusString(dir, name)

				manifestPath := filepath.Join(dir, name, "plugin.yaml")
				if status == "disabled" {
					manifestPath = filepath.Join(dir, name, "plugin.yaml.disabled")
				}
				manifest, err := plugin.ParseManifest(manifestPath)
				if err != nil {
					continue
				}

				found = true
				fmt.Printf("%-25s %-10s %-10s %s\n",
					name, manifest.Version, status, manifest.Description)
			}

			if !found {
				fmt.Println("No plugins installed.")
			}
			return nil
		},
	}
}

func buildPluginInstallCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "install <name>",
		Short: "Install a plugin from GitHub",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			dir := resolvePluginsDir()
			target := filepath.Join(dir, name)

			if _, err := os.Stat(filepath.Join(target, "plugin.yaml")); err == nil {
				return fmt.Errorf("plugin %q is already installed at %s", name, target)
			}

			os.MkdirAll(dir, 0755)

			repoURL := fmt.Sprintf("git@github.com:%s/%s.git", pluginGitOrg, name)
			fmt.Printf("Installing %s from %s...\n", name, repoURL)

			gitCmd := exec.Command("git", "clone", "--depth", "1", repoURL, target)
			gitCmd.Stdout = os.Stdout
			gitCmd.Stderr = os.Stderr
			if err := gitCmd.Run(); err != nil {
				return fmt.Errorf("failed to clone: %w", err)
			}

			if _, err := os.Stat(filepath.Join(target, "plugin.yaml")); err != nil {
				os.RemoveAll(target)
				return fmt.Errorf("cloned repo does not contain plugin.yaml — not a valid plugin")
			}

			manifest, err := plugin.ParseManifest(filepath.Join(target, "plugin.yaml"))
			if err != nil {
				os.RemoveAll(target)
				return fmt.Errorf("failed to parse plugin.yaml: %w", err)
			}

			if _, ok := plugin.LookupConstructor(manifest.Name); !ok {
				fmt.Printf("Warning: no compiled-in code for %q — plugin will need to be added to the binary\n", manifest.Name)
			} else {
				fmt.Printf("Found compiled-in code for %q\n", manifest.Name)
			}

			fmt.Printf("\nPlugin %q installed to %s\n", name, target)
			return nil
		},
	}
}

func buildPluginUninstallCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "uninstall <name>",
		Short: "Uninstall a plugin",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			dir := resolvePluginsDir()
			target := filepath.Join(dir, name)

			manifestPath := filepath.Join(target, "plugin.yaml")
			disabledPath := filepath.Join(target, "plugin.yaml.disabled")
			if _, err := os.Stat(manifestPath); err != nil {
				if _, err2 := os.Stat(disabledPath); err2 != nil {
					return fmt.Errorf("plugin %q is not installed", name)
				}
				manifestPath = disabledPath
			}

			manifest, err := plugin.ParseManifest(manifestPath)
			if err != nil {
				return fmt.Errorf("failed to parse plugin.yaml: %w", err)
			}

			if constructor, ok := plugin.LookupConstructor(manifest.Name); ok {
				p := constructor()
				if uninstallable, ok := p.(fplugin.Uninstallable); ok {
					fmt.Printf("Running %s cleanup...\n", manifest.Name)
					host := plugin.NewHostMinimal()
					if err := uninstallable.Uninstall(host); err != nil {
						fmt.Fprintf(os.Stderr, "Warning: cleanup error: %v\n", err)
					} else {
						fmt.Println("Cleanup complete.")
					}
				}
			}

			if err := os.RemoveAll(target); err != nil {
				return fmt.Errorf("failed to remove %s: %w", target, err)
			}

			fmt.Printf("\nPlugin %q uninstalled.\n", name)
			return nil
		},
	}
}

func buildPluginDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable <name>",
		Short: "Disable a plugin",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := resolvePluginsDir()
			if err := plugin.DisablePlugin(dir, args[0]); err != nil {
				return err
			}
			fmt.Printf("Plugin %q disabled.\n", args[0])
			return nil
		},
	}
}

func buildPluginEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable <name>",
		Short: "Enable a disabled plugin",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := resolvePluginsDir()
			if err := plugin.EnablePlugin(dir, args[0]); err != nil {
				return err
			}
			fmt.Printf("Plugin %q enabled.\n", args[0])
			return nil
		},
	}
}
