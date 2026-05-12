package main

import (
	"fmt"

	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/spf13/cobra"
)

func buildRegistryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "registry",
		Short: "Manage the local blueprint registry",
	}

	cmd.AddCommand(
		buildRegistryIndexCmd(),
		buildRegistrySearchCmd(),
		buildRegistryShowCmd(),
		buildRegistryListCmd(),
		buildRegistryVersionsCmd(),
	)
	return cmd
}

func openRegistry() (*registry.Registry, func(), error) {
	cfg := config.Default()
	store, err := persistence.Open(cfg.DBPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open store: %w", err)
	}
	reg := registry.New(store)
	cleanup := func() { _ = store.Close() }
	return reg, cleanup, nil
}

func buildRegistryIndexCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "index [dir]",
		Short: "Scan and index blueprint files",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) > 0 {
				dir = args[0]
			} else {
				// Try to use the configured blueprint_dir from settings.
				cfg := config.Default()
				sett, err := settings.Load(cfg.DataDir)
				if err == nil && sett.BlueprintDir != "" {
					dir = sett.BlueprintDir
				}
			}

			reg, cleanup, err := openRegistry()
			if err != nil {
				return err
			}
			defer cleanup()

			result, err := reg.Index(dir)
			if err != nil {
				return err
			}
			fmt.Printf("indexed: %d new, %d updated, %d unchanged\n",
				result.Indexed, result.Updated, result.Unchanged)
			return nil
		},
	}
}

func buildRegistrySearchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "search <query>",
		Short: "Search blueprints by keyword",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			reg, cleanup, err := openRegistry()
			if err != nil {
				return err
			}
			defer cleanup()

			entries, err := reg.Search(args[0])
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				fmt.Println("no results")
				return nil
			}
			for _, e := range entries {
				fmt.Printf("%-30s %-50s %s\n", e.Name, e.FilePath, e.Tags)
			}
			return nil
		},
	}
}

func buildRegistryShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show <name>",
		Short: "Show blueprint details from the registry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			reg, cleanup, err := openRegistry()
			if err != nil {
				return err
			}
			defer cleanup()

			entry, err := reg.Show(args[0])
			if err != nil {
				return err
			}
			fmt.Printf("name:        %s\n", entry.Name)
			fmt.Printf("slug:        %s\n", entry.Slug)
			fmt.Printf("title:       %s\n", entry.Title)
			fmt.Printf("description: %s\n", entry.Description)
			fmt.Printf("author:      %s\n", entry.Author)
			fmt.Printf("tags:        %s\n", entry.Tags)
			fmt.Printf("path:        %s\n", entry.FilePath)
			fmt.Printf("hash:        %s\n", entry.VersionHash)
			fmt.Printf("indexed_at:  %s\n", entry.IndexedAt)
			if entry.InputsJSON != "" && entry.InputsJSON != "[]" {
				fmt.Printf("inputs:      %s\n", entry.InputsJSON)
			}
			return nil
		},
	}
}

func buildRegistryVersionsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "versions <name>",
		Short: "Show version history for a blueprint",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			reg, cleanup, err := openRegistry()
			if err != nil {
				return err
			}
			defer cleanup()

			versions, err := reg.Versions(args[0])
			if err != nil {
				return err
			}
			if len(versions) == 0 {
				fmt.Println("no versions found")
				return nil
			}
			for _, v := range versions {
				fmt.Printf("%s  %s  %s\n", v.VersionHash[:16], v.IndexedAt, v.FilePath)
			}
			return nil
		},
	}
}

func buildRegistryListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all indexed blueprints",
		RunE: func(cmd *cobra.Command, args []string) error {
			reg, cleanup, err := openRegistry()
			if err != nil {
				return err
			}
			defer cleanup()

			entries, err := reg.List()
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				fmt.Println("no blueprints indexed")
				return nil
			}
			for _, e := range entries {
				fmt.Printf("%-30s %-50s %s  %s\n", e.Name, e.FilePath, e.VersionHash[:12], e.IndexedAt)
			}
			return nil
		},
	}
}
