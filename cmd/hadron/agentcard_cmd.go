package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/spf13/cobra"
)

func buildAgentCardCmd() *cobra.Command {
	var (
		allFlag bool
		urlFlag string
	)

	cmd := &cobra.Command{
		Use:   "agent-card [blueprint.yaml]",
		Short: "Generate an A2A Agent Card from blueprint metadata",
		Long:  "Generates an A2A-compatible Agent Card JSON document. Pass a single blueprint file, or use --all to scan a directory.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if allFlag {
				// --all mode: scan directory
				dir := "."
				if len(args) > 0 {
					dir = args[0]
				}
				card, err := agentcard.FromDirectory(dir, urlFlag)
				if err != nil {
					return err
				}
				return printCard(card)
			}

			// Single blueprint mode
			if len(args) == 0 {
				return fmt.Errorf("blueprint path required (or use --all <dir>)")
			}

			bp, err := blueprint.ParseFile(args[0])
			if err != nil {
				return fmt.Errorf("parse blueprint: %w", err)
			}

			skill := agentcard.SkillFromBlueprint(bp, args[0])
			card := &agentcard.AgentCard{
				Name:        skill.Name,
				Description: skill.Description,
				URL:         urlFlag,
				Provider:    agentcard.Provider{Organization: "Hadron"},
				Version:     "0.4.0",
				Capabilities: agentcard.Capabilities{
					Streaming:         false,
					PushNotifications: false,
				},
				DefaultInputModes:  []string{"application/json"},
				DefaultOutputModes: []string{"application/json"},
				Skills:             []agentcard.Skill{skill},
			}
			return printCard(card)
		},
	}

	cmd.Flags().BoolVar(&allFlag, "all", false, "scan directory for all blueprints")
	cmd.Flags().StringVar(&urlFlag, "url", "http://localhost:8095", "base URL for the agent")
	return cmd
}

func printCard(card *agentcard.AgentCard) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(card)
}
