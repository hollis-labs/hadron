package main

import (
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/internal/offlinebuild"
	"github.com/hollis-labs/go-workflow/diagnostic"
	"github.com/hollis-labs/go-workflow/offline"
	"github.com/spf13/cobra"
)

func buildOfflineCmd() *cobra.Command {
	var output, mode, toolName string
	var bindingFiles []string
	command := &cobra.Command{
		Use:   "build <workflow-path>",
		Short: "Build a supported graph-native workflow as a daemon-less executable",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			builder, err := offlinebuild.NewDefault()
			if err != nil {
				return err
			}
			bindings, err := offlinebuild.ReadBindings(bindingFiles)
			if err != nil {
				return err
			}
			result, err := builder.BuildExecutable(command.Context(), offlinebuild.Request{
				SourcePath: arguments[0], Mode: offline.Mode(mode), ToolName: toolName,
				Bindings: bindings, OutputPath: output,
			})
			if err != nil {
				return err
			}
			if len(result.Diagnostics) != 0 {
				return offlineBuildDiagnosticError(result.Diagnostics)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "built %s (%s)\n", result.OutputPath, result.Manifest.BuildDigest)
			return err
		},
	}
	command.Flags().StringVarP(&output, "output", "o", "", "output executable path (required)")
	command.Flags().StringVar(&mode, "as", string(offline.ModeCLI), "artifact surface: cli or mcp-server")
	command.Flags().StringVar(&toolName, "tool-name", "", "generated MCP tool name")
	command.Flags().StringArrayVar(&bindingFiles, "binding", nil, "external binding JSON file (repeatable)")
	_ = command.MarkFlagRequired("output")
	return command
}

func offlineBuildDiagnosticError(findings []diagnostic.Diagnostic) error {
	messages := make([]string, len(findings))
	for index, finding := range findings {
		location := ""
		if finding.Source != nil {
			location = fmt.Sprintf("%s:%d:%d: ", finding.Source.Locator, finding.Source.StartLine, finding.Source.StartColumn)
		}
		messages[index] = fmt.Sprintf("%s%s: %s", location, finding.Code, finding.Message)
	}
	return fmt.Errorf("offline build rejected:\n%s", strings.Join(messages, "\n"))
}
