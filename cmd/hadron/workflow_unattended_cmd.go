package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/unattended"
	"github.com/spf13/cobra"
)

// buildWorkflowUnattendedCmd edits the unattended allow-list file directly.
// It never talks to the daemon: the list has no HTTP, MCP or A2A write path,
// so its boundary is who can write the file (see docs/workflows.md).
func buildWorkflowUnattendedCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var dataDir string
	command := &cobra.Command{
		Use:   "unattended",
		Short: "Manage the allow-list of workflows that may start without confirmation",
		Long: `Manage the operator allow-list of workflows that may start unattended.

Hadron asks for confirmation before starting a workflow whose effects advise
it, and refuses the start when nobody is there to confirm (schedules,
reactors, failure handlers). An entry pins one plan id and one exact plan
digest; a matching start is allowed and its policy decision names the entry.

This command edits ` + unattended.FileName + ` in the data dir directly and
never calls the daemon. The daemon re-reads the file when it changes and
ignores it (allowing nothing) if it is group- or world-writable or owned by
another user. Any process running as your user, including agents Hadron,
Tether or Torque launch, can also write it.`,
	}
	defaultDir := config.Default().DataDir
	command.PersistentFlags().StringVar(&dataDir, "data-dir", defaultDir, "Hadron data directory holding "+unattended.FileName)
	path := func() string { return filepath.Join(dataDir, unattended.FileName) }
	command.AddCommand(
		buildUnattendedAllowCmd(dependencies, path),
		buildUnattendedRevokeCmd(path),
		buildUnattendedListCmd(dependencies, path),
	)
	return command
}

func buildUnattendedAllowCmd(dependencies workflowCommandDependencies, path func() string) *cobra.Command {
	var planID, digest, reason, activationID, principal, expires, id string
	command := &cobra.Command{
		Use:   "allow",
		Short: "Allow one exact plan (id + digest) to start without confirmation",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			now := dependencies.now().UTC()
			file, _, err := unattended.ReadChecked(path(), os.Getuid())
			if err != nil {
				return err
			}
			if id == "" {
				buffer := make([]byte, 6)
				if err = dependencies.random(buffer); err != nil {
					return fmt.Errorf("generate entry id: %w", err)
				}
				id = "ua-" + hex.EncodeToString(buffer)
			}
			for _, existing := range file.Entries {
				if existing.ID == id {
					return fmt.Errorf("entry %s already exists", id)
				}
			}
			entry := unattended.Entry{
				ID: id, PlanID: planID, Digest: digest,
				Scope:  unattended.Scope{ActivationID: activationID, Principal: principal},
				Reason: reason, AddedBy: localOperatorLabel(), AddedAt: now,
			}
			if expires != "" {
				at, expiryErr := parseUnattendedExpiry(expires, now)
				if expiryErr != nil {
					return expiryErr
				}
				entry.ExpiresAt = &at
			}
			if err = entry.Validate(); err != nil {
				return err
			}
			file.Entries = append(file.Entries, entry)
			if err = unattended.WriteAtomic(path(), file); err != nil {
				return err
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "added %s: %s @ %s\n", entry.ID, entry.PlanID, entry.Digest)
			return err
		},
	}
	command.Flags().StringVar(&planID, "plan", "", "plan id (as printed by `hadron workflow validate`)")
	command.Flags().StringVar(&digest, "digest", "", "exact plan digest, sha256:<hex> (as printed by `hadron workflow validate`)")
	command.Flags().StringVar(&reason, "reason", "", "why this workflow may run unattended (required)")
	command.Flags().StringVar(&activationID, "activation", "", "limit to starts from this activation registration")
	command.Flags().StringVar(&principal, "principal", "", "limit to starts bound to this principal")
	command.Flags().StringVar(&expires, "expires", "", "end the entry after a duration (e.g. 72h) or at an RFC 3339 time")
	command.Flags().StringVar(&id, "id", "", "entry id (default: generated)")
	_ = command.MarkFlagRequired("plan")
	_ = command.MarkFlagRequired("digest")
	_ = command.MarkFlagRequired("reason")
	return command
}

func buildUnattendedRevokeCmd(path func() string) *cobra.Command {
	return &cobra.Command{
		Use:   "revoke <entry-id>",
		Short: "Remove an allow-list entry",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			file, _, err := unattended.ReadChecked(path(), os.Getuid())
			if err != nil {
				return err
			}
			kept := file.Entries[:0]
			found := false
			for _, entry := range file.Entries {
				if entry.ID == arguments[0] {
					found = true
					continue
				}
				kept = append(kept, entry)
			}
			if !found {
				return fmt.Errorf("no entry %s", arguments[0])
			}
			file.Entries = kept
			if err = unattended.WriteAtomic(path(), file); err != nil {
				return err
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "revoked %s\n", arguments[0])
			return err
		},
	}
}

func buildUnattendedListCmd(dependencies workflowCommandDependencies, path func() string) *cobra.Command {
	var asJSON bool
	command := &cobra.Command{
		Use:   "list",
		Short: "List allow-list entries, marking expired ones",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			file, _, err := unattended.ReadChecked(path(), os.Getuid())
			if err != nil {
				return err
			}
			return writeUnattendedList(command.OutOrStdout(), file.Entries, dependencies.now().UTC(), asJSON)
		},
	}
	command.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return command
}

type unattendedListItem struct {
	unattended.Entry
	Status string `json:"status"`
}

func writeUnattendedList(out io.Writer, entries []unattended.Entry, now time.Time, asJSON bool) error {
	items := make([]unattendedListItem, 0, len(entries))
	for _, entry := range entries {
		status := "live"
		if entry.Expired(now) {
			status = "expired"
		}
		items = append(items, unattendedListItem{Entry: entry, Status: status})
	}
	if asJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(items)
	}
	if len(items) == 0 {
		_, err := fmt.Fprintln(out, "no unattended allow-list entries")
		return err
	}
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "ID\tSTATUS\tPLAN\tDIGEST\tSCOPE\tEXPIRES\tADDED BY\tADDED AT\tREASON"); err != nil {
		return err
	}
	for _, item := range items {
		expires := "-"
		if item.ExpiresAt != nil {
			expires = item.ExpiresAt.UTC().Format(time.RFC3339)
		}
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", item.ID, item.Status, item.PlanID,
			shortDigest(item.Digest), scopeLabel(item.Scope), expires, item.AddedBy,
			item.AddedAt.UTC().Format(time.RFC3339), item.Reason); err != nil {
			return err
		}
	}
	return table.Flush()
}

func parseUnattendedExpiry(raw string, now time.Time) (time.Time, error) {
	if duration, err := time.ParseDuration(raw); err == nil {
		if duration <= 0 {
			return time.Time{}, errors.New("--expires duration must be positive")
		}
		return now.Add(duration), nil
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("--expires must be a duration (72h) or an RFC 3339 time: %q", raw)
	}
	return at.UTC(), nil
}

// localOperatorLabel is who added an entry, as the OS reports it. It is a
// label, not an authenticated identity: anyone who can write the file can
// write any added_by.
func localOperatorLabel() string {
	name := "unknown"
	if current, err := user.Current(); err == nil && current.Username != "" {
		name = current.Username
	}
	return "local:" + name
}

func shortDigest(digest string) string {
	hexPart := strings.TrimPrefix(digest, "sha256:")
	if len(hexPart) > 12 {
		return "sha256:" + hexPart[:12]
	}
	return digest
}

func scopeLabel(scope unattended.Scope) string {
	var parts []string
	if scope.ActivationID != "" {
		parts = append(parts, "activation="+scope.ActivationID)
	}
	if scope.Principal != "" {
		parts = append(parts, "principal="+scope.Principal)
	}
	if len(parts) == 0 {
		return "any"
	}
	return strings.Join(parts, ",")
}
