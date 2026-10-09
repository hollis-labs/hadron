// Package launchartifacts binds an accepted Hadron launch to its freshly
// allocated private boot directory. It grants no project or credential effects.
package launchartifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/hollis-labs/substrate/harness/agentlaunch"
	"github.com/hollis-labs/substrate/harness/agentlaunch/launcher"
	"github.com/hollis-labs/substrate/harness/workspace"
	"github.com/hollis-labs/substrate/harness/workspace/local"
)

// Admission comes from the caller's existing accepted launch decision. Validate
// must recheck that decision while its operation custody remains held. Storage
// is explicit, local, and outside the session workspace; it is never discovered
// from HOME or a provider configuration directory.
type Admission struct {
	OperationID, DecisionID, Version, Owner string
	ControlParent                           string
	LocalFilesystem                         bool
	Validate                                func(context.Context) error
}

// Custody retains physical directory identities until this artifact operation
// finishes. Closing ports or custody preserves committed and partial roots.
type Custody struct {
	mu          sync.Mutex
	closed      bool
	claimed     bool
	admission   Admission
	compiled    *agentlaunch.CompiledLaunch
	digest      string
	root        workspace.RootRef
	control     workspace.RootRef
	rootFile    *os.File
	controlFile *os.File
	resources   workspace.Resources
}

// Digest gives a secret-free revision of an accepted value; values themselves
// are never included in errors or durable authority metadata.
func Digest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

// Prepare owns the actual allocator call, rather than accepting an arbitrary
// caller-supplied directory as proof of custody. Missing admission refuses
// before allocating either the boot directory or control storage.
func Prepare(ctx context.Context, compiled *agentlaunch.CompiledLaunch, admission Admission) (*agentlaunch.PreparedLaunch, *Custody, error) {
	if ctx == nil || compiled == nil || admission.Validate == nil || admission.OperationID == "" || admission.DecisionID == "" || admission.Version == "" || admission.Owner == "" || !admission.LocalFilesystem {
		return nil, nil, refusal("missing_launch_artifact_admission")
	}
	if err := admission.Validate(ctx); err != nil {
		return nil, nil, err
	}
	if err := compiled.Validate(); err != nil {
		return nil, nil, err
	}
	parent := admission.ControlParent
	canonical, err := filepath.EvalSymlinks(parent)
	if err != nil || !filepath.IsAbs(parent) || parent != filepath.Clean(parent) || canonical != parent {
		return nil, nil, refusal("noncanonical_artifact_control_parent")
	}
	digest, err := Digest(compiled)
	if err != nil {
		return nil, nil, err
	}
	control, err := os.MkdirTemp(parent, "hadron-artifact-control-*")
	if err != nil {
		return nil, nil, fmt.Errorf("allocate artifact control storage: %w", err)
	}
	if overlap(control, compiled.Plan.Workspace.WorkspaceDir) {
		return nil, nil, refusal("artifact_control_overlaps_workspace")
	}
	prepared, err := launcher.Prepare(ctx, compiled)
	if err != nil {
		return nil, nil, err
	}
	root := prepared.PlantedBootDir
	if overlap(control, root) {
		return nil, nil, refusal("artifact_control_overlaps_boot_root")
	}
	c := &Custody{admission: admission, compiled: compiled, digest: digest,
		root:    workspace.RootRef{ID: "boot:" + admission.OperationID, Path: root, AllowedBase: filepath.Dir(root), Owner: admission.Owner, Provenance: "hadron.accepted-launch:" + admission.DecisionID},
		control: workspace.RootRef{ID: "control:" + admission.OperationID, Path: control, AllowedBase: parent, Owner: admission.Owner, Provenance: "hadron.accepted-launch:" + admission.DecisionID},
	}
	c.rootFile, err = os.Open(root) // #nosec G304 -- fresh allocator-owned directory; held identity is checked before authority and under apply locks.
	if err != nil {
		return nil, nil, err
	}
	c.controlFile, err = os.Open(control) // #nosec G304 -- fresh private control directory; identity and canonical custody are validated before use.
	if err != nil {
		_ = c.Close()
		return nil, nil, err
	}
	binding, err := Digest(struct{ Decision, Operation, Plan, Root string }{admission.DecisionID, admission.OperationID, digest, root})
	if err != nil {
		_ = c.Close()
		return nil, nil, err
	}
	grant := workspace.EffectGrant{Kind: workspace.ArtifactEffect, RootID: c.root.ID,
		AuthorizationID: admission.DecisionID + ":" + binding, Version: admission.Version}
	c.resources = workspace.Resources{Roots: []workspace.RootRef{c.root}, LockRoot: c.control, LockNamespace: control,
		Capabilities: []workspace.Capability{workspace.CanonicalRoots, workspace.MutationLocks}, Grants: []workspace.EffectGrant{grant}}
	if custodyErr := c.validate(ctx); custodyErr != nil {
		_ = c.Close()
		return nil, nil, custodyErr
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		_ = c.Close()
		return nil, nil, errors.Join(refusal("fresh_artifact_root_not_empty"), err)
	}
	return prepared, c, nil
}

// Authorize supplies exactly one operation's held custody and live host ports.
// Resource substitution is checked by local ports as well as the engine.
func (c *Custody) Authorize(ctx context.Context, target string) (agentlaunch.ArtifactAuthority, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.claimed || target != c.root.Path {
		return agentlaunch.ArtifactAuthority{}, refusal("artifact_custody_unavailable")
	}
	if err := c.validate(ctx); err != nil {
		return agentlaunch.ArtifactAuthority{}, err
	}
	observe := func(ctx context.Context) (workspace.Observations, error) {
		if err := c.validate(ctx); err != nil {
			return workspace.Observations{}, err
		}
		now := time.Now().UTC()
		out := workspace.Observations{At: now, ExpiresAt: now.Add(time.Minute), Capabilities: slices.Clone(c.resources.Capabilities)}
		for _, ref := range []workspace.RootRef{c.root, c.control} {
			observed, err := workspace.InspectRoot(ref)
			if err != nil {
				return workspace.Observations{}, err
			}
			out.Roots = append(out.Roots, observed)
		}
		return out, nil
	}
	observed, err := observe(ctx)
	if err != nil {
		return agentlaunch.ArtifactAuthority{}, err
	}
	operationID := c.admission.OperationID
	ports, closePorts, err := local.New(local.Options{OperationID: operationID, ControlRoot: c.control, Resources: c.resources,
		LocalFilesystem: c.admission.LocalFilesystem, Evidence: observe,
		ValidateAuthority: func(ctx context.Context, spec workspace.Spec, resources workspace.Resources) error {
			if spec.Operation != workspace.Prepare || spec.OperationID != operationID {
				return refusal("artifact_operation_changed")
			}
			// The authority preflight has no effects; apply has precisely the
			// managed-tree effect granted by this accepted launch operation.
			if len(spec.Effects) > 1 || len(spec.Effects) == 1 && spec.Effects[0] != c.resources.Grants[0] {
				return refusal("artifact_effect_changed")
			}
			return c.validate(ctx)
		}})
	if err != nil {
		return agentlaunch.ArtifactAuthority{}, err
	}
	c.claimed = true
	resources := c.resources
	resources.Roots = slices.Clone(resources.Roots)
	resources.Grants = slices.Clone(resources.Grants)
	resources.Capabilities = slices.Clone(resources.Capabilities)
	input := workspace.TreeRequest{OperationID: operationID, Root: c.root, RootMode: 0700, Resources: resources, Observed: observed}
	return agentlaunch.ArtifactAuthority{Inactive: true, PrivateCustody: true, Input: input, Ports: ports, Close: closePorts}, nil
}

func (c *Custody) validate(ctx context.Context) error {
	if ctx == nil {
		return refusal("missing_launch_artifact_context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.admission.Validate(ctx); err != nil {
		return err
	}
	digest, err := Digest(c.compiled)
	if err != nil || digest != c.digest {
		return errors.Join(refusal("accepted_artifact_plan_changed"), err)
	}
	for _, held := range []struct {
		ref  workspace.RootRef
		file *os.File
	}{{c.root, c.rootFile}, {c.control, c.controlFile}} {
		before, err := held.file.Stat()
		if err != nil {
			return err
		}
		current, err := os.Lstat(held.ref.Path)
		if err != nil || !current.IsDir() || current.Mode() != os.ModeDir|0700 || !os.SameFile(before, current) {
			return errors.Join(refusal("artifact_private_custody_changed"), err)
		}
		if _, err := workspace.InspectRoot(held.ref); err != nil {
			return err
		}
	}
	return nil
}

func (c *Custody) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var err error
	for _, file := range []*os.File{c.rootFile, c.controlFile} {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
	}
	return err
}

func overlap(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	rel, err := filepath.Rel(a, b)
	if err != nil || rel == "." || filepath.IsLocal(rel) {
		return true
	}
	rel, err = filepath.Rel(b, a)
	return err != nil || rel == "." || filepath.IsLocal(rel)
}

func refusal(code string) error {
	return &workspace.Refusal{Code: code, Concern: "hadron.launch-artifacts", Status: workspace.Conflict}
}
