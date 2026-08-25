package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/registry"
)

// productionWorkflowCatalog serializes the file-backed catalog across daemon
// processes and reopens it for every operation. WorkflowIndex deliberately
// owns only an in-process snapshot/mutex; this composition wrapper makes the
// supported concurrent serve + stdio-MCP topology refresh-visible and prevents
// whole-file atomic replacements from losing another process's write.
type productionWorkflowCatalog struct {
	path     string
	lockPath string
}

func openProductionWorkflowCatalog(path string) (*productionWorkflowCatalog, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	catalog := &productionWorkflowCatalog{path: absolute, lockPath: absolute + ".lock"}
	_, err = withProductionWorkflowCatalog(catalog, true, func(index *registry.WorkflowIndex) (struct{}, error) {
		return struct{}{}, nil
	})
	return catalog, err
}

func withProductionWorkflowCatalog[T any](catalog *productionWorkflowCatalog, exclusive bool, operation func(*registry.WorkflowIndex) (T, error)) (T, error) {
	var zero T
	if catalog == nil || catalog.path == "" || catalog.lockPath == "" || operation == nil {
		return zero, errors.New("production workflow catalog is unavailable")
	}
	lock, err := openPrivateWorkflowLock(catalog.lockPath, "production workflow catalog")
	if err != nil {
		return zero, err
	}
	defer func() { _ = lock.Close() }()
	mode := syscall.LOCK_SH
	if exclusive {
		mode = syscall.LOCK_EX
	}
	if lockErr := syscall.Flock(int(lock.Fd()), mode); lockErr != nil { // #nosec G115 -- an open file descriptor is representable by the platform syscall API.
		return zero, lockErr
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }() // #nosec G115 -- same validated open file descriptor.
	index, err := registry.OpenWorkflowIndex(catalog.path)
	if err != nil {
		return zero, err
	}
	return operation(index)
}

func openPrivateWorkflowLock(path, purpose string) (*os.File, error) {
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600) // #nosec G304 -- host-owned path under the configured data directory.
	if err != nil {
		return nil, err
	}
	opened, statErr := lock.Stat()
	pathInfo, lstatErr := os.Lstat(path)
	if statErr != nil || lstatErr != nil || !opened.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(opened, pathInfo) || opened.Mode().Perm()&0o077 != 0 {
		_ = lock.Close()
		return nil, errors.New(purpose + " lock must be a private regular non-symlink file")
	}
	return lock, nil
}

func (c *productionWorkflowCatalog) RegisterWorkflow(ctx context.Context, record registry.WorkflowRecord, current bool) (registry.WorkflowRecord, error) {
	return withProductionWorkflowCatalog(c, true, func(index *registry.WorkflowIndex) (registry.WorkflowRecord, error) {
		return index.RegisterWorkflow(ctx, record, current)
	})
}

func (c *productionWorkflowCatalog) PinWorkflow(ctx context.Context, query registry.WorkflowQuery) (registry.WorkflowRecord, error) {
	return withProductionWorkflowCatalog(c, true, func(index *registry.WorkflowIndex) (registry.WorkflowRecord, error) {
		return index.PinWorkflow(ctx, query)
	})
}

func (c *productionWorkflowCatalog) UnpinWorkflowExact(ctx context.Context, query registry.WorkflowQuery) error {
	_, err := withProductionWorkflowCatalog(c, true, func(index *registry.WorkflowIndex) (struct{}, error) {
		return struct{}{}, index.UnpinWorkflowExact(ctx, query)
	})
	return err
}

func (c *productionWorkflowCatalog) ResolvePinnedWorkflow(ctx context.Context, name string) (registry.WorkflowResolution, error) {
	return withProductionWorkflowCatalog(c, false, func(index *registry.WorkflowIndex) (registry.WorkflowResolution, error) {
		return index.ResolvePinnedWorkflow(ctx, name)
	})
}

func (c *productionWorkflowCatalog) PublishWorkflow(ctx context.Context, query registry.WorkflowQuery) (registry.WorkflowRecord, error) {
	return withProductionWorkflowCatalog(c, true, func(index *registry.WorkflowIndex) (registry.WorkflowRecord, error) {
		return index.PublishWorkflow(ctx, query)
	})
}

func (c *productionWorkflowCatalog) InspectWorkflow(ctx context.Context, query registry.WorkflowQuery) (registry.WorkflowRecord, error) {
	return withProductionWorkflowCatalog(c, false, func(index *registry.WorkflowIndex) (registry.WorkflowRecord, error) {
		return index.InspectWorkflow(ctx, query)
	})
}

func (c *productionWorkflowCatalog) SearchWorkflows(ctx context.Context, namespace, query string) ([]registry.WorkflowRecord, error) {
	return withProductionWorkflowCatalog(c, false, func(index *registry.WorkflowIndex) ([]registry.WorkflowRecord, error) {
		return index.SearchWorkflows(ctx, namespace, query)
	})
}

func (c *productionWorkflowCatalog) ResolveWorkflow(ctx context.Context, query registry.WorkflowQuery) (registry.WorkflowResolution, error) {
	return withProductionWorkflowCatalog(c, false, func(index *registry.WorkflowIndex) (registry.WorkflowResolution, error) {
		return index.ResolveWorkflow(ctx, query)
	})
}

// WithSourceActivationCurrent keeps the shared catalog lock held from current
// alias validation through durable activation admission. Current-alias writers
// take the same sidecar lock exclusively, so a fire either linearizes wholly
// before a move or observes the completed move and fails closed.
func (c *productionWorkflowCatalog) WithSourceActivationCurrent(ctx context.Context, operation func(appworkflow.SourceActivationRegistry) error) error {
	if ctx == nil {
		return errors.New("source activation current fence requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := withProductionWorkflowCatalog(c, false, func(index *registry.WorkflowIndex) (struct{}, error) {
		if contextErr := ctx.Err(); contextErr != nil {
			return struct{}{}, contextErr
		}
		return struct{}{}, operation(index)
	})
	return err
}

func (c *productionWorkflowCatalog) RemoveCurrentWorkflowExact(ctx context.Context, query registry.WorkflowQuery) error {
	_, err := withProductionWorkflowCatalog(c, true, func(index *registry.WorkflowIndex) (struct{}, error) {
		return struct{}{}, index.RemoveCurrentWorkflowExact(ctx, query)
	})
	return err
}
