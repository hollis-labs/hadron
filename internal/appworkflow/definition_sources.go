package appworkflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/internal/pack"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/go-workflow/authoring"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
)

type definitionSourceOptions struct {
	roots                []string
	fileAuthority        string
	fileTrustClass       string
	packageAuthority     string
	packageTrustClass    string
	registry             hadronregistry.WorkflowResolver
	authoring            AuthoringSourceResolver
	maxSourceBytes       int64
	maxArchiveBytes      int64
	maxArchiveEntries    int
	maxArchiveTotalBytes int64
	afterFirstRead       func()
}

type rootedDefinitionPath struct {
	root     string
	relative string
	locator  string
	info     os.FileInfo
}

func normalizeDefinitionSourceOptions(options DefinitionResolverOptions) (definitionSourceOptions, error) {
	if options.Registry != nil && nilInterface(options.Registry) {
		return definitionSourceOptions{}, invalidDefinitionOptions("workflow registry must not be typed nil")
	}
	if options.Authoring != nil && nilInterface(options.Authoring) {
		return definitionSourceOptions{}, invalidDefinitionOptions("workflow authoring source resolver must not be typed nil")
	}
	if options.MaxSourceBytes < 0 || options.MaxArchiveBytes < 0 || options.MaxArchiveEntries < 0 || options.MaxArchiveTotalBytes < 0 {
		return definitionSourceOptions{}, invalidDefinitionOptions("source and archive bounds must not be negative")
	}
	result := definitionSourceOptions{
		fileAuthority: options.FileAuthority, fileTrustClass: options.FileTrustClass,
		packageAuthority: options.PackageAuthority, packageTrustClass: options.PackageTrustClass,
		registry: options.Registry, authoring: options.Authoring, maxSourceBytes: options.MaxSourceBytes,
		maxArchiveBytes: options.MaxArchiveBytes, maxArchiveEntries: options.MaxArchiveEntries,
		maxArchiveTotalBytes: options.MaxArchiveTotalBytes,
	}
	if result.fileAuthority == "" {
		result.fileAuthority = "project"
	}
	if result.fileTrustClass == "" {
		result.fileTrustClass = "project"
	}
	if result.packageAuthority == "" {
		result.packageAuthority = "package"
	}
	if result.packageTrustClass == "" {
		result.packageTrustClass = "packaged"
	}
	for _, field := range []struct{ name, value string }{
		{"file authority", result.fileAuthority},
		{"file trust class", result.fileTrustClass},
		{"package authority", result.packageAuthority},
		{"package trust class", result.packageTrustClass},
	} {
		if !utf8.ValidString(field.value) || strings.IndexFunc(field.value, unicode.IsControl) >= 0 {
			return definitionSourceOptions{}, invalidDefinitionOptions(field.name + " must be valid UTF-8 without control characters")
		}
	}
	if result.maxSourceBytes <= 0 {
		result.maxSourceBytes = 4 << 20
	}
	if result.maxArchiveBytes <= 0 {
		result.maxArchiveBytes = 16 << 20
	}
	if result.maxArchiveEntries <= 0 {
		result.maxArchiveEntries = 256
	}
	if result.maxArchiveTotalBytes <= 0 {
		result.maxArchiveTotalBytes = 32 << 20
	}
	for _, root := range options.Roots {
		canonical, err := canonicalRoot(root)
		if err != nil {
			return definitionSourceOptions{}, invalidDefinitionOptions(err.Error())
		}
		result.roots = append(result.roots, canonical)
	}
	return result, nil
}

func canonicalRoot(root string) (string, error) {
	if !utf8.ValidString(root) || strings.TrimSpace(root) == "" {
		return "", errors.New("definition root must be valid UTF-8 and non-empty")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve definition root: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve definition root symlinks: %w", err)
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", fmt.Errorf("stat definition root: %w", err)
	}
	if !info.IsDir() {
		return "", errors.New("definition root must be a directory")
	}
	return filepath.Clean(canonical), nil
}

func (r *DefinitionResolver) resolveFreshSource(ctx context.Context, requested graph.DefinitionRef) (ResolvedSource, error) {
	kind := strings.TrimSpace(requested.Kind)
	if kind == "" || kind == "workflow" {
		if requested.Locator != "" {
			kind = DefinitionKindFile
		} else if requested.ID != "" {
			kind = DefinitionKindRegistry
		}
	}
	switch kind {
	case DefinitionKindFile:
		return r.resolveFileSource(ctx, requested)
	case DefinitionKindRegistry:
		return r.resolveRegistrySource(ctx, requested)
	case DefinitionKindPackage:
		return r.resolvePackageSource(ctx, requested)
	case DefinitionKindAuthoring:
		return r.resolveAuthoringSource(ctx, requested)
	default:
		return ResolvedSource{}, definitionError(CodeDefinitionInvalid, ErrDefinitionUnresolved, requested.Locator, "workflow definition kind is unsupported", "Use file, registry, package, or a workflow locator reference.")
	}
}

func (r *DefinitionResolver) resolveAuthoringSource(ctx context.Context, requested graph.DefinitionRef) (ResolvedSource, error) {
	if r.sources.authoring == nil {
		return ResolvedSource{}, definitionError(CodeDefinitionUnresolved, ErrDefinitionUnresolved, requested.Locator, "workflow authoring material is unavailable", "Stage exact authoring material before validation.")
	}
	resolved, err := r.sources.authoring.ResolveAuthoringSource(ctx, requested)
	if err != nil {
		return ResolvedSource{}, definitionError(CodeDefinitionUnresolved, errors.Join(ErrDefinitionUnresolved, err), requested.Locator, "workflow authoring material could not be resolved exactly", "Restage the exact authoring envelope.")
	}
	if len(resolved.Bytes) == 0 || int64(len(resolved.Bytes)) > r.sources.maxSourceBytes {
		return ResolvedSource{}, definitionError(CodeDefinitionUnsafe, ErrUnsafeDefinitionSource, requested.Locator, "workflow authoring material is empty or exceeds the configured source bound", "Submit non-empty authoring material no larger than the host's maximum source bytes.")
	}
	if resolved.Requested.Kind == "" {
		resolved.Requested = requested
	}
	return resolved, nil
}

func (r *DefinitionResolver) resolveFileSource(ctx context.Context, requested graph.DefinitionRef) (ResolvedSource, error) {
	if strings.TrimSpace(requested.Locator) == "" {
		return ResolvedSource{}, definitionError(CodeDefinitionInvalid, ErrDefinitionUnresolved, "", "file workflow reference requires a locator", "Supply an explicit workflow file or directory locator.")
	}
	selected, err := r.resolveRootedPath(requested.Locator)
	if err != nil {
		return ResolvedSource{}, definitionError(CodeDefinitionUnsafe, errors.Join(ErrUnsafeDefinitionSource, err), requested.Locator, "workflow file is outside an authorized source root or unavailable", "Choose a graph-native workflow beneath an authorized root.")
	}
	if selected.info.IsDir() {
		selected, err = r.resolveRootedPath(filepath.Join(requested.Locator, "workflow.yaml"))
		if err != nil {
			return ResolvedSource{}, definitionError(CodeDefinitionUnresolved, errors.Join(ErrDefinitionUnresolved, err), requested.Locator, "workflow directory does not contain workflow.yaml", "Add workflow.yaml or reference a named *.workflow.yaml file.")
		}
	}
	if !selected.info.Mode().IsRegular() || !supportedDefinitionFile(filepath.Base(selected.relative)) {
		return ResolvedSource{}, definitionError(CodeDefinitionInvalid, ErrDefinitionUnresolved, requested.Locator, "workflow locator must select workflow.yaml or a named *.workflow.yaml file", "Rename the graph-native source to a supported workflow filename.")
	}
	contents, err := readStableFile(ctx, selected, r.sources.maxSourceBytes, r.sources.afterFirstRead)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ResolvedSource{}, ctxErr
		}
		return ResolvedSource{}, definitionError(CodeDefinitionUnsafe, errors.Join(ErrUnsafeDefinitionSource, err), requested.Locator, "workflow file changed or became unsafe while reading", "Retry after the workflow file is stable beneath its authorized root.")
	}
	digest := values.SHA256Digest(contents)
	if requested.Digest != "" && requested.Digest != digest {
		return ResolvedSource{}, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, requested.Locator, "workflow file content does not match its pinned digest", "Update the digest only after reviewing the exact source change.")
	}
	provenance := graph.Provenance{
		Authority: r.sources.fileAuthority, Origin: "workflow-file", Locator: selected.locator,
		Digest: digest, Metadata: graph.Metadata{"trust_class": r.sources.fileTrustClass},
	}
	definition := graph.DefinitionRef{
		Authority: r.sources.fileAuthority, Kind: "workflow", ID: requested.ID,
		Locator: selected.locator, Version: requested.Version, Digest: digest, Provenance: &provenance,
	}
	return ResolvedSource{Requested: requested, Definition: definition, Bytes: contents, Digest: digest, SourceFormat: graph.SourceWorkflow, SourceSchemaID: authoring.WorkflowSourceSchemaID, SourceSchemaVersion: authoring.WorkflowSourceSchemaVersion, TrustClass: r.sources.fileTrustClass, Movable: requested.Digest == ""}, nil
}

func (r *DefinitionResolver) resolveRegistrySource(ctx context.Context, requested graph.DefinitionRef) (ResolvedSource, error) {
	if r.sources.registry == nil {
		return ResolvedSource{}, definitionError(CodeDefinitionUnresolved, ErrDefinitionUnresolved, requested.Locator, "workflow registry resolution is unavailable", "Configure the Hadron graph-native workflow registry.")
	}
	resolution, err := r.sources.registry.ResolveWorkflow(ctx, hadronregistry.WorkflowQuery{Name: requested.ID, Version: requested.Version, Digest: requested.Digest})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ResolvedSource{}, ctxErr
		}
		code, cause := CodeDefinitionUnresolved, ErrDefinitionUnresolved
		if errors.Is(err, hadronregistry.ErrWorkflowConflict) {
			code, cause = CodeDefinitionPinConflict, ErrDefinitionPinConflict
		}
		return ResolvedSource{}, definitionError(code, errors.Join(cause, err), requested.Locator, "workflow registry reference could not be resolved exactly", "Use an existing immutable version or matching source digest.")
	}
	record := resolution.Record
	if len(record.Source) == 0 || int64(len(record.Source)) > r.sources.maxSourceBytes {
		return ResolvedSource{}, definitionError(CodeDefinitionUnsafe, ErrUnsafeDefinitionSource, requested.Locator, "workflow registry source is empty or exceeds the configured source bound", "Publish a non-empty graph-native workflow no larger than the host's maximum source bytes.")
	}
	if record.Name != requested.ID || (requested.Version != "" && record.Version != requested.Version) ||
		(requested.Digest != "" && record.Digest != requested.Digest) {
		return ResolvedSource{}, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, requested.Locator, "workflow registry returned a definition outside the requested immutable identity", "Repair the registry resolver so name, version, and digest pins are exact.")
	}
	if strings.TrimSpace(record.Version) == "" || strings.TrimSpace(record.Authority) == "" || strings.TrimSpace(record.TrustClass) == "" ||
		strings.TrimSpace(record.Provenance.Origin) == "" || strings.TrimSpace(record.Provenance.Locator) == "" ||
		(record.Provenance.Authority != "" && record.Provenance.Authority != record.Authority) ||
		(record.Provenance.Revision != "" && record.Provenance.Revision != record.Version) {
		return ResolvedSource{}, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, requested.Locator, "workflow registry returned incomplete or conflicting authority, trust, or provenance", "Repair the immutable graph-native registry record.")
	}
	provenance := record.Provenance
	if provenance.Authority == "" {
		provenance.Authority = record.Authority
	}
	if provenance.Metadata == nil {
		provenance.Metadata = make(graph.Metadata)
	}
	provenance.Metadata["trust_class"] = record.TrustClass
	definition := graph.DefinitionRef{
		Authority: record.Authority, Kind: "workflow", ID: record.SourceDefinitionID(),
		Locator: provenance.Locator, Version: record.Version, Digest: record.Digest, Provenance: &provenance,
	}
	return ResolvedSource{Requested: requested, Definition: definition, Bytes: record.Source, Digest: record.Digest, SourceFormat: record.SourceFormat, SourceSchemaID: record.SourceSchemaID, SourceSchemaVersion: record.SourceSchemaVersion, TrustClass: record.TrustClass, Movable: resolution.Movable}, nil
}

func (r *DefinitionResolver) resolvePackageSource(ctx context.Context, requested graph.DefinitionRef) (ResolvedSource, error) {
	archiveLocator, selector, err := splitPackageLocator(requested.Locator)
	if err != nil {
		return ResolvedSource{}, definitionError(CodeDefinitionInvalid, err, requested.Locator, "workflow package locator is invalid", "Use an archive path with an optional #workflow.yaml or #name.workflow.yaml selector.")
	}
	selectedPath, err := r.resolveRootedPath(archiveLocator)
	if err != nil || !selectedPath.info.Mode().IsRegular() {
		return ResolvedSource{}, definitionError(CodeDefinitionUnsafe, errors.Join(ErrUnsafeDefinitionSource, err), requested.Locator, "workflow package is outside an authorized source root or unavailable", "Choose a package beneath an authorized root.")
	}
	archive, err := readStableFile(ctx, selectedPath, r.sources.maxArchiveBytes, r.sources.afterFirstRead)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ResolvedSource{}, ctxErr
		}
		return ResolvedSource{}, definitionError(CodeDefinitionUnsafe, errors.Join(ErrUnsafeDefinitionSource, err), requested.Locator, "workflow package changed or became unsafe while reading", "Retry after the package is stable beneath its authorized root.")
	}
	selected, err := pack.ReadWorkflowSource(archive, selector, pack.WorkflowArchiveLimits{
		MaxArchiveBytes: r.sources.maxArchiveBytes, MaxEntries: r.sources.maxArchiveEntries,
		MaxTotalBytes: r.sources.maxArchiveTotalBytes, MaxSourceBytes: r.sources.maxSourceBytes,
	})
	if err != nil {
		return ResolvedSource{}, definitionError(CodeDefinitionUnsafe, errors.Join(ErrUnsafeDefinitionSource, err), requested.Locator, "workflow package is malformed or ambiguous", "Use a bounded package containing one selected graph-native workflow source.")
	}
	if err := ctx.Err(); err != nil {
		return ResolvedSource{}, err
	}
	if requested.Digest != "" && requested.Digest != selected.SourceDigest {
		return ResolvedSource{}, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, requested.Locator, "selected package workflow does not match its pinned source digest", "Pin the selected workflow source digest, not the package container digest.")
	}
	virtualLocator := selectedPath.locator + "!/" + selected.Entry
	provenance := graph.Provenance{
		Authority: r.sources.packageAuthority, Origin: "workflow-package", Locator: virtualLocator,
		Digest: selected.SourceDigest,
		Metadata: graph.Metadata{
			"trust_class": r.sources.packageTrustClass, "package_digest": selected.ArchiveDigest,
			"package_locator": selectedPath.locator, "package_entry": selected.Entry,
		},
	}
	definition := graph.DefinitionRef{
		Authority: r.sources.packageAuthority, Kind: "workflow", ID: requested.ID,
		Locator: virtualLocator, Version: requested.Version, Digest: selected.SourceDigest, Provenance: &provenance,
	}
	return ResolvedSource{Requested: requested, Definition: definition, Bytes: selected.Source, Digest: selected.SourceDigest, SourceFormat: graph.SourceWorkflow, SourceSchemaID: authoring.WorkflowSourceSchemaID, SourceSchemaVersion: authoring.WorkflowSourceSchemaVersion, TrustClass: r.sources.packageTrustClass, Movable: requested.Digest == ""}, nil
}

func (r *DefinitionResolver) resolveRootedPath(locator string) (rootedDefinitionPath, error) {
	if len(r.sources.roots) == 0 {
		return rootedDefinitionPath{}, errors.New("no authorized definition roots are configured")
	}
	if !utf8.ValidString(locator) || strings.TrimSpace(locator) == "" || strings.ContainsRune(locator, '\x00') {
		return rootedDefinitionPath{}, errors.New("locator must be valid UTF-8 and non-empty")
	}
	var found *rootedDefinitionPath
	var lastErr error
	for _, rootName := range r.sources.roots {
		candidate := locator
		if !filepath.IsAbs(candidate) {
			candidate = filepath.Join(rootName, candidate)
		}
		absolute, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		canonicalCandidate, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			lastErr = err
			continue
		}
		relative, err := filepath.Rel(rootName, canonicalCandidate)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
			continue
		}
		root, err := os.OpenRoot(rootName)
		if err != nil {
			lastErr = err
			continue
		}
		info, statErr := root.Stat(filepath.ToSlash(relative))
		closeErr := root.Close()
		if statErr != nil || closeErr != nil {
			lastErr = errors.Join(statErr, closeErr)
			continue
		}
		candidatePath := rootedDefinitionPath{
			root: rootName, relative: filepath.ToSlash(relative),
			locator: canonicalCandidate, info: info,
		}
		if found != nil && (found.root != candidatePath.root || found.relative != candidatePath.relative) {
			return rootedDefinitionPath{}, errors.New("relative locator is ambiguous across authorized roots")
		}
		found = &candidatePath
	}
	if found == nil {
		if lastErr != nil {
			return rootedDefinitionPath{}, fmt.Errorf("locator does not resolve beneath an authorized root: %w", lastErr)
		}
		return rootedDefinitionPath{}, errors.New("locator does not resolve beneath an authorized root")
	}
	return *found, nil
}

func readStableFile(ctx context.Context, path rootedDefinitionPath, limit int64, afterFirstRead func()) ([]byte, error) {
	root, err := os.OpenRoot(path.root)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	first, firstInfo, err := readFileSnapshot(ctx, root, path.relative, limit)
	if err != nil {
		return nil, err
	}
	if afterFirstRead != nil {
		afterFirstRead()
	}
	second, secondInfo, err := readFileSnapshot(ctx, root, path.relative, limit)
	if err != nil {
		return nil, err
	}
	if !os.SameFile(firstInfo, secondInfo) || !firstInfo.ModTime().Equal(secondInfo.ModTime()) || firstInfo.Size() != secondInfo.Size() || !bytes.Equal(first, second) {
		return nil, errors.New("source changed across stable reads")
	}
	return first, nil
}

func readFileSnapshot(ctx context.Context, root *os.Root, path string, limit int64) ([]byte, os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	file, err := root.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = file.Close() }()
	before, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > limit {
		return nil, nil, fmt.Errorf("source must be a regular file no larger than %d bytes", limit)
	}
	contents, err := io.ReadAll(io.LimitReader(file, saturatingOverflowProbe(limit)))
	if err != nil {
		return nil, nil, err
	}
	if int64(len(contents)) > limit {
		return nil, nil, fmt.Errorf("source exceeds %d bytes", limit)
	}
	after, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	pathInfo, err := root.Stat(path)
	if err != nil {
		return nil, nil, err
	}
	if !os.SameFile(before, after) || !os.SameFile(after, pathInfo) || !before.ModTime().Equal(after.ModTime()) || before.Size() != after.Size() {
		return nil, nil, errors.New("source changed during read")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return contents, after, nil
}

func saturatingOverflowProbe(limit int64) int64 {
	if limit == math.MaxInt64 {
		return math.MaxInt64
	}
	return limit + 1
}

func splitPackageLocator(locator string) (string, string, error) {
	if strings.Count(locator, "#") > 1 {
		return "", "", errors.New("package locator contains multiple selectors")
	}
	archive, selector, _ := strings.Cut(locator, "#")
	if strings.TrimSpace(archive) == "" {
		return "", "", errors.New("package archive path is required")
	}
	return archive, selector, nil
}

func supportedDefinitionFile(name string) bool {
	return name == "workflow.yaml" || (len(name) > len(".workflow.yaml") && strings.HasSuffix(name, ".workflow.yaml"))
}
