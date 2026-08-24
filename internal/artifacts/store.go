package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/values"
)

// Store combines Hadron-local run/project storage with explicitly approved
// external artifact authorities.
type Store struct {
	root            string
	rootIdentity    os.FileInfo
	objectsIdentity os.FileInfo
	stagingIdentity os.FileInfo
	authorizer      values.ArtifactAuthorizer
	externals       map[string]values.ArtifactStore
}

// New constructs a fail-closed adapter. External authorities are copied and
// must use canonical lower-case names distinct from hadron-local.
func New(root string, authorizer values.ArtifactAuthorizer, external map[string]values.ArtifactStore) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, values.ErrArtifactInvalid)
	}
	if authorizer == nil {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureAuthority, nil, values.ErrArtifactAuthority)
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, err)
	}
	pathErr := rejectSymlinkComponents(absolute)
	if pathErr != nil {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, pathErr)
	}
	mkdirErr := os.MkdirAll(absolute, 0o700)
	if mkdirErr != nil {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, mkdirErr)
	}
	pathErr = rejectSymlinkComponents(absolute)
	if pathErr != nil {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, pathErr)
	}
	rootInfo, err := os.Lstat(absolute)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, values.ErrArtifactInvalid)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, err)
	}
	if resolved != absolute {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, values.ErrArtifactInvalid)
	}
	chmodErr := os.Chmod(resolved, 0o700) // #nosec G302 -- directories must be owner-only and traversable.
	if chmodErr != nil {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, chmodErr)
	}
	for _, component := range []string{"objects", "staging"} {
		_, chainErr := ensureDirectoryChain(resolved, component)
		if chainErr != nil {
			return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, chainErr)
		}
	}
	rootIdentity, err := os.Lstat(resolved)
	if err != nil {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, err)
	}
	objectsIdentity, err := os.Lstat(filepath.Join(resolved, "objects"))
	if err != nil {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, err)
	}
	stagingIdentity, err := os.Lstat(filepath.Join(resolved, "staging"))
	if err != nil {
		return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, nil, err)
	}
	copied := make(map[string]values.ArtifactStore, len(external))
	for authority, delegate := range external {
		if !canonicalAuthority(authority) || authority == LocalAuthority || delegate == nil {
			return nil, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureAuthority, nil, values.ErrArtifactAuthority)
		}
		copied[authority] = delegate
	}
	return &Store{
		root: resolved, rootIdentity: rootIdentity,
		objectsIdentity: objectsIdentity, stagingIdentity: stagingIdentity,
		authorizer: authorizer, externals: copied,
	}, nil
}

// Put streams a locally durable run/project artifact. None and external are
// rejected by the core request validation and unknown stores remain fail-closed.
func (s *Store) Put(ctx context.Context, request values.ArtifactPutRequest, source io.Reader) (values.ArtifactMetadata, error) {
	request = snapshotPutRequest(request)
	if err := checkContext(ctx, values.ArtifactOperationPut, nil); err != nil {
		return values.ArtifactMetadata{}, err
	}
	if err := request.Validate(); err != nil {
		return values.ArtifactMetadata{}, err
	}
	if !canonicalAuthority(request.Store) {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureAuthority, nil, values.ErrArtifactAuthority)
	}
	if err := s.authorize(ctx, values.ArtifactOperationPut, request.Access, nil, &request.Owner); err != nil {
		return values.ArtifactMetadata{}, err
	}
	if request.Store != LocalAuthority {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureAuthority, nil, values.ErrArtifactAuthority)
	}
	if source == nil {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, nil, values.ErrArtifactInvalid)
	}
	if err := s.validateLocalRoots(); err != nil {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, nil, err)
	}
	return s.putLocal(ctx, request, source)
}

func (s *Store) putLocal(ctx context.Context, request values.ArtifactPutRequest, source io.Reader) (metadata values.ArtifactMetadata, resultErr error) {
	stagingRoot := filepath.Join(s.root, "staging")
	stage, err := os.MkdirTemp(stagingRoot, "partial-")
	if err != nil {
		return metadata, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, nil, err)
	}
	stageModeErr := os.Chmod(stage, 0o700) // #nosec G302 -- staging directories must be owner-only and traversable.
	if stageModeErr != nil {
		_ = os.RemoveAll(stage)
		return metadata, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, nil, stageModeErr)
	}
	defer func() {
		if resultErr != nil || stage != "" {
			_ = os.RemoveAll(stage)
		}
	}()

	payloadPath := filepath.Join(stage, payloadName)
	payload, err := os.OpenFile(payloadPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- stage is rooted, generated, and identity-checked.
	if err != nil {
		return metadata, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, nil, err)
	}
	hasher := sha256.New()
	limited := &io.LimitedReader{R: contextReader{ctx: ctx, reader: source}, N: request.MaxBytes + 1}
	size, copyErr := io.Copy(io.MultiWriter(payload, hasher), limited)
	if copyErr == nil {
		copyErr = payload.Sync()
	}
	closeErr := payload.Close()
	if copyErr != nil {
		return metadata, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, nil, copyErr)
	}
	if closeErr != nil {
		return metadata, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, nil, closeErr)
	}
	contextErr := checkContext(ctx, values.ArtifactOperationPut, nil)
	if contextErr != nil {
		return metadata, contextErr
	}
	if size > request.MaxBytes {
		return metadata, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureSize, nil, values.ErrArtifactSizeLimit)
	}
	digest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if request.ExpectedDigest != "" && request.ExpectedDigest != digest {
		return metadata, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureDigest, nil, values.ErrArtifactDigest)
	}
	if request.ExpectedSize != nil && *request.ExpectedSize != size {
		return metadata, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureDigest, nil, values.ErrArtifactDigest)
	}

	createdAt := request.CreatedAt.UTC().Round(0)
	expiresAt := request.ExpiresAt
	if !expiresAt.IsZero() {
		expiresAt = expiresAt.UTC().Round(0)
	}
	ownerHash := hashOwner(request.Owner.ID)
	identity := artifactIdentity{
		Store: LocalAuthority, OwnerScope: request.Owner.Scope, OwnerHash: ownerHash,
		Digest: digest, MediaType: request.Metadata.MediaType, SizeBytes: size,
		Producer: request.Metadata.Producer, Redaction: request.Metadata.Redaction,
		Retention: request.Metadata.Retention, CreatedAt: createdAt, ExpiresAt: expiresAt,
	}
	id, err := artifactID(identity)
	if err != nil {
		return metadata, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, nil, err)
	}
	locator := localLocator{scope: request.Owner.Scope, ownerHash: ownerHash, artifactID: id}
	ref := values.ArtifactRef{
		Store: LocalAuthority, URI: localURI(locator), Digest: digest,
		MediaType: request.Metadata.MediaType, SizeBytes: size, Producer: request.Metadata.Producer,
		Redaction: request.Metadata.Redaction, Retention: request.Metadata.Retention,
	}
	metadata = values.ArtifactMetadata{Ref: ref, Owner: request.Owner, CreatedAt: createdAt, ExpiresAt: expiresAt}
	manifest, err := encodeManifest(metadata)
	if err != nil {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, &ref, err)
	}
	manifestFile, err := os.OpenFile(filepath.Join(stage, manifestName), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- stage is rooted, generated, and identity-checked.
	if err != nil {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, &ref, err)
	}
	_, writeErr := manifestFile.Write(manifest)
	if writeErr == nil {
		writeErr = manifestFile.Sync()
	}
	manifestCloseErr := manifestFile.Close()
	if writeErr != nil {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, &ref, writeErr)
	}
	if manifestCloseErr != nil {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, &ref, manifestCloseErr)
	}
	if syncErr := syncDirectory(stage); syncErr != nil {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, &ref, syncErr)
	}
	contextErr = checkContext(ctx, values.ArtifactOperationPut, &ref)
	if contextErr != nil {
		return values.ArtifactMetadata{}, contextErr
	}

	parent, err := ensureDirectoryChain(filepath.Join(s.root, "objects"), string(locator.scope), locator.ownerHash)
	if err != nil {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, &ref, err)
	}
	final := filepath.Join(parent, locator.artifactID)
	if existing, err := verifyStored(final, locator, &ref); err == nil {
		return existing, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, &ref, err)
	}
	if err := os.Rename(stage, final); err != nil {
		if existing, verifyErr := verifyStored(final, locator, &ref); verifyErr == nil {
			return existing, nil
		}
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, &ref, err)
	}
	stage = ""
	if err := syncDirectory(parent); err != nil {
		_ = os.RemoveAll(final)
		return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationPut, values.ArtifactFailureInvalid, &ref, err)
	}
	return metadata, nil
}

// Stat returns immutable metadata after pre-resolution and owner-aware checks.
func (s *Store) Stat(ctx context.Context, access values.ArtifactAccess, ref values.ArtifactRef) (values.ArtifactMetadata, error) {
	if err := checkContext(ctx, values.ArtifactOperationStat, &ref); err != nil {
		return values.ArtifactMetadata{}, err
	}
	if err := validateCanonicalRef(ref); err != nil {
		return values.ArtifactMetadata{}, operationError(values.ArtifactOperationStat, ref, err)
	}
	if err := s.authorize(ctx, values.ArtifactOperationStat, access, &ref, nil); err != nil {
		return values.ArtifactMetadata{}, err
	}
	if ref.Store != LocalAuthority {
		delegate, ok := s.externals[ref.Store]
		if !ok {
			return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureAuthority, &ref, values.ErrArtifactAuthority)
		}
		metadata, err := delegate.Stat(ctx, access, ref)
		if err != nil {
			return values.ArtifactMetadata{}, safeDelegateError(values.ArtifactOperationStat, ref, err)
		}
		if metadata.Ref != ref || metadata.Ref.Retention != values.RetentionExternal || metadata.Validate() != nil {
			return values.ArtifactMetadata{}, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
		}
		if err := s.authorize(ctx, values.ArtifactOperationStat, access, &ref, &metadata.Owner); err != nil {
			return values.ArtifactMetadata{}, err
		}
		if err := values.CheckArtifactExpiry(values.ArtifactOperationStat, metadata, access.At); err != nil {
			return values.ArtifactMetadata{}, err
		}
		return metadata, nil
	}
	if err := s.validateLocalRoots(); err != nil {
		return values.ArtifactMetadata{}, operationError(values.ArtifactOperationStat, ref, err)
	}
	return s.statLocal(ctx, values.ArtifactOperationStat, access, ref)
}

// Open returns a digest-verifying stream only after authorization and expiry.
func (s *Store) Open(ctx context.Context, access values.ArtifactAccess, ref values.ArtifactRef) (values.ArtifactReadCloser, error) {
	if err := checkContext(ctx, values.ArtifactOperationOpen, &ref); err != nil {
		return nil, err
	}
	if err := validateCanonicalRef(ref); err != nil {
		return nil, operationError(values.ArtifactOperationOpen, ref, err)
	}
	if err := s.authorize(ctx, values.ArtifactOperationOpen, access, &ref, nil); err != nil {
		return nil, err
	}
	if ref.Store != LocalAuthority {
		delegate, ok := s.externals[ref.Store]
		if !ok {
			return nil, values.NewArtifactError(values.ArtifactOperationOpen, values.ArtifactFailureAuthority, &ref, values.ErrArtifactAuthority)
		}
		reader, err := delegate.Open(ctx, access, ref)
		if err != nil {
			return nil, safeDelegateError(values.ArtifactOperationOpen, ref, err)
		}
		if reader == nil {
			return nil, values.NewArtifactError(values.ArtifactOperationOpen, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
		}
		metadata := reader.Metadata()
		if metadata.Ref != ref || metadata.Ref.Retention != values.RetentionExternal || metadata.Validate() != nil {
			_ = reader.Close()
			return nil, values.NewArtifactError(values.ArtifactOperationOpen, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
		}
		if err := s.authorize(ctx, values.ArtifactOperationOpen, access, &ref, &metadata.Owner); err != nil {
			_ = reader.Close()
			return nil, err
		}
		if err := values.CheckArtifactExpiry(values.ArtifactOperationOpen, metadata, access.At); err != nil {
			_ = reader.Close()
			return nil, err
		}
		return reader, nil
	}
	if err := s.validateLocalRoots(); err != nil {
		return nil, operationError(values.ArtifactOperationOpen, ref, err)
	}
	metadata, err := s.statLocal(ctx, values.ArtifactOperationOpen, access, ref)
	if err != nil {
		return nil, err
	}
	locator, _ := parseLocalRef(ref)
	directory := s.artifactDirectory(locator)
	payload, payloadInfo, err := openRegularFile(filepath.Join(directory, payloadName), metadata.Ref.SizeBytes)
	if err != nil {
		return nil, operationError(values.ArtifactOperationOpen, ref, err)
	}
	if payloadInfo.Size() != metadata.Ref.SizeBytes {
		_ = payload.Close()
		return nil, values.NewArtifactError(values.ArtifactOperationOpen, values.ArtifactFailureDigest, &ref, values.ErrArtifactDigest)
	}
	if verifyErr := verifyPayloadFile(ctx, payload, metadata.Ref); verifyErr != nil {
		_ = payload.Close()
		return nil, verifyErr
	}
	reader, err := values.NewVerifyingArtifactReader(metadata, payload)
	if err != nil {
		_ = payload.Close()
		return nil, err
	}
	return reader, nil
}

func (s *Store) statLocal(ctx context.Context, operation values.ArtifactOperation, access values.ArtifactAccess, ref values.ArtifactRef) (values.ArtifactMetadata, error) {
	locator, err := parseLocalRef(ref)
	if err != nil {
		return values.ArtifactMetadata{}, operationError(operation, ref, err)
	}
	metadata, err := verifyStored(s.artifactDirectory(locator), locator, &ref)
	if err != nil {
		return values.ArtifactMetadata{}, operationError(operation, ref, err)
	}
	if err := s.authorize(ctx, operation, access, &ref, &metadata.Owner); err != nil {
		return values.ArtifactMetadata{}, err
	}
	if err := values.CheckArtifactExpiry(operation, metadata, access.At); err != nil {
		return values.ArtifactMetadata{}, err
	}
	return metadata, nil
}

// Delete removes one local reference idempotently. External references are
// preserved and never delegated for deletion.
func (s *Store) Delete(ctx context.Context, request values.ArtifactDeleteRequest) (values.ArtifactCleanupResult, error) {
	if err := checkContext(ctx, values.ArtifactOperationDelete, &request.Ref); err != nil {
		return values.ArtifactCleanupResult{}, err
	}
	if err := request.Validate(); err != nil {
		return values.ArtifactCleanupResult{}, err
	}
	if err := validateCanonicalRef(request.Ref); err != nil {
		return values.ArtifactCleanupResult{}, operationError(values.ArtifactOperationDelete, request.Ref, err)
	}
	if err := s.authorize(ctx, values.ArtifactOperationDelete, request.Access, &request.Ref, nil); err != nil {
		return values.ArtifactCleanupResult{}, err
	}
	if request.Ref.Store != LocalAuthority {
		if _, ok := s.externals[request.Ref.Store]; !ok {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationDelete, values.ArtifactFailureAuthority, &request.Ref, values.ErrArtifactAuthority)
		}
		if request.Ref.Retention != values.RetentionExternal {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationDelete, values.ArtifactFailureRetention, &request.Ref, values.ErrArtifactRetention)
		}
		return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupPreservedExternal}, nil
	}
	if err := s.validateLocalRoots(); err != nil {
		return values.ArtifactCleanupResult{}, operationError(values.ArtifactOperationDelete, request.Ref, err)
	}
	locator, err := parseLocalRef(request.Ref)
	if err != nil {
		return values.ArtifactCleanupResult{}, operationError(values.ArtifactOperationDelete, request.Ref, err)
	}
	directory := s.artifactDirectory(locator)
	metadata, err := verifyStored(directory, locator, &request.Ref)
	if errors.Is(err, os.ErrNotExist) {
		return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupAlreadyAbsent}, nil
	}
	if err != nil {
		return values.ArtifactCleanupResult{}, operationError(values.ArtifactOperationDelete, request.Ref, err)
	}
	if err := s.authorize(ctx, values.ArtifactOperationDelete, request.Access, &request.Ref, &metadata.Owner); err != nil {
		return values.ArtifactCleanupResult{}, err
	}
	if err := checkContext(ctx, values.ArtifactOperationDelete, &request.Ref); err != nil {
		return values.ArtifactCleanupResult{}, err
	}
	if err := os.RemoveAll(directory); err != nil {
		return values.ArtifactCleanupResult{}, operationError(values.ArtifactOperationDelete, request.Ref, err)
	}
	return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupDeleted, DeletedCount: 1}, nil
}

// Cleanup applies deterministic none/external/owner/expiry/partial retention.
func (s *Store) Cleanup(ctx context.Context, request values.ArtifactCleanupRequest) (values.ArtifactCleanupResult, error) {
	if err := checkContext(ctx, values.ArtifactOperationCleanup, request.Ref); err != nil {
		return values.ArtifactCleanupResult{}, err
	}
	if err := request.Validate(); err != nil {
		return values.ArtifactCleanupResult{}, err
	}
	if request.Ref != nil {
		if err := validateCanonicalRef(*request.Ref); err != nil {
			return values.ArtifactCleanupResult{}, operationError(values.ArtifactOperationCleanup, *request.Ref, err)
		}
	}
	if err := s.authorize(ctx, values.ArtifactOperationCleanup, request.Access, request.Ref, cleanupOwner(request)); err != nil {
		return values.ArtifactCleanupResult{}, err
	}
	switch request.Kind {
	case values.ArtifactCleanupNone:
		return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupNotStored}, nil
	case values.ArtifactCleanupExternal:
		if _, ok := s.externals[request.Ref.Store]; !ok {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureAuthority, request.Ref, values.ErrArtifactAuthority)
		}
		return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupPreservedExternal}, nil
	case values.ArtifactCleanupPartials:
		if err := s.validateLocalRoots(); err != nil {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, err)
		}
		return s.cleanupPartials(ctx, request.Before)
	case values.ArtifactCleanupRun, values.ArtifactCleanupProject:
		if err := s.validateLocalRoots(); err != nil {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, err)
		}
		return s.cleanupOwner(ctx, request)
	case values.ArtifactCleanupExpired:
		if err := s.validateLocalRoots(); err != nil {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, err)
		}
		return s.cleanupExpired(ctx, request)
	default:
		return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, request.Ref, values.ErrArtifactInvalid)
	}
}

func (s *Store) cleanupOwner(ctx context.Context, request values.ArtifactCleanupRequest) (values.ArtifactCleanupResult, error) {
	ownerDirectory := filepath.Join(s.root, "objects", string(request.Owner.Scope), hashOwner(request.Owner.ID))
	candidates, err := s.collectOwnerArtifacts(ctx, ownerDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupAlreadyAbsent}, nil
	}
	if err != nil {
		return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, err)
	}
	for _, candidate := range candidates {
		if err := checkContext(ctx, values.ArtifactOperationCleanup, &candidate.metadata.Ref); err != nil {
			return values.ArtifactCleanupResult{}, err
		}
		if candidate.metadata.Owner != request.Owner {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, &candidate.metadata.Ref, values.ErrArtifactInvalid)
		}
		if err := s.authorize(ctx, values.ArtifactOperationCleanup, request.Access, &candidate.metadata.Ref, &candidate.metadata.Owner); err != nil {
			return values.ArtifactCleanupResult{}, err
		}
	}
	return removeCandidates(ctx, candidates, values.ArtifactCleanupAlreadyAbsent)
}

func (s *Store) cleanupExpired(ctx context.Context, request values.ArtifactCleanupRequest) (values.ArtifactCleanupResult, error) {
	var candidates []storedCandidate
	for _, scope := range []values.ArtifactOwnerScope{values.ArtifactOwnerRun, values.ArtifactOwnerProject} {
		if err := checkContext(ctx, values.ArtifactOperationCleanup, nil); err != nil {
			return values.ArtifactCleanupResult{}, err
		}
		scopeRoot := filepath.Join(s.root, "objects", string(scope))
		owners, err := os.ReadDir(scopeRoot)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, err)
		}
		for _, ownerEntry := range owners {
			if err := checkContext(ctx, values.ArtifactOperationCleanup, nil); err != nil {
				return values.ArtifactCleanupResult{}, err
			}
			if ownerEntry.Type()&os.ModeSymlink != 0 || !ownerEntry.IsDir() || len(ownerEntry.Name()) != sha256.Size*2 || !lowerHex(ownerEntry.Name()) {
				return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, values.ErrArtifactInvalid)
			}
			ownerCandidates, err := s.collectOwnerArtifacts(ctx, filepath.Join(scopeRoot, ownerEntry.Name()))
			if err != nil {
				return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, err)
			}
			for _, candidate := range ownerCandidates {
				if !candidate.metadata.ExpiresAt.IsZero() && !candidate.metadata.ExpiresAt.After(request.Before) {
					candidates = append(candidates, candidate)
				}
			}
		}
	}
	sortCandidates(candidates)
	for _, candidate := range candidates {
		if err := checkContext(ctx, values.ArtifactOperationCleanup, &candidate.metadata.Ref); err != nil {
			return values.ArtifactCleanupResult{}, err
		}
		if err := s.authorize(ctx, values.ArtifactOperationCleanup, request.Access, &candidate.metadata.Ref, &candidate.metadata.Owner); err != nil {
			return values.ArtifactCleanupResult{}, err
		}
	}
	return removeCandidates(ctx, candidates, values.ArtifactCleanupNotStored)
}

func (s *Store) cleanupPartials(ctx context.Context, before time.Time) (values.ArtifactCleanupResult, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, "staging"))
	if err != nil {
		return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, err)
	}
	var paths []string
	for _, entry := range entries {
		if err := checkContext(ctx, values.ArtifactOperationCleanup, nil); err != nil {
			return values.ArtifactCleanupResult{}, err
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() || !strings.HasPrefix(entry.Name(), "partial-") {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, values.ErrArtifactInvalid)
		}
		info, err := entry.Info()
		if err != nil {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, err)
		}
		if !info.ModTime().After(before) {
			paths = append(paths, filepath.Join(s.root, "staging", entry.Name()))
		}
	}
	sort.Strings(paths)
	for _, path := range paths {
		if err := checkContext(ctx, values.ArtifactOperationCleanup, nil); err != nil {
			return values.ArtifactCleanupResult{}, err
		}
		if err := os.RemoveAll(path); err != nil {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, nil, err)
		}
	}
	if len(paths) == 0 {
		return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupNotStored}, nil
	}
	return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupDeleted, DeletedCount: len(paths)}, nil
}

type storedCandidate struct {
	path     string
	metadata values.ArtifactMetadata
}

func (s *Store) collectOwnerArtifacts(ctx context.Context, ownerDirectory string) ([]storedCandidate, error) {
	ownerInfo, err := os.Lstat(ownerDirectory)
	if err != nil {
		return nil, err
	}
	if !ownerInfo.IsDir() || ownerInfo.Mode()&os.ModeSymlink != 0 {
		return nil, values.ErrArtifactInvalid
	}
	entries, err := os.ReadDir(ownerDirectory)
	if err != nil {
		return nil, err
	}
	candidates := make([]storedCandidate, 0, len(entries))
	for _, entry := range entries {
		if err := checkContext(ctx, values.ArtifactOperationCleanup, nil); err != nil {
			return nil, err
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() || len(entry.Name()) != sha256.Size*2 || !lowerHex(entry.Name()) {
			return nil, values.ErrArtifactInvalid
		}
		directory := filepath.Join(ownerDirectory, entry.Name())
		manifest, err := readManifest(directory)
		if err != nil {
			return nil, err
		}
		locator, err := parseLocalRef(manifest.Metadata.Ref)
		if err != nil || locator.artifactID != entry.Name() ||
			locator.ownerHash != filepath.Base(ownerDirectory) ||
			string(locator.scope) != filepath.Base(filepath.Dir(ownerDirectory)) {
			return nil, values.ErrArtifactInvalid
		}
		metadata, err := verifyStored(directory, locator, &manifest.Metadata.Ref)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, storedCandidate{path: directory, metadata: metadata})
	}
	sortCandidates(candidates)
	return candidates, nil
}

func removeCandidates(ctx context.Context, candidates []storedCandidate, empty values.ArtifactCleanupOutcome) (values.ArtifactCleanupResult, error) {
	if len(candidates) == 0 {
		return values.ArtifactCleanupResult{Outcome: empty}, nil
	}
	for _, candidate := range candidates {
		if err := checkContext(ctx, values.ArtifactOperationCleanup, &candidate.metadata.Ref); err != nil {
			return values.ArtifactCleanupResult{}, err
		}
		if err := os.RemoveAll(candidate.path); err != nil {
			return values.ArtifactCleanupResult{}, values.NewArtifactError(values.ArtifactOperationCleanup, values.ArtifactFailureInvalid, &candidate.metadata.Ref, err)
		}
	}
	return values.ArtifactCleanupResult{Outcome: values.ArtifactCleanupDeleted, DeletedCount: len(candidates)}, nil
}

func sortCandidates(candidates []storedCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].metadata.Ref.Digest == candidates[j].metadata.Ref.Digest {
			return candidates[i].metadata.Ref.URI < candidates[j].metadata.Ref.URI
		}
		return candidates[i].metadata.Ref.Digest < candidates[j].metadata.Ref.Digest
	})
}

func cleanupOwner(request values.ArtifactCleanupRequest) *values.ArtifactOwner {
	if request.Kind == values.ArtifactCleanupRun || request.Kind == values.ArtifactCleanupProject {
		owner := request.Owner
		return &owner
	}
	return nil
}

func (s *Store) artifactDirectory(locator localLocator) string {
	return filepath.Join(s.root, "objects", string(locator.scope), locator.ownerHash, locator.artifactID)
}

func (s *Store) authorize(ctx context.Context, operation values.ArtifactOperation, access values.ArtifactAccess, ref *values.ArtifactRef, owner *values.ArtifactOwner) error {
	if err := checkContext(ctx, operation, ref); err != nil {
		return err
	}
	if err := access.Validate(operation); err != nil {
		return err
	}
	var refCopy *values.ArtifactRef
	if ref != nil {
		copyValue := *ref
		refCopy = &copyValue
	}
	var ownerCopy *values.ArtifactOwner
	if owner != nil {
		copyValue := *owner
		ownerCopy = &copyValue
	}
	if err := s.authorizer.AuthorizeArtifact(ctx, values.ArtifactAuthorization{
		Operation: operation, Access: access, Ref: refCopy, Owner: ownerCopy,
	}); err != nil {
		return values.NewArtifactError(operation, values.ArtifactFailureUnauthorized, ref, err)
	}
	return nil
}

func operationError(operation values.ArtifactOperation, ref values.ArtifactRef, cause error) error {
	failure := artifactFailure(cause)
	return values.NewArtifactError(operation, failure, &ref, cause)
}

func safeDelegateError(operation values.ArtifactOperation, ref values.ArtifactRef, cause error) error {
	return values.NewArtifactError(operation, artifactFailure(cause), &ref, cause)
}

func artifactFailure(cause error) values.ArtifactFailure {
	switch {
	case errors.Is(cause, values.ErrArtifactAuthority):
		return values.ArtifactFailureAuthority
	case errors.Is(cause, values.ErrArtifactUnauthorized):
		return values.ArtifactFailureUnauthorized
	case errors.Is(cause, os.ErrNotExist):
		return values.ArtifactFailureNotFound
	case errors.Is(cause, values.ErrArtifactNotFound):
		return values.ArtifactFailureNotFound
	case errors.Is(cause, values.ErrArtifactDigest):
		return values.ArtifactFailureDigest
	case errors.Is(cause, values.ErrArtifactExpired):
		return values.ArtifactFailureExpired
	case errors.Is(cause, values.ErrArtifactSizeLimit):
		return values.ArtifactFailureSize
	case errors.Is(cause, values.ErrArtifactRetention):
		return values.ArtifactFailureRetention
	default:
		return values.ArtifactFailureInvalid
	}
}

func syncDirectory(path string) error {
	directory, err := os.Open(path) // #nosec G304 -- path is a generated directory under an identity-checked root.
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

func (s *Store) validateLocalRoots() error {
	rootInfo, err := os.Lstat(s.root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(s.rootIdentity, rootInfo) {
		return values.ErrArtifactInvalid
	}
	for _, entry := range []struct {
		name     string
		identity os.FileInfo
	}{
		{name: "objects", identity: s.objectsIdentity},
		{name: "staging", identity: s.stagingIdentity},
	} {
		info, err := os.Lstat(filepath.Join(s.root, entry.name))
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(entry.identity, info) {
			return values.ErrArtifactInvalid
		}
	}
	return nil
}

func verifyPayloadFile(ctx context.Context, file *os.File, ref values.ArtifactRef) error {
	hasher := sha256.New()
	size, err := io.Copy(hasher, contextReader{ctx: ctx, reader: file})
	if err != nil {
		return values.NewArtifactError(values.ArtifactOperationOpen, values.ArtifactFailureInvalid, &ref, err)
	}
	actual := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if size != ref.SizeBytes || actual != ref.Digest {
		return values.NewArtifactError(values.ArtifactOperationOpen, values.ArtifactFailureDigest, &ref, values.ErrArtifactDigest)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return values.NewArtifactError(values.ArtifactOperationOpen, values.ArtifactFailureInvalid, &ref, err)
	}
	return nil
}

func snapshotPutRequest(request values.ArtifactPutRequest) values.ArtifactPutRequest {
	if request.ExpectedSize != nil {
		expected := *request.ExpectedSize
		request.ExpectedSize = &expected
	}
	return request
}

func checkContext(ctx context.Context, operation values.ArtifactOperation, ref *values.ArtifactRef) error {
	if ctx == nil {
		return values.NewArtifactError(operation, values.ArtifactFailureInvalid, ref, values.ErrArtifactInvalid)
	}
	if err := ctx.Err(); err != nil {
		return values.NewArtifactError(operation, values.ArtifactFailureInvalid, ref, err)
	}
	return nil
}

// Compile-time contract assertion.
var _ values.ArtifactStore = (*Store)(nil)
