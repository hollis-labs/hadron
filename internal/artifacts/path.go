package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/hollis-labs/go-workflow/values"
)

const (
	LocalAuthority = "hadron-local"
	artifactScheme = "artifact"
	payloadName    = "payload"
	manifestName   = "manifest.json"
)

var authorityPattern = regexp.MustCompile(`^[a-z][a-z0-9.-]{0,62}$`)
var componentPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._~-]*$`)

type localLocator struct {
	scope      values.ArtifactOwnerScope
	ownerHash  string
	artifactID string
}

func canonicalAuthority(authority string) bool {
	return authorityPattern.MatchString(authority)
}

func validateCanonicalRef(ref values.ArtifactRef) error {
	if err := ref.Validate(); err != nil || !canonicalAuthority(ref.Store) {
		return values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
	}
	if ref.Store != LocalAuthority {
		// The approved external delegate owns its URI grammar. The router keeps
		// the reference opaque and validates only the resolution authority.
		return nil
	}
	parsed, err := url.Parse(ref.URI)
	if err != nil || parsed.Scheme != artifactScheme || parsed.Host != ref.Store ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" ||
		parsed.RawPath != "" || parsed.Path == "" || parsed.Path[0] != '/' {
		return values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
	}
	segments := strings.Split(strings.TrimPrefix(parsed.Path, "/"), "/")
	if len(segments) == 0 {
		return values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
	}
	for _, segment := range segments {
		if !componentPattern.MatchString(segment) || segment == "." || segment == ".." {
			return values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
		}
	}
	if parsed.String() != ref.URI {
		return values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
	}
	return nil
}

func parseLocalRef(ref values.ArtifactRef) (localLocator, error) {
	if err := validateCanonicalRef(ref); err != nil || ref.Store != LocalAuthority {
		return localLocator{}, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
	}
	parsed, _ := url.Parse(ref.URI)
	segments := strings.Split(strings.TrimPrefix(parsed.Path, "/"), "/")
	if len(segments) != 3 || len(segments[1]) != sha256.Size*2 || len(segments[2]) != sha256.Size*2 {
		return localLocator{}, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
	}
	scope := values.ArtifactOwnerScope(segments[0])
	if !scope.Valid() || !lowerHex(segments[1]) || !lowerHex(segments[2]) {
		return localLocator{}, values.NewArtifactError(values.ArtifactOperationStat, values.ArtifactFailureInvalid, &ref, values.ErrArtifactInvalid)
	}
	return localLocator{scope: scope, ownerHash: segments[1], artifactID: segments[2]}, nil
}

func localURI(locator localLocator) string {
	return artifactScheme + "://" + LocalAuthority + "/" + string(locator.scope) + "/" + locator.ownerHash + "/" + locator.artifactID
}

func hashOwner(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

func lowerHex(raw string) bool {
	if strings.ToLower(raw) != raw {
		return false
	}
	_, err := hex.DecodeString(raw)
	return err == nil
}

func ensureDirectory(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		mkdirErr := os.Mkdir(path, 0o700)
		if mkdirErr != nil && !errors.Is(mkdirErr, os.ErrExist) {
			return mkdirErr
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return values.ErrArtifactInvalid
	}
	return nil
}

func rejectSymlinkComponents(path string) error {
	cleaned := filepath.Clean(path)
	volume := filepath.VolumeName(cleaned)
	remainder := strings.TrimPrefix(cleaned, volume)
	current := volume + string(os.PathSeparator)
	for _, component := range strings.Split(strings.TrimPrefix(remainder, string(os.PathSeparator)), string(os.PathSeparator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return values.ErrArtifactInvalid
		}
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(buffer)
	if contextErr := r.ctx.Err(); contextErr != nil {
		return 0, contextErr
	}
	return n, err
}

func ensureDirectoryChain(root string, components ...string) (string, error) {
	current := root
	for _, component := range components {
		if !componentPattern.MatchString(component) {
			return "", values.ErrArtifactInvalid
		}
		current = filepath.Join(current, component)
		if err := ensureDirectory(current); err != nil {
			return "", err
		}
	}
	return current, nil
}

func requireRegularFile(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, values.ErrArtifactInvalid
	}
	return info, nil
}

func openRegularFile(path string, maximumSize int64) (*os.File, os.FileInfo, error) {
	before, err := requireRegularFile(path)
	if err != nil {
		return nil, nil, err
	}
	if maximumSize >= 0 && before.Size() > maximumSize {
		return nil, nil, values.ErrArtifactSizeLimit
	}
	// #nosec G304 -- callers supply derived paths beneath an identity-checked artifact root.
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	after, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) ||
		(maximumSize >= 0 && after.Size() > maximumSize) {
		_ = file.Close()
		return nil, nil, values.ErrArtifactInvalid
	}
	return file, after, nil
}
