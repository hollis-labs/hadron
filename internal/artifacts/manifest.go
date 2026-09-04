package artifacts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/hollis-labs/go-workflow/values"
)

const manifestVersion = 1

type storedManifest struct {
	Version  int                     `json:"version"`
	Metadata values.ArtifactMetadata `json:"metadata"`
}

type artifactIdentity struct {
	Store      string                    `json:"store"`
	OwnerScope values.ArtifactOwnerScope `json:"owner_scope"`
	OwnerHash  string                    `json:"owner_hash"`
	Digest     string                    `json:"digest"`
	MediaType  string                    `json:"media_type"`
	SizeBytes  int64                     `json:"size_bytes"`
	Producer   values.Producer           `json:"producer"`
	Redaction  values.RedactionClass     `json:"redaction"`
	Retention  values.RetentionClass     `json:"retention"`
	CreatedAt  time.Time                 `json:"created_at"`
	ExpiresAt  time.Time                 `json:"expires_at,omitempty"`
}

func artifactID(identity artifactIdentity) (string, error) {
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func encodeManifest(metadata values.ArtifactMetadata) ([]byte, error) {
	if err := metadata.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(storedManifest{Version: manifestVersion, Metadata: metadata})
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func decodeManifest(content []byte) (storedManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var manifest storedManifest
	if err := decoder.Decode(&manifest); err != nil {
		return storedManifest{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return storedManifest{}, values.ErrArtifactInvalid
		}
		return storedManifest{}, err
	}
	if manifest.Version != manifestVersion {
		return storedManifest{}, values.ErrArtifactInvalid
	}
	if err := manifest.Metadata.Validate(); err != nil {
		return storedManifest{}, err
	}
	return manifest, nil
}

func readManifest(directory string) (storedManifest, error) {
	manifestPath := filepath.Join(directory, manifestName)
	file, info, err := openRegularFile(manifestPath, 1<<20)
	if err != nil {
		return storedManifest{}, err
	}
	content, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	closeErr := file.Close()
	if err != nil {
		return storedManifest{}, err
	}
	if closeErr != nil {
		return storedManifest{}, closeErr
	}
	if int64(len(content)) != info.Size() {
		return storedManifest{}, values.ErrArtifactInvalid
	}
	return decodeManifest(content)
}

func verifyStored(directory string, locator localLocator, expected *values.ArtifactRef) (values.ArtifactMetadata, error) {
	directoryInfo, err := os.Lstat(directory)
	if err != nil {
		return values.ArtifactMetadata{}, err
	}
	if !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 {
		return values.ArtifactMetadata{}, values.ErrArtifactInvalid
	}
	manifest, err := readManifest(directory)
	if err != nil {
		return values.ArtifactMetadata{}, err
	}
	metadata := manifest.Metadata
	if metadata.Ref.Store != LocalAuthority || metadata.Ref.URI != localURI(locator) ||
		metadata.Owner.Scope != locator.scope || hashOwner(metadata.Owner.ID) != locator.ownerHash {
		return values.ArtifactMetadata{}, values.ErrArtifactInvalid
	}
	identity := artifactIdentity{
		Store: LocalAuthority, OwnerScope: metadata.Owner.Scope, OwnerHash: locator.ownerHash,
		Digest: metadata.Ref.Digest, MediaType: metadata.Ref.MediaType, SizeBytes: metadata.Ref.SizeBytes,
		Producer: metadata.Ref.Producer, Redaction: metadata.Ref.Redaction, Retention: metadata.Ref.Retention,
		CreatedAt: metadata.CreatedAt, ExpiresAt: metadata.ExpiresAt,
	}
	computedID, err := artifactID(identity)
	if err != nil || computedID != locator.artifactID {
		return values.ArtifactMetadata{}, values.ErrArtifactInvalid
	}
	if expected != nil && !reflect.DeepEqual(metadata.Ref, *expected) {
		return values.ArtifactMetadata{}, values.ErrArtifactInvalid
	}
	payloadInfo, err := requireRegularFile(filepath.Join(directory, payloadName))
	if err != nil {
		return values.ArtifactMetadata{}, err
	}
	if payloadInfo.Size() != metadata.Ref.SizeBytes {
		return values.ArtifactMetadata{}, values.ErrArtifactDigest
	}
	return metadata, nil
}
