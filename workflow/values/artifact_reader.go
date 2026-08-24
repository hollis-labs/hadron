package values

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"
)

type verifyingArtifactReader struct {
	mu       sync.Mutex
	metadata ArtifactMetadata
	reader   io.ReadCloser
	hash     hash.Hash
	closed   bool
	verified bool
	terminal error
}

// NewVerifyingArtifactReader wraps a stream so successful EOF proves the
// immutable reference digest. Close is idempotent; closing before verified EOF
// deterministically returns ErrArtifactUnverified.
func NewVerifyingArtifactReader(metadata ArtifactMetadata, reader io.ReadCloser) (ArtifactReadCloser, error) {
	if err := metadata.Validate(); err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, NewArtifactError(ArtifactOperationOpen, ArtifactFailureInvalid, &metadata.Ref, ErrArtifactInvalid)
	}
	return &verifyingArtifactReader{metadata: metadata, reader: reader, hash: sha256.New()}, nil
}

func (r *verifyingArtifactReader) Read(buffer []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		if r.terminal != nil {
			return 0, r.terminal
		}
		return 0, io.ErrClosedPipe
	}
	if r.terminal != nil {
		return 0, r.terminal
	}
	n, err := r.reader.Read(buffer)
	if n > 0 {
		_, _ = r.hash.Write(buffer[:n])
	}
	if errors.Is(err, io.EOF) {
		actual := "sha256:" + hex.EncodeToString(r.hash.Sum(nil))
		if actual != r.metadata.Ref.Digest {
			r.terminal = NewArtifactError(ArtifactOperationOpen, ArtifactFailureDigest, &r.metadata.Ref, ErrArtifactDigest)
			return n, r.terminal
		}
		r.verified = true
		return n, io.EOF
	}
	if err != nil {
		r.terminal = NewArtifactError(ArtifactOperationOpen, ArtifactFailureInvalid, &r.metadata.Ref, err)
		return n, r.terminal
	}
	return n, nil
}

func (r *verifyingArtifactReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.terminal
	}
	r.closed = true
	closeErr := r.reader.Close()
	if r.terminal != nil {
		return r.terminal
	}
	if !r.verified {
		r.terminal = NewArtifactError(ArtifactOperationOpen, ArtifactFailureDigest, &r.metadata.Ref, ErrArtifactUnverified)
		return r.terminal
	}
	if closeErr != nil {
		r.terminal = NewArtifactError(ArtifactOperationOpen, ArtifactFailureInvalid, &r.metadata.Ref, closeErr)
		return r.terminal
	}
	return nil
}

func (r *verifyingArtifactReader) Metadata() ArtifactMetadata { return r.metadata }

func (r *verifyingArtifactReader) Verified() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.verified
}
