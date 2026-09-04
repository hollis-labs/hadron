package pack

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/go-workflow/values"
)

var ErrInvalidWorkflowArchive = errors.New("invalid graph-native workflow package")

type WorkflowArchiveLimits struct {
	MaxArchiveBytes int64
	MaxEntries      int
	MaxTotalBytes   int64
	// MaxSourceBytes bounds both the selected source and the complete
	// decompressed tar stream, including hidden metadata, headers, padding,
	// and end blocks. This prevents PAX/GNU metadata from bypassing entry-body
	// accounting inside archive/tar.
	MaxSourceBytes int64
}

type WorkflowArchiveSource struct {
	Entry         string
	Source        []byte
	SourceDigest  string
	ArchiveDigest string
}

// ReadWorkflowSource selects one graph-native workflow from a bounded tar.gz
// package without extracting any archive entry to disk.
func ReadWorkflowSource(data []byte, selector string, limits WorkflowArchiveLimits) (WorkflowArchiveSource, error) {
	limits = normalizeWorkflowArchiveLimits(limits)
	if int64(len(data)) > limits.MaxArchiveBytes {
		return WorkflowArchiveSource{}, fmt.Errorf("%w: compressed archive exceeds %d bytes", ErrInvalidWorkflowArchive, limits.MaxArchiveBytes)
	}
	selector, err := cleanArchiveName(selector, true)
	if err != nil {
		return WorkflowArchiveSource{}, err
	}
	compressed := bytes.NewReader(data)
	reader, err := gzip.NewReader(compressed)
	if err != nil {
		return WorkflowArchiveSource{}, fmt.Errorf("%w: open gzip stream: %w", ErrInvalidWorkflowArchive, err)
	}
	reader.Multistream(false)
	defer func() { _ = reader.Close() }()

	streamBudget := limits.MaxSourceBytes
	limitedBudget := saturatingOverflowProbe(streamBudget)
	decompressed := &io.LimitedReader{R: reader, N: limitedBudget}
	tarReader := tar.NewReader(decompressed)
	seen := make(map[string]struct{})
	candidates := make(map[string][]byte)
	var total int64
	entries := 0
	for {
		header, nextErr := tarReader.Next()
		if decompressedTarExceeded(decompressed, limitedBudget, streamBudget) {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: decompressed tar stream exceeds %d bytes", ErrInvalidWorkflowArchive, streamBudget)
		}
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: read entry: %w", ErrInvalidWorkflowArchive, nextErr)
		}
		entries++
		if entries > limits.MaxEntries {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: archive exceeds %d entries", ErrInvalidWorkflowArchive, limits.MaxEntries)
		}
		name, cleanErr := cleanArchiveName(header.Name, false)
		if cleanErr != nil {
			return WorkflowArchiveSource{}, cleanErr
		}
		collisionKey := strings.ToLower(name)
		if _, duplicate := seen[collisionKey]; duplicate {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: duplicate entry %q", ErrInvalidWorkflowArchive, name)
		}
		seen[collisionKey] = struct{}{}
		switch header.Typeflag {
		case tar.TypeDir:
			if header.Size != 0 {
				return WorkflowArchiveSource{}, fmt.Errorf("%w: directory entry %q has nonzero size", ErrInvalidWorkflowArchive, name)
			}
			continue
		case tar.TypeReg:
		default:
			return WorkflowArchiveSource{}, fmt.Errorf("%w: entry %q uses unsupported type", ErrInvalidWorkflowArchive, name)
		}
		if header.Size < 0 || header.Size > limits.MaxTotalBytes-total {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: uncompressed content exceeds %d bytes", ErrInvalidWorkflowArchive, limits.MaxTotalBytes)
		}
		total += header.Size
		isCandidate := supportedWorkflowName(path.Base(name))
		if !isCandidate {
			copied, copyErr := io.CopyN(io.Discard, tarReader, header.Size)
			if copyErr != nil || copied != header.Size {
				return WorkflowArchiveSource{}, fmt.Errorf("%w: discard entry %q: %w", ErrInvalidWorkflowArchive, name, copyErr)
			}
			continue
		}
		if header.Size > limits.MaxSourceBytes {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: workflow source %q exceeds %d bytes", ErrInvalidWorkflowArchive, name, limits.MaxSourceBytes)
		}
		contents, readErr := io.ReadAll(io.LimitReader(tarReader, saturatingOverflowProbe(limits.MaxSourceBytes)))
		if readErr != nil {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: read entry %q: %w", ErrInvalidWorkflowArchive, name, readErr)
		}
		if int64(len(contents)) != header.Size {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: entry %q size changed while reading", ErrInvalidWorkflowArchive, name)
		}
		candidates[name] = contents
	}
	var trailing [1]byte
	count, finishErr := decompressed.Read(trailing[:])
	if count != 0 {
		if decompressedTarExceeded(decompressed, limitedBudget, streamBudget) {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: decompressed tar stream exceeds %d bytes", ErrInvalidWorkflowArchive, streamBudget)
		}
		return WorkflowArchiveSource{}, fmt.Errorf("%w: archive has uncompressed data after its tar end marker", ErrInvalidWorkflowArchive)
	}
	if !errors.Is(finishErr, io.EOF) {
		return WorkflowArchiveSource{}, fmt.Errorf("%w: finish gzip stream: %w", ErrInvalidWorkflowArchive, finishErr)
	}
	if err := reader.Close(); err != nil {
		return WorkflowArchiveSource{}, fmt.Errorf("%w: close gzip stream: %w", ErrInvalidWorkflowArchive, err)
	}
	if compressed.Len() != 0 {
		return WorkflowArchiveSource{}, fmt.Errorf("%w: compressed archive has trailing or concatenated data", ErrInvalidWorkflowArchive)
	}

	entry := selector
	if entry == "" {
		if _, exists := candidates["workflow.yaml"]; exists {
			entry = "workflow.yaml"
		} else if len(candidates) == 1 {
			for candidate := range candidates {
				entry = candidate
			}
		} else if len(candidates) == 0 {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: archive contains no graph-native workflow source", ErrInvalidWorkflowArchive)
		} else {
			return WorkflowArchiveSource{}, fmt.Errorf("%w: archive contains multiple workflow sources; select one explicitly", ErrInvalidWorkflowArchive)
		}
	}
	contents, exists := candidates[entry]
	if !exists {
		return WorkflowArchiveSource{}, fmt.Errorf("%w: selected workflow source %q is absent", ErrInvalidWorkflowArchive, entry)
	}
	return WorkflowArchiveSource{
		Entry: entry, Source: bytes.Clone(contents),
		SourceDigest: values.SHA256Digest(contents), ArchiveDigest: values.SHA256Digest(data),
	}, nil
}

func saturatingOverflowProbe(limit int64) int64 {
	if limit == math.MaxInt64 {
		return math.MaxInt64
	}
	return limit + 1
}

func decompressedTarExceeded(reader *io.LimitedReader, initial, maximum int64) bool {
	if reader == nil || initial < reader.N {
		return true
	}
	return initial-reader.N > maximum
}

func normalizeWorkflowArchiveLimits(input WorkflowArchiveLimits) WorkflowArchiveLimits {
	if input.MaxArchiveBytes <= 0 {
		input.MaxArchiveBytes = 16 << 20
	}
	if input.MaxEntries <= 0 {
		input.MaxEntries = 256
	}
	if input.MaxTotalBytes <= 0 {
		input.MaxTotalBytes = 32 << 20
	}
	if input.MaxSourceBytes <= 0 {
		input.MaxSourceBytes = 4 << 20
	}
	return input
}

func cleanArchiveName(input string, optional bool) (string, error) {
	if input == "" && optional {
		return "", nil
	}
	if input == "" || !utf8.ValidString(input) || strings.Contains(input, "\\") ||
		strings.IndexFunc(input, unicode.IsControl) >= 0 || strings.HasPrefix(input, "/") {
		return "", fmt.Errorf("%w: unsafe archive path %q", ErrInvalidWorkflowArchive, input)
	}
	cleaned := path.Clean(input)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || cleaned != input {
		return "", fmt.Errorf("%w: unsafe archive path %q", ErrInvalidWorkflowArchive, input)
	}
	if optional && !supportedWorkflowName(path.Base(cleaned)) {
		return "", fmt.Errorf("%w: selected entry must be workflow.yaml or *.workflow.yaml", ErrInvalidWorkflowArchive)
	}
	return cleaned, nil
}

func supportedWorkflowName(name string) bool {
	return name == "workflow.yaml" || (len(name) > len(".workflow.yaml") && strings.HasSuffix(name, ".workflow.yaml"))
}
