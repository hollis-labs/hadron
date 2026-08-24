package pack

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestReadWorkflowSourceSelectsBoundedGraphNativeSource(t *testing.T) {
	archive := workflowArchive(t, []archiveEntry{
		{name: "README.md", body: "documentation"},
		{name: "nested/orders.workflow.yaml", body: "workflow:\n  name: orders\n"},
	})
	selected, err := ReadWorkflowSource(archive, "", WorkflowArchiveLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if selected.Entry != "nested/orders.workflow.yaml" || string(selected.Source) != "workflow:\n  name: orders\n" || selected.SourceDigest == selected.ArchiveDigest {
		t.Fatalf("selected = %+v", selected)
	}
	selected.Source[0] = 'x'
	repeated, err := ReadWorkflowSource(archive, "nested/orders.workflow.yaml", WorkflowArchiveLimits{})
	if err != nil || string(repeated.Source) != "workflow:\n  name: orders\n" {
		t.Fatalf("repeated = %+v, %v", repeated, err)
	}
}

func TestReadWorkflowSourceAcceptsMaximumSourceBound(t *testing.T) {
	archive := workflowArchive(t, []archiveEntry{{name: "workflow.yaml", body: "workflow:\n  name: maximum\n"}})
	selected, err := ReadWorkflowSource(archive, "", WorkflowArchiveLimits{
		MaxArchiveBytes: math.MaxInt64,
		MaxSourceBytes:  math.MaxInt64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if selected.Entry != "workflow.yaml" || string(selected.Source) != "workflow:\n  name: maximum\n" {
		t.Fatalf("selected = %+v", selected)
	}
}

func TestReadWorkflowSourceRejectsAmbiguityTraversalLinksDuplicatesAndBounds(t *testing.T) {
	valid := workflowArchive(t, []archiveEntry{{name: "workflow.yaml", body: "workflow:\n  name: one\n"}})
	tests := []struct {
		name     string
		entries  []archiveEntry
		archive  []byte
		selector string
		limits   WorkflowArchiveLimits
	}{
		{name: "ambiguous", entries: []archiveEntry{{name: "a.workflow.yaml", body: "a"}, {name: "b.workflow.yaml", body: "b"}}},
		{name: "traversal", entries: []archiveEntry{{name: "../workflow.yaml", body: "a"}}},
		{name: "case duplicate", entries: []archiveEntry{{name: "workflow.yaml", body: "a"}, {name: "WORKFLOW.YAML", body: "b"}}},
		{name: "symlink", entries: []archiveEntry{{name: "workflow.yaml", kind: tar.TypeSymlink, link: "elsewhere"}}},
		{name: "nonzero directory", archive: workflowArchiveWithNonzeroDirectory(t)},
		{name: "entry bound", entries: []archiveEntry{{name: "workflow.yaml", body: "a"}, {name: "README", body: "b"}}, limits: WorkflowArchiveLimits{MaxEntries: 1}},
		{name: "source bound", entries: []archiveEntry{{name: "workflow.yaml", body: "oversized"}}, limits: WorkflowArchiveLimits{MaxSourceBytes: 2}},
		{name: "bare suffix discovery", entries: []archiveEntry{{name: ".workflow.yaml", body: "workflow:\n  name: hidden\n"}}},
		{name: "bare suffix selector", entries: []archiveEntry{{name: ".workflow.yaml", body: "workflow:\n  name: hidden\n"}}, selector: ".workflow.yaml"},
		{name: "concatenated gzip member", archive: append(append([]byte(nil), valid...), valid...)},
		{name: "trailing compressed payload", archive: append(append([]byte(nil), valid...), []byte("trailing")...)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			archive := test.archive
			if archive == nil {
				archive = workflowArchive(t, test.entries)
			}
			_, err := ReadWorkflowSource(archive, test.selector, test.limits)
			if !errors.Is(err, ErrInvalidWorkflowArchive) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestCleanArchiveNameRejectsInvalidUTF8AndControlCharacters(t *testing.T) {
	for _, name := range []string{
		"line\nbreak.workflow.yaml",
		"tab\tname.workflow.yaml",
		string([]byte{0xff}) + ".workflow.yaml",
	} {
		if _, err := cleanArchiveName(name, false); !errors.Is(err, ErrInvalidWorkflowArchive) {
			t.Fatalf("cleanArchiveName(%q) error = %v", name, err)
		}
	}
}

func TestReadWorkflowSourceBoundsHiddenPAXDecompressionByMaxSourceBytes(t *testing.T) {
	archive := paxWorkflowArchive(t, strings.Repeat("highly-compressible-metadata", 1024))
	if len(archive) >= 4<<10 {
		t.Fatalf("fixture is not a compressed bomb: compressed bytes = %d", len(archive))
	}
	_, err := ReadWorkflowSource(archive, "", WorkflowArchiveLimits{MaxSourceBytes: 4 << 10})
	if !errors.Is(err, ErrInvalidWorkflowArchive) || !strings.Contains(err.Error(), "decompressed tar stream exceeds") {
		t.Fatalf("PAX decompression error = %v", err)
	}
}

type archiveEntry struct {
	name string
	body string
	kind byte
	link string
}

func workflowArchive(t *testing.T, entries []archiveEntry) []byte {
	t.Helper()
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	for _, entry := range entries {
		kind := entry.kind
		if kind == 0 {
			kind = tar.TypeReg
		}
		header := &tar.Header{Name: entry.name, Typeflag: kind, Linkname: entry.link, Mode: 0o600, Size: int64(len(entry.body))}
		if kind == tar.TypeSymlink {
			header.Size = 0
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Size != 0 {
			if _, err := tarWriter.Write([]byte(entry.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func workflowArchiveWithNonzeroDirectory(t *testing.T) []byte {
	t.Helper()
	// archive/tar correctly refuses to emit a nonzero-size directory. Build a
	// regular entry, then corrupt only its type flag and checksum to exercise
	// hostile archives produced outside the standard library.
	var raw bytes.Buffer
	tarWriter := tar.NewWriter(&raw)
	for _, entry := range []archiveEntry{
		{name: "directory", body: "hidden expansion"},
		{name: "workflow.yaml", body: "workflow:\n  name: safe\n"},
	} {
		header := &tar.Header{Name: entry.name, Typeflag: tar.TypeReg, Mode: 0o600, Size: int64(len(entry.body))}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write([]byte(entry.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	archive := raw.Bytes()
	archive[156] = tar.TypeDir
	for index := 148; index < 156; index++ {
		archive[index] = ' '
	}
	checksum := 0
	for _, value := range archive[:512] {
		checksum += int(value)
	}
	copy(archive[148:156], []byte(fmt.Sprintf("%06o\x00 ", checksum)))

	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	if _, err := gzipWriter.Write(archive); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func paxWorkflowArchive(t *testing.T, metadata string) []byte {
	t.Helper()
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	body := []byte("workflow:\n  name: pax-bounded\n")
	header := &tar.Header{
		Name: "workflow.yaml", Typeflag: tar.TypeReg, Mode: 0o600, Size: int64(len(body)),
		PAXRecords: map[string]string{"comment": metadata},
	}
	if err := tarWriter.WriteHeader(header); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
