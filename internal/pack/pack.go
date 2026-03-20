package pack

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

// Manifest describes the contents of a .hbp (Hadron Blueprint Package) archive.
type Manifest struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	Hash         string   `json:"hash"`
	Author       string   `json:"author,omitempty"`
	Description  string   `json:"description,omitempty"`
	Dependencies []string `json:"dependencies,omitempty"`
	CreatedAt    string   `json:"created_at"`
}

// Pack bundles a blueprint with all its imports into a .hbp archive (tar.gz with manifest).
func Pack(blueprintPath string, outputPath string, resolver blueprint.ImportResolver) error {
	absBP, err := filepath.Abs(blueprintPath)
	if err != nil {
		return fmt.Errorf("resolve blueprint path: %w", err)
	}

	bp, err := blueprint.ParseFile(absBP)
	if err != nil {
		return fmt.Errorf("parse blueprint: %w", err)
	}

	// Compute content hash of the main blueprint.
	raw, err := os.ReadFile(absBP)
	if err != nil {
		return fmt.Errorf("read blueprint: %w", err)
	}
	h := sha256.Sum256(raw)
	hash := hex.EncodeToString(h[:])

	// Collect all imported file paths.
	importPaths, err := blueprint.CollectImportPaths(absBP, resolver)
	if err != nil {
		return fmt.Errorf("collect imports: %w", err)
	}

	// Build dependency names from import paths.
	var deps []string
	for _, imp := range bp.Imports {
		if strings.TrimSpace(imp.Path) == "" {
			continue
		}
		deps = append(deps, imp.Path)
	}

	name := bp.Spec.Name
	if name == "" {
		name = bp.Spec.Slug
	}

	manifest := Manifest{
		Name:         name,
		Version:      bp.Version,
		Hash:         hash,
		Author:       bp.Spec.Author,
		Description:  bp.Spec.Description,
		Dependencies: deps,
		CreatedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	// Determine the base directory for relative paths within the archive.
	baseDir := filepath.Dir(absBP)

	// Create the output archive.
	if outputPath == "" {
		outputPath = name + ".hbp"
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer outFile.Close()

	gw := gzip.NewWriter(outFile)
	defer gw.Close()

	tw := tar.NewWriter(gw)
	defer tw.Close()

	// Write manifest.json.
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := addBytesToTar(tw, "manifest.json", manifestJSON); err != nil {
		return fmt.Errorf("add manifest to archive: %w", err)
	}

	// Write the main blueprint.
	mainRelPath, err := filepath.Rel(baseDir, absBP)
	if err != nil {
		mainRelPath = filepath.Base(absBP)
	}
	if err := addFileToTar(tw, absBP, mainRelPath); err != nil {
		return fmt.Errorf("add main blueprint to archive: %w", err)
	}

	// Write all imported blueprints.
	for _, impPath := range importPaths {
		relPath, relErr := filepath.Rel(baseDir, impPath)
		if relErr != nil {
			relPath = filepath.Base(impPath)
		}
		// Avoid adding the same file twice (if main is in the imports list).
		if relPath == mainRelPath {
			continue
		}
		if err := addFileToTar(tw, impPath, relPath); err != nil {
			return fmt.Errorf("add import %q to archive: %w", relPath, err)
		}
	}

	return nil
}

// Unpack extracts a .hbp archive to a directory and returns the manifest.
func Unpack(archivePath string, outputDir string) (*Manifest, error) {
	if outputDir == "" {
		outputDir = "."
	}

	f, err := os.Open(archivePath)
	if err != nil {
		return nil, fmt.Errorf("open archive: %w", err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("open gzip reader: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)

	var manifest *Manifest

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read tar entry: %w", err)
		}

		// Sanitize path to prevent directory traversal.
		cleanName := filepath.Clean(header.Name)
		if strings.HasPrefix(cleanName, "..") || filepath.IsAbs(cleanName) {
			return nil, fmt.Errorf("invalid path in archive: %s", header.Name)
		}

		target := filepath.Join(outputDir, cleanName)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return nil, fmt.Errorf("create directory %s: %w", target, err)
			}
		case tar.TypeReg:
			// Ensure parent directory exists.
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return nil, fmt.Errorf("create parent dir for %s: %w", target, err)
			}

			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", header.Name, err)
			}

			if cleanName == "manifest.json" {
				var m Manifest
				if err := json.Unmarshal(data, &m); err != nil {
					return nil, fmt.Errorf("parse manifest: %w", err)
				}
				manifest = &m
			}

			if err := os.WriteFile(target, data, os.FileMode(header.Mode)); err != nil {
				return nil, fmt.Errorf("write %s: %w", target, err)
			}
		}
	}

	if manifest == nil {
		return nil, fmt.Errorf("archive does not contain manifest.json")
	}

	return manifest, nil
}

func addBytesToTar(tw *tar.Writer, name string, data []byte) error {
	header := &tar.Header{
		Name:    name,
		Size:    int64(len(data)),
		Mode:    0o644,
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

func addFileToTar(tw *tar.Writer, filePath, archiveName string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read %s: %w", filePath, err)
	}
	return addBytesToTar(tw, archiveName, data)
}
