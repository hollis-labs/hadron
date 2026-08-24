package conformance

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
)

//go:embed testdata/fixtures/*/*.json
var embeddedFixtureFiles embed.FS

type embeddedFixtureStore struct{}

// EmbeddedFixtures returns the package's stable, extraction-ready fixture
// store.
func EmbeddedFixtures() FixtureStore {
	return embeddedFixtureStore{}
}

func (embeddedFixtureStore) Fixtures(set FixtureSet) ([]Fixture, error) {
	dir := path.Join("testdata/fixtures", string(set))
	entries, err := fs.ReadDir(embeddedFixtureFiles, dir)
	if err != nil {
		return nil, fmt.Errorf("read conformance fixture set %s: %w", set, err)
	}

	fixtures := make([]Fixture, 0, len(entries))
	names := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || path.Ext(entry.Name()) != ".json" {
			continue
		}

		fixturePath := path.Join(dir, entry.Name())
		data, err := embeddedFixtureFiles.ReadFile(fixturePath)
		if err != nil {
			return nil, fmt.Errorf("read conformance fixture %s: %w", fixturePath, err)
		}
		fixture, err := decodeFixture(fixturePath, data)
		if err != nil {
			return nil, err
		}
		fixture.Set = set
		if _, duplicate := names[fixture.Name]; duplicate {
			return nil, fmt.Errorf("decode conformance fixture %s: duplicate name %q", fixturePath, fixture.Name)
		}
		names[fixture.Name] = struct{}{}
		fixtures = append(fixtures, fixture)
	}

	sort.Slice(fixtures, func(i, j int) bool {
		return fixtures[i].Path < fixtures[j].Path
	})
	return fixtures, nil
}

func decodeFixture(fixturePath string, data []byte) (Fixture, error) {
	var document struct {
		Name        string          `json:"name"`
		Expectation Expectation     `json:"expect"`
		Input       json.RawMessage `json:"input"`
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return Fixture{}, fmt.Errorf("decode conformance fixture %s: %w", fixturePath, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Fixture{}, fmt.Errorf("decode conformance fixture %s: trailing JSON content", fixturePath)
	}
	if document.Name == "" {
		return Fixture{}, fmt.Errorf("decode conformance fixture %s: name is required", fixturePath)
	}
	if document.Expectation != ExpectPass && document.Expectation != ExpectFail {
		return Fixture{}, fmt.Errorf("decode conformance fixture %s: expect must be %q or %q", fixturePath, ExpectPass, ExpectFail)
	}
	if len(document.Input) == 0 {
		return Fixture{}, fmt.Errorf("decode conformance fixture %s: input is required", fixturePath)
	}

	return Fixture{
		Name:        document.Name,
		Path:        fixturePath,
		Expectation: document.Expectation,
		Input:       document.Input,
	}, nil
}
