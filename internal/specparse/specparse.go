package specparse

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type format int

const (
	formatYAML format = iota
	formatJSONC
)

func Unmarshal(path string, data []byte, out any) error {
	switch detectFormat(path, data) {
	case formatJSONC:
		normalized := normalizeJSONC(data)
		if err := json.Unmarshal(normalized, out); err != nil {
			return fmt.Errorf("parse json/jsonc: %w", err)
		}
	default:
		if err := yaml.Unmarshal(data, out); err != nil {
			return fmt.Errorf("parse yaml: %w", err)
		}
	}
	return nil
}

func detectFormat(path string, data []byte) format {
	ext := strings.ToLower(filepath.Ext(strings.TrimSpace(path)))
	if ext == ".json" || ext == ".jsonc" {
		return formatJSONC
	}
	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") ||
		strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "/*") {
		return formatJSONC
	}
	return formatYAML
}

func normalizeJSONC(data []byte) []byte {
	withoutComments := stripComments(data)
	return stripTrailingCommas(withoutComments)
}

func stripComments(data []byte) []byte {
	out := make([]byte, 0, len(data))
	inString := false
	escaped := false
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(data); i++ {
		ch := data[i]

		if inLineComment {
			if ch == '\n' {
				inLineComment = false
				out = append(out, ch)
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && i+1 < len(data) && data[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}

		if inString {
			out = append(out, ch)
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}

		if ch == '"' {
			inString = true
			out = append(out, ch)
			continue
		}
		if ch == '/' && i+1 < len(data) && data[i+1] == '/' {
			inLineComment = true
			i++
			continue
		}
		if ch == '/' && i+1 < len(data) && data[i+1] == '*' {
			inBlockComment = true
			i++
			continue
		}

		out = append(out, ch)
	}

	return out
}

func stripTrailingCommas(data []byte) []byte {
	out := make([]byte, 0, len(data))
	inString := false
	escaped := false

	for i := 0; i < len(data); i++ {
		ch := data[i]

		if inString {
			out = append(out, ch)
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}

		if ch == '"' {
			inString = true
			out = append(out, ch)
			continue
		}

		if ch == ',' {
			j := i + 1
			for j < len(data) {
				if data[j] == ' ' || data[j] == '\n' || data[j] == '\r' || data[j] == '\t' {
					j++
					continue
				}
				break
			}
			if j < len(data) && (data[j] == '}' || data[j] == ']') {
				continue
			}
		}

		out = append(out, ch)
	}

	return out
}
