package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// jsonArg reads a JSON value from a file, or from stdin when path is "-".
func jsonArg(path string, in io.Reader) (json.RawMessage, error) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(in)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("%s does not hold valid JSON", path)
	}
	return data, nil
}
