package specparse

import "testing"

func TestUnmarshal_JSONC(t *testing.T) {
	var out struct {
		Name   string   `json:"name"`
		Values []string `json:"values"`
	}
	src := []byte(`
{
  // top-level comment
  "name": "demo",
  "values": [
    "a",
    "b", // trailing
  ],
}
`)
	if err := Unmarshal("test.jsonc", src, &out); err != nil {
		t.Fatalf("unmarshal jsonc: %v", err)
	}
	if out.Name != "demo" || len(out.Values) != 2 {
		t.Fatalf("unexpected jsonc result: %+v", out)
	}
}

func TestUnmarshal_YAML(t *testing.T) {
	var out struct {
		Name string `yaml:"name"`
	}
	src := []byte("name: demo")
	if err := Unmarshal("test.yaml", src, &out); err != nil {
		t.Fatalf("unmarshal yaml: %v", err)
	}
	if out.Name != "demo" {
		t.Fatalf("unexpected yaml result: %+v", out)
	}
}
