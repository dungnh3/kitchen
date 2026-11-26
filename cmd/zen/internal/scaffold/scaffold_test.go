package scaffold

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

// The golden files live in package main; accepting -update here lets
// "go test ./... -update" (cmd/zen/CLAUDE.md) run over every package.
var _ = flag.Bool("update", false, "rewrite golden files (used by package main)")

func TestRender(t *testing.T) {
	fsys := fstest.MapFS{
		"t/main.go.tmpl":        {Data: []byte("package main\nfunc main(){println(\"{% .Name %}\")}\n")},
		"t/.github/ci.yml.tmpl": {Data: []byte("run: ${{ github.sha }} {% .Name %}\n")},
		"t/static.txt":          {Data: []byte("{% not a template %}")},
	}
	got, err := Render(fsys, "t", map[string]string{"Name": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if want := "package main\n\nfunc main() { println(\"x\") }\n"; string(got["main.go"]) != want {
		t.Errorf("main.go = %q, want gofmt'd %q", got["main.go"], want)
	}
	if string(got[".github/ci.yml"]) != "run: ${{ github.sha }} x\n" {
		t.Errorf("ci.yml = %q", got[".github/ci.yml"])
	}
	if string(got["static.txt"]) != "{% not a template %}" {
		t.Errorf("static.txt = %q", got["static.txt"])
	}
}

func TestRenderErrors(t *testing.T) {
	tests := []struct {
		name string
		fs   fstest.MapFS
		want string
	}{
		{"missing key", fstest.MapFS{"t/a.txt.tmpl": {Data: []byte("{% .Missing %}")}}, "render a.txt.tmpl"},
		{"bad go", fstest.MapFS{"t/a.go.tmpl": {Data: []byte("package {% .Name %} func(")}}, "gofmt a.go"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Render(tt.fs, "t", map[string]string{"Name": "x"})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestWrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "proj")
	files := Files{"a/b/c.txt": []byte("c"), "d.txt": []byte("d")}
	if err := Write(dir, files); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "a", "b", "c.txt")) //nolint:gosec // test temp dir
	if err != nil || string(b) != "c" {
		t.Fatalf("c.txt = %q, %v", b, err)
	}
	if err := Write(dir, files); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("second Write = %v, want ErrNotEmpty", err)
	}
}

func TestWriteRejectsEscape(t *testing.T) {
	dir := t.TempDir()
	if err := Write(filepath.Join(dir, "p"), Files{"../escape.txt": []byte("x")}); err == nil {
		t.Fatal("Write allowed a path outside the target directory")
	}
	if _, err := os.Stat(filepath.Join(dir, "escape.txt")); !os.IsNotExist(err) {
		t.Fatalf("escape.txt was written: %v", err)
	}
}
