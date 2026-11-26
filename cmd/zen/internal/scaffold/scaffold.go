// Package scaffold renders a template tree into a set of files and writes
// them into a new project directory.
//
// Files whose name ends in ".tmpl" are executed with text/template (using
// "{%" and "%}" delimiters so GitHub Actions "${{ }}" expressions and Go
// code need no escaping) and lose the suffix; other files are copied as is.
// Generated .go files are gofmt'd; a formatting error is reported with the
// file name, which catches template bugs at generation time.
package scaffold

import (
	"bytes"
	"errors"
	"fmt"
	"go/format"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"text/template"
)

// Files maps slash-separated relative paths to file contents.
type Files map[string][]byte

// Paths returns the file paths in sorted order.
func (f Files) Paths() []string { return slices.Sorted(maps.Keys(f)) }

// ErrNotEmpty is returned by [Write] when the target directory already
// contains files.
var ErrNotEmpty = errors.New("scaffold: target directory is not empty")

const tmplSuffix = ".tmpl"

// Render executes every file under root in fsys with data.
func Render(fsys fs.FS, root string, data any) (Files, error) {
	out := Files{}
	err := fs.WalkDir(fsys, root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel := strings.TrimPrefix(p, root+"/")
		src, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		if name, ok := strings.CutSuffix(rel, tmplSuffix); ok {
			t, err := template.New(rel).Delims("{%", "%}").Option("missingkey=error").Parse(string(src))
			if err != nil {
				return fmt.Errorf("scaffold: parse %s: %w", rel, err)
			}
			var buf bytes.Buffer
			if err := t.Execute(&buf, data); err != nil {
				return fmt.Errorf("scaffold: render %s: %w", rel, err)
			}
			rel, src = name, buf.Bytes()
		}
		if path.Ext(rel) == ".go" {
			formatted, err := format.Source(src)
			if err != nil {
				return fmt.Errorf("scaffold: gofmt %s: %w", rel, err)
			}
			src = formatted
		}
		out[rel] = src
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Write creates dir (and parents) and writes files into it. It refuses to
// write into a directory that already has entries, so it never overwrites
// existing work. All writes go through an [os.Root], so a file path cannot
// escape dir.
func Write(dir string, files Files) error {
	// Generated sources are ordinary project files: world-readable like git checkouts.
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // see above
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("%w: %s", ErrNotEmpty, dir)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	for _, p := range files.Paths() {
		name := filepath.FromSlash(p)
		if d := filepath.Dir(name); d != "." {
			if err := root.MkdirAll(d, 0o755); err != nil { //nolint:gosec // project directory
				return err
			}
		}
		if err := root.WriteFile(name, files[p], 0o644); err != nil { //nolint:gosec // project source file
			return err
		}
	}
	return nil
}
