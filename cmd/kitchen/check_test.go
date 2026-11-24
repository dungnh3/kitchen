package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dungnh3/kitchen/cmd/kitchen/internal/scaffold"
)

// newCheckProject writes the service-events golden case into a temp dir.
func newCheckProject(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "proj")
	if err := scaffold.Write(dir, renderSvc(t, serviceGoldenCases["service-events"])); err != nil {
		t.Fatal(err)
	}
	return dir
}

func check(t *testing.T, dir string, write bool) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := runCheck(t.Context(), checkParams{Dir: dir, Write: write, Offline: true}, &out)
	return out.String(), err
}

func editManifest(t *testing.T, dir string, f func(*manifest)) {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	m, err := readManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	f(&m)
	b, err := m.marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestFile), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name    string
		change  func(t *testing.T, dir string)
		wantErr error
		wantOut string
	}{
		{"fresh project", func(*testing.T, string) {}, nil, "5 managed files checked"},
		{"edited", func(t *testing.T, dir string) {
			appendFile(t, filepath.Join(dir, ".golangci.yml"), "# local tweak\n")
		}, errEdited, ".golangci.yml: edited by hand"},
		{"deleted", func(t *testing.T, dir string) {
			if err := os.Remove(filepath.Join(dir, "Dockerfile")); err != nil {
				t.Fatal(err)
			}
		}, errEdited, "Dockerfile: deleted"},
		{"owned by the project", func(t *testing.T, dir string) {
			appendFile(t, filepath.Join(dir, ".golangci.yml"), "# local tweak\n")
			editManifest(t, dir, func(m *manifest) { delete(m.Managed, ".golangci.yml") })
		}, nil, "4 managed files checked"},
		{"older template", func(t *testing.T, dir string) {
			old := []byte("# written by an older kitchen\n")
			if err := os.WriteFile(filepath.Join(dir, ".dockerignore"), old, 0o600); err != nil {
				t.Fatal(err)
			}
			editManifest(t, dir, func(m *manifest) { m.Managed[".dockerignore"] = hashOf(old) })
		}, nil, `.dockerignore: warning: the current template differs; run "kitchen check -write"`},
		{"no manifest", func(t *testing.T, dir string) {
			if err := os.Remove(filepath.Join(dir, manifestFile)); err != nil {
				t.Fatal(err)
			}
		}, errNoManifest, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := newCheckProject(t)
			tt.change(t, dir)
			out, err := check(t, dir, false)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v\n%s", err, tt.wantErr, out)
			}
			if !strings.Contains(out, tt.wantOut) {
				t.Errorf("output %q lacks %q", out, tt.wantOut)
			}
		})
	}
}

func TestCheckWrite(t *testing.T) {
	dir := newCheckProject(t)
	// .dockerignore comes from an older template; CI was removed from the
	// manifest and the project (a managed file that is new to the project);
	// .golangci.yml was edited and must survive -write.
	old := []byte("# written by an older kitchen\n")
	if err := os.WriteFile(filepath.Join(dir, ".dockerignore"), old, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, ".github", "workflows", "ci.yml")); err != nil {
		t.Fatal(err)
	}
	editManifest(t, dir, func(m *manifest) {
		m.Managed[".dockerignore"] = hashOf(old)
		delete(m.Managed, ".github/workflows/ci.yml")
	})
	appendFile(t, filepath.Join(dir, ".golangci.yml"), "# local tweak\n")

	out, err := check(t, dir, true)
	if !errors.Is(err, errEdited) {
		t.Fatalf("err = %v, want errEdited\n%s", err, out)
	}
	for _, want := range []string{".dockerignore: updated", ".github/workflows/ci.yml: updated"} {
		if !strings.Contains(out, want) {
			t.Errorf("output %q lacks %q", out, want)
		}
	}
	lint, err := os.ReadFile(filepath.Join(dir, ".golangci.yml")) //nolint:gosec // file in the test project
	if err != nil || !bytes.HasSuffix(lint, []byte("# local tweak\n")) {
		t.Errorf("-write changed the edited .golangci.yml (err %v)", err)
	}

	// After restoring the edited file, the project is clean.
	files := renderSvc(t, serviceGoldenCases["service-events"])
	if err := os.WriteFile(filepath.Join(dir, ".golangci.yml"), files[".golangci.yml"], 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := check(t, dir, false); err != nil || strings.Contains(out, "warning") {
		t.Fatalf("after -write: err %v\n%s", err, out)
	}
}

func TestManagedHeader(t *testing.T) {
	files := renderSvc(t, serviceGoldenCases["service-postgres"])
	for _, rel := range serviceManaged {
		if !bytes.Contains(files[rel], []byte("# Managed by kitchen")) {
			t.Errorf("%s has no managed header", rel)
		}
	}
	if !bytes.HasPrefix(files["Dockerfile"], []byte("# syntax=docker/dockerfile:1\n# Managed by kitchen")) {
		t.Errorf("Dockerfile must keep the syntax directive first:\n%.120s", files["Dockerfile"])
	}
}

func TestCheckCLI(t *testing.T) {
	dir := newCheckProject(t)
	var out bytes.Buffer
	if code := run(t.Context(), []string{"check", "-dir", dir, "-offline"}, nil, &out, &out); code != 0 {
		t.Fatalf("exit %d\n%s", code, out.String())
	}
	appendFile(t, filepath.Join(dir, "Dockerfile"), "USER root\n")
	out.Reset()
	if code := run(t.Context(), []string{"check", "-dir", dir, "-offline"}, nil, &out, &out); code != 1 {
		t.Fatalf("edited: exit %d, want 1\n%s", code, out.String())
	}
}

func appendFile(t *testing.T, name, s string) {
	t.Helper()
	f, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0) //nolint:gosec // file in the test project
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
