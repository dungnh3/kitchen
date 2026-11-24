package main

import (
	"bytes"
	"errors"
	"flag"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/cmd/kitchen/internal/scaffold"
)

var update = flag.Bool("update", false, "rewrite golden files")

func TestMain(m *testing.M) {
	// Golden files must not depend on how the test binary was built.
	cliVersion = func() string { return "v0.1.0" }
	goleak.VerifyTestMain(m)
}

const testWorkflow = "acme/devops-workflows/.github/workflows/go.yml@0123456789abcdef0123456789abcdef01234567"

var goldenCases = map[string]mcpParams{
	"standalone": {
		projectParams: projectParams{Module: "github.com/acme/mcp-notes", Description: "Notes tools.", CI: ciStandalone, KitchenVersion: defaultKitchenVersion},
		MCPKitVersion: defaultMCPKitVersion,
	},
	"reusable": {
		projectParams: projectParams{
			Module: "example.com/team/mcp-tickets", Name: "tickets", Description: "Ticket tools.",
			CI: ciReusable, CIWorkflow: testWorkflow, KitchenVersion: defaultKitchenVersion,
		},
		MCPKitVersion: defaultMCPKitVersion,
	},
}

func render(t *testing.T, p mcpParams) scaffold.Files {
	t.Helper()
	p.applyDefaults()
	if err := p.validate(); err != nil {
		t.Fatal(err)
	}
	files, err := renderMCP(p)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// TestGolden compares the generated tree with testdata/golden/<case>.
// Run "go test -run TestGolden -update" after changing templates.
func TestGolden(t *testing.T) {
	for name, p := range goldenCases {
		t.Run(name, func(t *testing.T) {
			checkGolden(t, filepath.Join("testdata", "golden", name), render(t, p))
		})
	}
}

// checkGolden compares files with the tree in dir, or rewrites dir with -update.
func checkGolden(t *testing.T, dir string, files scaffold.Files) {
	t.Helper()
	if *update {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		for _, rel := range files.Paths() {
			fp := filepath.Join(dir, filepath.FromSlash(rel)+".golden")
			if err := os.MkdirAll(filepath.Dir(fp), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(fp, files[rel], 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	want := map[string]bool{}
	err := filepath.WalkDir(dir, func(fp string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, fp)
		rel = strings.TrimSuffix(filepath.ToSlash(rel), ".golden")
		want[rel] = true
		golden, err := os.ReadFile(fp) //nolint:gosec // testdata path
		if err != nil {
			return err
		}
		got, ok := files[rel]
		switch {
		case !ok:
			t.Errorf("%s: not generated", rel)
		case !bytes.Equal(got, golden):
			t.Errorf("%s differs from golden file (run with -update to accept):\n%s", rel, got)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, rel := range files.Paths() {
		if !want[rel] {
			t.Errorf("%s: generated but has no golden file", rel)
		}
	}
}

// TestGeneratedProjectBuilds generates a project and runs go build, vet and
// test on it, using a go.work that replaces mcpkit and the kitchen modules
// with the local checkout (they are not tagged yet). Skipped with -short or when modules cannot be downloaded.
func TestGeneratedProjectBuilds(t *testing.T) {
	if testing.Short() {
		t.Skip("slow: builds a generated project")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not in PATH")
	}
	mcpkitDir, err := filepath.Abs(filepath.Join("..", "..", "mcpkit"))
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(mcpkitDir)
	// The generated project needs the sums of mcpkit's and otelx/sdk's
	// dependency graphs.
	var sum []byte
	for _, m := range []string{"mcpkit", filepath.Join("otelx", "sdk")} {
		b, err := os.ReadFile(filepath.Join(root, m, "go.sum")) //nolint:gosec // sibling module
		if err != nil {
			t.Skipf("module %s not found next to cmd/kitchen: %v", m, err)
		}
		sum = append(sum, b...)
	}

	for name, p := range goldenCases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "proj")
			if err := scaffold.Write(dir, render(t, p)); err != nil {
				t.Fatal(err)
			}
			// The go.sum files of mcpkit and otelx/sdk together cover the
			// modules the generated project needs.
			if err := os.WriteFile(filepath.Join(dir, "go.sum"), sum, 0o600); err != nil { //nolint:gosec // test temp dir
				t.Fatal(err)
			}
			// The kitchen modules are not tagged yet.
			work := "go " + goDirective + "\n\nuse .\n\n" +
				"replace github.com/dungnh3/kitchen/mcpkit => " + mcpkitDir + "\n" +
				"replace github.com/dungnh3/kitchen => " + root + "\n" +
				"replace github.com/dungnh3/kitchen/auth => " + filepath.Join(root, "auth") + "\n" +
				"replace github.com/dungnh3/kitchen/otelx/sdk => " + filepath.Join(root, "otelx", "sdk") + "\n"
			if err := os.WriteFile(filepath.Join(dir, "go.work"), []byte(work), 0o600); err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{
				{"build", "./..."},
				{"vet", "./..."},
				{"test", "-count=1", "./..."},
			} {
				cmd := exec.CommandContext(t.Context(), goBin, args...) //nolint:gosec // fixed go subcommands
				cmd.Dir = dir
				cmd.Env = append(cmd.Environ(), "GOWORK="+filepath.Join(dir, "go.work"), "GOFLAGS=-mod=readonly")
				out, err := cmd.CombinedOutput()
				if err != nil {
					if bytes.Contains(out, []byte("dial tcp")) || bytes.Contains(out, []byte("no such host")) {
						t.Skipf("modules unavailable offline: %s", out)
					}
					t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, out)
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	ok := mcpParams{projectParams: projectParams{Module: "github.com/acme/mcp-x", CI: ciStandalone, KitchenVersion: "v0.1.0"}, MCPKitVersion: "v0.1.0"}
	tests := []struct {
		name   string
		mutate func(*mcpParams)
		want   string
	}{
		{"ok", func(*mcpParams) {}, ""},
		{"missing module", func(p *mcpParams) { p.Module = "" }, "-module is required"},
		{"bad module", func(p *mcpParams) { p.Module = "Github.com/ACME/x y" }, "-module"},
		{"bad name", func(p *mcpParams) { p.Name = "Bad_Name" }, "-name"},
		{"multi-line description", func(p *mcpParams) { p.Description = "a\nb" }, "one line"},
		{"bad ci", func(p *mcpParams) { p.CI = "jenkins" }, "-ci"},
		{"workflow without reusable", func(p *mcpParams) { p.CIWorkflow = testWorkflow }, "requires -ci=reusable"},
		{"reusable with tag", func(p *mcpParams) {
			p.CI, p.CIWorkflow = ciReusable, "acme/wf/.github/workflows/go.yml@master"
		}, "40-char commit SHA"},
		{"reusable ok", func(p *mcpParams) { p.CI, p.CIWorkflow = ciReusable, testWorkflow }, ""},
		{"bad version", func(p *mcpParams) { p.MCPKitVersion = "latest" }, "-mcpkit-version"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := ok
			tt.mutate(&p)
			p.applyDefaults()
			err := p.validate()
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestRunCLI(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	newArgs := []string{"new", "mcp", "-module", "github.com/acme/mcp-x", "-dir", dir, "-yes"}
	tests := []struct {
		name     string
		args     []string
		wantCode int
		wantOut  string
	}{
		{"no args", nil, 2, ""},
		{"create", newArgs, 0, "Next steps"},
		{"refuse non-empty dir", newArgs, 1, "not empty"},
		{"unknown template", []string{"new", "lambda"}, 1, "unknown template"},
		{"bad flag", []string{"new", "mcp", "-nope"}, 2, ""},
		{"help flag", []string{"new", "mcp", "-h"}, 0, "-module"},
		{"version", []string{"version"}, 0, ""},
	}
	for _, tt := range tests { // sequential: "refuse" depends on "create"
		var out bytes.Buffer
		code := run(t.Context(), tt.args, nil, &out, &out)
		if code != tt.wantCode {
			t.Errorf("%s: exit %d, want %d\n%s", tt.name, code, tt.wantCode, out.String())
		}
		if !strings.Contains(out.String(), tt.wantOut) {
			t.Errorf("%s: output %q lacks %q", tt.name, out.String(), tt.wantOut)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		t.Errorf("go.mod not generated: %v", err)
	}
}

func TestParseHelp(t *testing.T) {
	var out bytes.Buffer
	if _, err := parseMCPFlags([]string{"-h"}, &out); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v", err)
	}
}
