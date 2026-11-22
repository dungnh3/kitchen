package main

import (
	"bytes"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dungnh3/kitchen/cmd/kitchen/internal/scaffold"
)

var serviceGoldenCases = map[string]serviceParams{
	"service-postgres": {
		projectParams: projectParams{Module: "github.com/acme/orders", Description: "Order management.", CI: ciStandalone, KitchenVersion: defaultKitchenVersion},
		DB:            dbPostgres,
	},
	"service-nodb": {
		projectParams: projectParams{
			Module: "example.com/team/edge-gateway", Name: "edge-gw", Description: "Edge gateway.",
			CI: ciReusable, CIWorkflow: testWorkflow, KitchenVersion: defaultKitchenVersion,
		},
		DB: dbNone,
	},
}

func renderSvc(t *testing.T, p serviceParams) scaffold.Files {
	t.Helper()
	p.applyDefaults()
	if err := p.validate(); err != nil {
		t.Fatal(err)
	}
	files, err := renderService(p)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// TestServiceGolden compares the generated trees with testdata/golden/<case>.
// Run "go test -run Golden -update" after changing templates.
func TestServiceGolden(t *testing.T) {
	for name, p := range serviceGoldenCases {
		t.Run(name, func(t *testing.T) {
			checkGolden(t, filepath.Join("testdata", "golden", name), renderSvc(t, p))
		})
	}
}

// TestTemplatesHaveNoGoFiles: a plain .go file under templates/ would be
// compiled as a package of this module (with the generated project's
// imports), so Go sources must end in .go.tmpl.
func TestTemplatesHaveNoGoFiles(t *testing.T) {
	err := fs.WalkDir(templatesFS, "templates", func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".go") {
			t.Errorf("%s: rename to %s.tmpl", p, p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestServiceTreeByDB(t *testing.T) {
	pgFiles := renderSvc(t, serviceGoldenCases["service-postgres"])
	noDB := renderSvc(t, serviceGoldenCases["service-nodb"])
	for _, f := range []string{"migrations/00001_create_items.sql", "sqlc.yaml", "internal/store/pgstore/pgstore.go", "internal/app/migrate.go"} {
		if _, ok := pgFiles[f]; !ok {
			t.Errorf("postgres: %s not generated", f)
		}
		if _, ok := noDB[f]; ok {
			t.Errorf("-db none: %s generated", f)
		}
	}
	if _, ok := pgFiles["proto/orders/v1/item_service.proto"]; !ok {
		t.Errorf("proto file not under proto/orders/v1: %v", pgFiles.Paths())
	}
	if _, ok := noDB["proto/edge_gw/v1/item_service.proto"]; !ok {
		t.Errorf("proto file not under proto/edge_gw/v1: %v", noDB.Paths())
	}
}

// TestGeneratedServiceBuilds generates each service case, runs the same
// code generation as the CLI, then go build, vet, test (the integration
// test starts the service, with a Postgres container when Docker runs),
// golangci-lint, buf lint and, with a database, sqlc diff (the shipped sqlc
// output must match the queries). The kitchen modules are replaced with the
// local checkout because they are not tagged yet. Skipped with -short or
// when modules cannot be downloaded.
func TestGeneratedServiceBuilds(t *testing.T) {
	if testing.Short() {
		t.Skip("slow: generates and builds services")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not in PATH")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "otelx", "sdk", "go.mod")); err != nil {
		t.Skipf("kitchen checkout not found next to cmd/kitchen: %v", err)
	}
	local := map[string]string{
		"github.com/dungnh3/kitchen":                  root,
		"github.com/dungnh3/kitchen/auth":             filepath.Join(root, "auth"),
		"github.com/dungnh3/kitchen/migrate":          filepath.Join(root, "migrate"),
		"github.com/dungnh3/kitchen/otelx/sdk":        filepath.Join(root, "otelx", "sdk"),
		"github.com/dungnh3/kitchen/otelx/prometheus": filepath.Join(root, "otelx", "prometheus"),
	}

	for name, p := range serviceGoldenCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p.applyDefaults()
			dir := filepath.Join(t.TempDir(), "proj")
			if err := scaffold.Write(dir, renderSvc(t, p)); err != nil {
				t.Fatal(err)
			}
			env := append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod")
			goCmd := func(args ...string) {
				t.Helper()
				cmd := exec.CommandContext(t.Context(), goBin, args...) //nolint:gosec // fixed go subcommands
				cmd.Dir, cmd.Env = dir, env
				out, err := cmd.CombinedOutput()
				if err != nil {
					skipOffline(t, out)
					t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, out)
				}
			}
			for mod, path := range local {
				goCmd("mod", "edit", "-replace", mod+"="+path)
			}
			var out bytes.Buffer
			if err := generateService(t.Context(), dir, env, &out, &out); err != nil {
				skipOffline(t, out.Bytes())
				t.Fatalf("generate: %v\n%s", err, out.String())
			}
			for _, rel := range []string{"gen/" + p.ProtoPackage() + "/v1/item_service.pb.go", "gen/" + p.ProtoPackage() + "/v1/item_service.pb.gw.go", "go.sum"} {
				if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(rel))); err != nil {
					t.Fatalf("not generated: %v", err)
				}
			}
			goCmd("build", "./...")
			goCmd("vet", "./...")
			goCmd("test", "-race", "-count=1", "./...")
			goCmd("run", "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@"+golangciLintVersion, "run", "--allow-parallel-runners", "./...")
			goCmd("run", "github.com/bufbuild/buf/cmd/buf@"+serviceVersions.Buf, "lint")
			if p.Postgres() {
				goCmd("run", "github.com/sqlc-dev/sqlc/cmd/sqlc@"+serviceVersions.Sqlc, "diff")
			}
		})
	}
}

func skipOffline(t *testing.T, out []byte) {
	t.Helper()
	if bytes.Contains(out, []byte("dial tcp")) || bytes.Contains(out, []byte("no such host")) {
		t.Skipf("modules unavailable offline: %s", out)
	}
}

func TestServiceValidate(t *testing.T) {
	ok := serviceParams{projectParams: projectParams{Module: "github.com/acme/orders", CI: ciStandalone, KitchenVersion: "v0.1.0"}, DB: dbPostgres}
	tests := []struct {
		name   string
		mutate func(*serviceParams)
		want   string
	}{
		{"ok", func(*serviceParams) {}, ""},
		{"no db", func(p *serviceParams) { p.DB = dbNone }, ""},
		{"bad db", func(p *serviceParams) { p.DB = "mysql" }, "-db"},
		{"missing module", func(p *serviceParams) { p.Module = "" }, "-module is required"},
		{"bad name", func(p *serviceParams) { p.Name = "Orders" }, "-name"},
		{"bad kitchen version", func(p *serviceParams) { p.KitchenVersion = "main" }, "-kitchen-version"},
		{"reusable with tag", func(p *serviceParams) {
			p.CI, p.CIWorkflow = ciReusable, "acme/wf/.github/workflows/go.yml@v1"
		}, "40-char commit SHA"},
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

func TestRunServiceCLI(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	newArgs := []string{"new", "service", "-module", "github.com/acme/orders", "-dir", dir, "-no-generate", "-yes"}
	tests := []struct {
		name     string
		args     []string
		wantCode int
		wantOut  string
	}{
		{"create", newArgs, 0, "make generate"},
		{"refuse non-empty dir", newArgs, 1, "not empty"},
		{"bad db", []string{"new", "service", "-module", "github.com/acme/x", "-db", "mysql", "-yes"}, 1, "-db"},
		{"bad flag", []string{"new", "service", "-nope"}, 2, ""},
		{"help flag", []string{"new", "service", "-h"}, 0, "-no-generate"},
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
	if _, err := os.Stat(filepath.Join(dir, "proto", "orders", "v1", "item_service.proto")); err != nil {
		t.Errorf("proto not generated: %v", err)
	}
}
