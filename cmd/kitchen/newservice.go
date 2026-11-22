package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	"charm.land/huh/v2"

	"github.com/dungnh3/kitchen/cmd/kitchen/internal/scaffold"
)

// serviceVersions are the dependency and tool versions written into
// generated services. Bump them together with the golden files
// (go test ./... -update) and keep the library versions in line with the
// kitchen modules, so "go mod tidy" does not have to move them.
var serviceVersions = struct {
	Buf, Sqlc                                string
	Protobuf, GRPC, ProtocGenGoGRPC, Gateway string
	GenprotoAPI, ProtovalidateGen            string
	Pgx, Env, UUID, Prometheus, XText        string
	Testcontainers, Goleak                   string
}{
	Buf:              "v1.75.0",
	Sqlc:             "v1.31.1",
	Protobuf:         "v1.36.12",
	GRPC:             "v1.85.0",
	ProtocGenGoGRPC:  "v1.6.2",
	Gateway:          "v2.34.0",
	GenprotoAPI:      "v0.0.0-20261110000943-09e21b237f2c",
	ProtovalidateGen: "v1.36.12-20260825204119-511051f7f437.2",
	Pgx:              "v5.13.0",
	Env:              "v11.4.2",
	UUID:             "v1.6.0",
	Prometheus:       "v1.25.0",
	XText:            "v0.44.0",
	Testcontainers:   "v0.45.0",
	Goleak:           "v1.3.0",
}

// postgresImage is the Postgres image of the local database and the
// integration test.
const postgresImage = "postgres:18-alpine"

// Databases.
const (
	dbPostgres = "postgres"
	dbNone     = "none"
)

// protoPkgDir is the placeholder directory in templates/service/proto that
// becomes the proto package name.
const protoPkgDir = "_pkg_"

type serviceParams struct {
	projectParams
	DB         string
	NoGenerate bool
}

// Postgres reports whether the service uses PostgreSQL.
func (p serviceParams) Postgres() bool { return p.DB == dbPostgres }

// ProtoPackage is the proto package (without ".v1") and the directory under
// proto/ and gen/: the name with dashes turned into underscores.
func (p serviceParams) ProtoPackage() string { return strings.ReplaceAll(p.Name, "-", "_") }

type serviceTemplateData struct {
	serviceParams
	Versions            any
	GoDirective         string
	GoImage             string
	RuntimeImage        string
	PostgresImage       string
	GolangciLintVersion string
	Actions             actionPins
}

func newService(ctx context.Context, args []string, stdin *os.File, stdout, stderr io.Writer) error {
	p, err := parseServiceFlags(args, stderr)
	if err != nil {
		return err
	}
	if p.Module == "" && !p.Yes && isTerminal(stdin) {
		if err := promptService(ctx, &p); err != nil {
			return err
		}
	}
	p.applyDefaults()
	if err := p.validate(); err != nil {
		return err
	}
	files, err := renderService(p)
	if err != nil {
		return err
	}
	if err := scaffold.Write(p.Dir, files); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "Created %s (%d files).\n", p.Dir, len(files))
	manual := "  go mod tidy -e\n  make generate\n  go mod tidy\n"
	if p.NoGenerate {
		_, _ = fmt.Fprintf(stdout, "\nNext steps:\n  cd %s\n%s  make test\n  make run\n", p.Dir, manual)
		return nil
	}
	_, _ = fmt.Fprintln(stdout, "Generating code (this downloads modules and buf on the first run)...")
	if err := generateService(ctx, p.Dir, nil, stdout, stderr); err != nil {
		return fmt.Errorf("%w\nThe project is in %s; finish it by hand:\n  cd %s\n%s", err, p.Dir, p.Dir, manual)
	}
	_, _ = fmt.Fprintf(stdout, "\nNext steps:\n  cd %s\n  make test\n  make run\n", p.Dir)
	return nil
}

// generateService resolves the modules, runs buf and tidies go.mod in dir.
// env, when non-nil, replaces the environment of the commands. The sqlc
// output ships pre-generated, so sqlc (which needs cgo) is not run here.
func generateService(ctx context.Context, dir string, env []string, stdout, stderr io.Writer) error {
	steps := [][]string{
		// -e: the packages under gen/ do not exist yet.
		{"go", "mod", "tidy", "-e"},
		{"go", "run", "github.com/bufbuild/buf/cmd/buf@" + serviceVersions.Buf, "generate"},
		{"go", "mod", "tidy"},
	}
	for _, s := range steps {
		_, _ = fmt.Fprintf(stdout, "  %s\n", strings.Join(s, " "))
		cmd := exec.CommandContext(ctx, s[0], s[1:]...) //nolint:gosec // fixed commands
		cmd.Dir = dir
		cmd.Env = env
		cmd.Stdout, cmd.Stderr = stdout, stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("%s: %w", strings.Join(s, " "), err)
		}
	}
	return nil
}

func parseServiceFlags(args []string, stderr io.Writer) (serviceParams, error) {
	var p serviceParams
	fs := flag.NewFlagSet("kitchen new service", flag.ContinueOnError)
	fs.SetOutput(stderr)
	p.addFlags(fs, "github.com/acme/orders", "service name, used for the proto package and telemetry")
	fs.StringVar(&p.DB, "db", dbPostgres, `database: "postgres" (pgx + sqlc + goose migrations) or "none"`)
	fs.BoolVar(&p.NoGenerate, "no-generate", false, "do not run go mod tidy and buf generate after writing the project")
	return p, parseFlags(fs, args)
}

func (p *serviceParams) applyDefaults() {
	p.projectParams.applyDefaults("A gRPC and REST service.")
}

func (p *serviceParams) validate() error {
	errs := p.projectParams.validate()
	switch p.DB {
	case dbPostgres, dbNone:
	default:
		errs = append(errs, fmt.Errorf("-db %q: want %q or %q", p.DB, dbPostgres, dbNone))
	}
	return errors.Join(errs...)
}

// postgresOnly lists the template paths (or directory prefixes ending in
// "/") that exist only with -db postgres.
var postgresOnly = []string{
	"migrations/",
	"sqlc.yaml",
	"internal/app/migrate.go",
	"internal/store/pgstore/",
}

func renderService(p serviceParams) (scaffold.Files, error) {
	files, err := scaffold.Render(templatesFS, "templates/service", serviceTemplateData{
		serviceParams:       p,
		Versions:            serviceVersions,
		GoDirective:         goDirective,
		GoImage:             goImage,
		RuntimeImage:        runtimeImage,
		PostgresImage:       postgresImage,
		GolangciLintVersion: golangciLintVersion,
		Actions:             actions,
	})
	if err != nil {
		return nil, err
	}
	out := scaffold.Files{}
	for rel, b := range files {
		if !p.Postgres() && matchesAny(rel, postgresOnly) {
			continue
		}
		if dir, rest, ok := strings.Cut(rel, "proto/"+protoPkgDir+"/"); ok && dir == "" {
			rel = path.Join("proto", p.ProtoPackage(), rest)
		}
		out[rel] = b
	}
	return out, nil
}

func matchesAny(rel string, patterns []string) bool {
	for _, pat := range patterns {
		if rel == pat || strings.HasSuffix(pat, "/") && strings.HasPrefix(rel, pat) {
			return true
		}
	}
	return false
}

func promptService(ctx context.Context, p *serviceParams) error {
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().Title("Go module path").Placeholder("github.com/acme/orders").
				Value(&p.Module).Validate(validateModule),
			huh.NewInput().Title("Service name").Description("Leave empty to use the last element of the module path.").
				Value(&p.Name),
			huh.NewInput().Title("Description").Value(&p.Description),
			huh.NewSelect[string]().Title("Database").Options(
				huh.NewOption("PostgreSQL (pgx, sqlc, goose migrations)", dbPostgres),
				huh.NewOption("None", dbNone),
			).Value(&p.DB),
			huh.NewSelect[string]().Title("CI workflow").Options(
				huh.NewOption("Self-contained GitHub Actions", ciStandalone),
				huh.NewOption("Reusable workflow pinned to a commit", ciReusable),
			).Value(&p.CI),
		),
		huh.NewGroup(
			huh.NewInput().Title("Reusable workflow").
				Placeholder("acme/devops-workflows/.github/workflows/go.yml@<sha>").
				Value(&p.CIWorkflow).Validate(validateWorkflow),
		).WithHideFunc(func() bool { return p.CI != ciReusable }),
	)
	return form.RunWithContext(ctx)
}
