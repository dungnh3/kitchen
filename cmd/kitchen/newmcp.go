package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"charm.land/huh/v2"
	"golang.org/x/mod/module"

	"github.com/dungnh3/kitchen/cmd/kitchen/internal/scaffold"
)

//go:embed all:templates
var templatesFS embed.FS

// Versions written into generated projects. Bump them together with the
// golden files (go test ./... -update).
const (
	defaultMCPKitVersion = "v0.1.0"
	goSDKVersion         = "v1.8.0"
	goDirective          = "1.26.0"
	goImage              = "1.27-alpine3.24"
	runtimeImage         = "gcr.io/distroless/static-debian13:nonroot"
	golangciLintVersion  = "v2.15.0"
)

// actions pins GitHub Actions to full commit SHAs (tag in the comment).
var actions = actionPins{
	Checkout:     "3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1",
	SetupGo:      "d8cbb8045d7f43706b9177cceb36c1f91a6644d5 # v7.1.0",
	GolangciLint: "ba0d7d2ec06a0ea1cb5fa41b2e4a3ab91d21278a # v9.3.0",
	SetupBuildx:  "f87e5991a6d7451dcb8d9637bfbc97413f497069 # v4.4.1",
	BuildPush:    "c3c9e263c25d99ce0380d002d59b67737d91b0dc # v7.4.0",
}

type actionPins struct {
	Checkout, SetupGo, GolangciLint, SetupBuildx, BuildPush string
}

// CI modes.
const (
	ciStandalone = "standalone"
	ciReusable   = "reusable"
)

type mcpParams struct {
	Module        string
	Dir           string
	Name          string
	Description   string
	CI            string
	CIWorkflow    string
	MCPKitVersion string
	Yes           bool
}

// templateData is what the templates see.
type templateData struct {
	mcpParams
	GoSDKVersion        string
	GoDirective         string
	GoImage             string
	RuntimeImage        string
	GolangciLintVersion string
	Actions             actionPins
}

func newMCP(ctx context.Context, args []string, stdin *os.File, stdout, stderr io.Writer) error {
	p, err := parseMCPFlags(args, stderr)
	if err != nil {
		return err
	}
	if p.Module == "" && !p.Yes && isTerminal(stdin) {
		if err := promptMCP(ctx, &p); err != nil {
			return err
		}
	}
	p.applyDefaults()
	if err := p.validate(); err != nil {
		return err
	}
	files, err := renderMCP(p)
	if err != nil {
		return err
	}
	if err := scaffold.Write(p.Dir, files); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "Created %s (%d files).\n\nNext steps:\n  cd %s\n  go mod tidy\n  make test\n  make run\n",
		p.Dir, len(files), p.Dir)
	return nil
}

func parseMCPFlags(args []string, stderr io.Writer) (mcpParams, error) {
	var p mcpParams
	fs := flag.NewFlagSet("kitchen new mcp", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&p.Module, "module", "", "Go module path of the new project, e.g. github.com/acme/mcp-notes (required)")
	fs.StringVar(&p.Dir, "dir", "", "output directory; must not exist or be empty (default: last element of -module)")
	fs.StringVar(&p.Name, "name", "", "server name reported to clients (default: last element of -module)")
	fs.StringVar(&p.Description, "description", "", "one-line description for the README")
	fs.StringVar(&p.CI, "ci", ciStandalone, `CI workflow: "standalone" (self-contained GitHub Actions) or "reusable"`)
	fs.StringVar(&p.CIWorkflow, "ci-workflow", "", "reusable workflow for -ci=reusable: owner/repo/.github/workflows/file.yml@<40-char commit SHA>")
	fs.StringVar(&p.MCPKitVersion, "mcpkit-version", defaultMCPKitVersion, "mcpkit version required in go.mod")
	fs.BoolVar(&p.Yes, "yes", false, "never prompt; use flags only")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return p, err
		}
		return p, errUsage
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "unexpected arguments: %v\n", fs.Args())
		return p, errUsage
	}
	return p, nil
}

func (p *mcpParams) applyDefaults() {
	base := path.Base(p.Module)
	if p.Name == "" {
		p.Name = base
	}
	if p.Dir == "" {
		p.Dir = base
	}
	if p.Description == "" {
		p.Description = "An MCP server."
	}
}

var (
	nameRE     = regexp.MustCompile(`^[a-z][a-z0-9-]*[a-z0-9]$`)
	workflowRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/\.github/workflows/[A-Za-z0-9_.-]+\.ya?ml@[0-9a-f]{40}$`)
	semverRE   = regexp.MustCompile(`^v\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$`)
)

func (p *mcpParams) validate() error {
	var errs []error
	if p.Module == "" {
		errs = append(errs, errors.New("-module is required"))
	} else if err := validateModule(p.Module); err != nil {
		errs = append(errs, err)
	}
	if !nameRE.MatchString(p.Name) {
		errs = append(errs, fmt.Errorf("-name %q: use lower-case letters, digits and dashes", p.Name))
	}
	if strings.ContainsAny(p.Description, "\n\r") {
		errs = append(errs, errors.New("-description must be one line"))
	}
	switch p.CI {
	case ciStandalone:
		if p.CIWorkflow != "" {
			errs = append(errs, errors.New("-ci-workflow requires -ci=reusable"))
		}
	case ciReusable:
		if err := validateWorkflow(p.CIWorkflow); err != nil {
			errs = append(errs, err)
		}
	default:
		errs = append(errs, fmt.Errorf("-ci %q: want %q or %q", p.CI, ciStandalone, ciReusable))
	}
	if !semverRE.MatchString(p.MCPKitVersion) {
		errs = append(errs, fmt.Errorf("-mcpkit-version %q is not a semantic version", p.MCPKitVersion))
	}
	if p.Dir != "" && filepath.Clean(p.Dir) == "." {
		if entries, err := os.ReadDir(p.Dir); err == nil && len(entries) > 0 {
			errs = append(errs, scaffold.ErrNotEmpty)
		}
	}
	return errors.Join(errs...)
}

func validateModule(m string) error {
	if err := module.CheckPath(m); err != nil {
		return fmt.Errorf("-module: %w", err)
	}
	return nil
}

func validateWorkflow(w string) error {
	if !workflowRE.MatchString(w) {
		return fmt.Errorf("-ci-workflow %q: want owner/repo/.github/workflows/file.yml@<40-char commit SHA> (tags and branches are mutable)", w)
	}
	return nil
}

func renderMCP(p mcpParams) (scaffold.Files, error) {
	return scaffold.Render(templatesFS, "templates/mcp", templateData{
		mcpParams:           p,
		GoSDKVersion:        goSDKVersion,
		GoDirective:         goDirective,
		GoImage:             goImage,
		RuntimeImage:        runtimeImage,
		GolangciLintVersion: golangciLintVersion,
		Actions:             actions,
	})
}

func isTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func promptMCP(ctx context.Context, p *mcpParams) error {
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().Title("Go module path").Placeholder("github.com/acme/mcp-notes").
				Value(&p.Module).Validate(validateModule),
			huh.NewInput().Title("Server name").Description("Leave empty to use the last element of the module path.").
				Value(&p.Name),
			huh.NewInput().Title("Description").Value(&p.Description),
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
