package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"charm.land/huh/v2"

	"github.com/dungnh3/kitchen/cmd/zen/internal/scaffold"
)

//go:embed all:templates
var templatesFS embed.FS

// Versions written into generated projects. Bump them together with the
// golden files (go test ./... -update).
const (
	defaultMCPKitVersion  = "v0.1.0"
	defaultKitchenVersion = "v0.1.0"
	goSDKVersion          = "v1.8.0"
	goDirective           = "1.26.0"
	goImage               = "1.27-alpine3.24"
	runtimeImage          = "gcr.io/distroless/static-debian13:nonroot"
	golangciLintVersion   = "v2.15.0"
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

type mcpParams struct {
	projectParams
	MCPKitVersion string
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
	fs := flag.NewFlagSet("zen new mcp", flag.ContinueOnError)
	fs.SetOutput(stderr)
	p.addFlags(fs, "github.com/acme/mcp-notes", "server name reported to clients")
	fs.StringVar(&p.MCPKitVersion, "mcpkit-version", defaultMCPKitVersion, "mcpkit version required in go.mod")
	return p, parseFlags(fs, args)
}

func (p *mcpParams) applyDefaults() { p.projectParams.applyDefaults("An MCP server.") }

func (p *mcpParams) validate() error {
	errs := p.projectParams.validate()
	if !semverRE.MatchString(p.MCPKitVersion) {
		errs = append(errs, fmt.Errorf("-mcpkit-version %q is not a semantic version", p.MCPKitVersion))
	}
	return errors.Join(errs...)
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
