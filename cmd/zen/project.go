package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/mod/module"

	"github.com/dungnh3/kitchen/cmd/zen/internal/scaffold"
)

// projectParams are the flags shared by every template.
type projectParams struct {
	Module      string
	Dir         string
	Name        string
	Description string
	CI          string
	CIWorkflow  string
	// KitchenVersion is the version of the kitchen modules in go.mod.
	KitchenVersion string
	Yes            bool
}

// CI modes.
const (
	ciStandalone = "standalone"
	ciReusable   = "reusable"
)

func (p *projectParams) addFlags(fs *flag.FlagSet, example, nameUsage string) {
	fs.StringVar(&p.Module, "module", "", "Go module path of the new project, e.g. "+example+" (required)")
	fs.StringVar(&p.Dir, "dir", "", "output directory; must not exist or be empty (default: last element of -module)")
	fs.StringVar(&p.Name, "name", "", nameUsage+" (default: last element of -module)")
	fs.StringVar(&p.Description, "description", "", "one-line description for the README")
	fs.StringVar(&p.CI, "ci", ciStandalone, `CI workflow: "standalone" (self-contained GitHub Actions) or "reusable"`)
	fs.StringVar(&p.CIWorkflow, "ci-workflow", "", "reusable workflow for -ci=reusable: owner/repo/.github/workflows/file.yml@<40-char commit SHA>")
	fs.StringVar(&p.KitchenVersion, "kitchen-version", defaultKitchenVersion, "version of the kitchen modules required in go.mod")
	fs.BoolVar(&p.Yes, "yes", false, "never prompt; use flags only")
}

func (p *projectParams) applyDefaults(description string) {
	base := path.Base(p.Module)
	if p.Name == "" {
		p.Name = base
	}
	if p.Dir == "" {
		p.Dir = base
	}
	if p.Description == "" {
		p.Description = description
	}
}

var (
	nameRE     = regexp.MustCompile(`^[a-z][a-z0-9-]*[a-z0-9]$`)
	workflowRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/\.github/workflows/[A-Za-z0-9_.-]+\.ya?ml@[0-9a-f]{40}$`)
	semverRE   = regexp.MustCompile(`^v\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$`)
)

func (p *projectParams) validate() []error {
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
	if !semverRE.MatchString(p.KitchenVersion) {
		errs = append(errs, fmt.Errorf("-kitchen-version %q is not a semantic version", p.KitchenVersion))
	}
	if p.Dir != "" && filepath.Clean(p.Dir) == "." {
		if entries, err := os.ReadDir(p.Dir); err == nil && len(entries) > 0 {
			errs = append(errs, scaffold.ErrNotEmpty)
		}
	}
	return errs
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

// parseFlags parses args into fs and rejects positional arguments.
func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return errUsage
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(fs.Output(), "unexpected arguments: %v\n", fs.Args())
		return errUsage
	}
	return nil
}
