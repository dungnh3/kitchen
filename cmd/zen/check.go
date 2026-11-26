package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"time"

	"golang.org/x/mod/semver"
)

const cliModule = "github.com/dungnh3/kitchen/cmd/zen"

type checkParams struct {
	Dir     string
	Write   bool
	Offline bool
}

// checkCommand runs "zen check": it fails when a managed file was
// edited or deleted, warns when the current template or a newer zen
// would write a managed file differently, and with -write updates the
// unedited managed files to the current template.
func checkCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	var p checkParams
	fs := flag.NewFlagSet("zen check", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&p.Dir, "dir", ".", "project directory")
	fs.BoolVar(&p.Write, "write", false, "update the managed files that were not edited to the current template")
	fs.BoolVar(&p.Offline, "offline", false, "do not look up the latest zen version")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	return runCheck(ctx, p, stdout)
}

// errEdited is returned when managed files were edited by hand.
var errEdited = errors.New("managed files were edited by hand")

func runCheck(ctx context.Context, p checkParams, out io.Writer) error {
	root, err := os.OpenRoot(p.Dir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	m, err := readManifest(root)
	if err != nil {
		return err
	}
	sp := m.Answers.params()
	sp.applyDefaults()
	if err := sp.validate(); err != nil {
		return fmt.Errorf("%s answers: %w", manifestFile, err)
	}
	files, err := renderService(sp)
	if err != nil {
		return err
	}

	var edited, outdated []string
	for _, rel := range slices.Sorted(maps.Keys(m.Managed)) {
		disk, err := root.ReadFile(filepath.FromSlash(rel))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			_, _ = fmt.Fprintf(out, "%s: deleted; restore it, or remove it from managed in %s to stop managing it\n", rel, manifestFile)
			edited = append(edited, rel)
			continue
		case err != nil:
			return err
		case hashOf(disk) != m.Managed[rel]:
			_, _ = fmt.Fprintf(out, "%s: edited by hand; restore it, or remove it from managed in %s to own it\n", rel, manifestFile)
			edited = append(edited, rel)
			continue
		}
		cur, ok := files[rel]
		switch {
		case !ok:
			_, _ = fmt.Fprintf(out, "%s: warning: the template no longer generates it; remove it from managed in %s\n", rel, manifestFile)
		case !bytes.Equal(cur, disk):
			outdated = append(outdated, rel)
		}
	}
	// A managed file that is in neither the manifest nor the project is new
	// in the template; one that exists on disk is owned by the project.
	for _, rel := range serviceManaged {
		if _, ok := m.Managed[rel]; ok {
			continue
		}
		if _, err := root.Stat(filepath.FromSlash(rel)); errors.Is(err, fs.ErrNotExist) {
			outdated = append(outdated, rel)
		}
	}

	if p.Write && len(outdated) > 0 {
		for _, rel := range outdated {
			if err := writeFile(root, rel, files[rel]); err != nil {
				return err
			}
			m.Managed[rel] = hashOf(files[rel])
			_, _ = fmt.Fprintf(out, "%s: updated to the current template\n", rel)
		}
		m.Zen = cliVersion()
		b, err := m.marshal()
		if err != nil {
			return err
		}
		if err := writeFile(root, manifestFile, b); err != nil {
			return err
		}
	} else {
		for _, rel := range outdated {
			_, _ = fmt.Fprintf(out, "%s: warning: the current template differs; run \"zen check -write\"\n", rel)
		}
	}

	if !p.Offline {
		reportLatest(ctx, out)
	}
	if len(edited) > 0 {
		return fmt.Errorf("%w: %d file(s)", errEdited, len(edited))
	}
	_, _ = fmt.Fprintf(out, "%d managed files checked\n", len(m.Managed))
	return nil
}

func writeFile(root *os.Root, rel string, b []byte) error {
	name := filepath.FromSlash(rel)
	if d := filepath.Dir(name); d != "." {
		if err := root.MkdirAll(d, 0o755); err != nil { //nolint:gosec // project directory
			return err
		}
	}
	return root.WriteFile(name, b, 0o644) //nolint:gosec // project source file
}

// reportLatest warns when a newer zen is released. It asks the Go
// module proxy through "go list" and stays silent when that fails, for
// example offline or before the first release.
func reportLatest(ctx context.Context, out io.Writer) {
	cur := cliVersion()
	if !semver.IsValid(cur) {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-m", "-json", cliModule+"@latest")
	cmd.Dir = os.TempDir() // outside any module or workspace
	b, err := cmd.Output()
	if err != nil {
		return
	}
	var mod struct{ Version string }
	if json.Unmarshal(b, &mod) != nil || !semver.IsValid(mod.Version) {
		return
	}
	if semver.Compare(mod.Version, cur) > 0 {
		_, _ = fmt.Fprintf(out, "warning: zen %s is available (this is %s): go run %s@%s check -write\n", mod.Version, cur, cliModule, mod.Version)
	}
}
