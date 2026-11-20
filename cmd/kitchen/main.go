// Command kitchen scaffolds new projects that use the kitchen libraries.
//
// Usage:
//
//	kitchen new mcp [flags]    create an MCP server
//	kitchen version
//
// Run "kitchen new mcp -h" for the flags. With a terminal and missing
// required flags it asks for them interactively; with --yes or without a
// terminal it only uses flags, which suits CI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

const usage = `kitchen scaffolds projects that use github.com/dungnh3/kitchen.

Usage:
  kitchen new mcp [flags]   create an MCP server (run with -h for flags)
  kitchen version           print the version

"kitchen new service" (gRPC service) is planned; see docs/design/codegen-mcp.md.
`

func run(ctx context.Context, args []string, stdin *os.File, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "new":
		if len(args) < 2 {
			_, _ = fmt.Fprint(stderr, usage)
			return 2
		}
		switch args[1] {
		case "mcp":
			err = newMCP(ctx, args[2:], stdin, stdout, stderr)
		case "service":
			err = errors.New(`"new service" is not available yet: it ships once the kitchen server, otel, errs, pg and pubsub packages are released`)
		default:
			err = fmt.Errorf("unknown template %q", args[1])
		}
	case "version":
		_, _ = fmt.Fprintln(stdout, version())
	case "help", "-h", "--help":
		_, _ = fmt.Fprint(stdout, usage)
	default:
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		return 2
	default:
		_, _ = fmt.Fprintln(stderr, "kitchen:", err)
		return 1
	}
}

// errUsage reports a flag error already printed by the flag package.
var errUsage = errors.New("usage")

func version() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "devel"
}
