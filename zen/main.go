// Command zen scaffolds new projects that use the kitchen libraries.
//
// Usage:
//
//	zen new service [flags]  create a gRPC + REST service
//	zen new mcp [flags]      create an MCP server
//	zen add <kind> <name>    add a domain, consumer or job to a generated service
//	zen check [flags]        check the managed files of a generated service
//	zen version
//
// Run "zen new service -h" or "zen new mcp -h" for the flags. With
// a terminal and missing required flags it asks for them interactively; with
// --yes or without a terminal it only uses flags, which suits CI.
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

const usage = `zen scaffolds projects that use github.com/dungnh3/kitchen.

Usage:
  zen new service [flags]  create a gRPC + REST service (run with -h for flags)
  zen new mcp [flags]      create an MCP server (run with -h for flags)
  zen add <kind> <name>    add a domain, consumer or job to a generated service
  zen check [flags]        check the managed files of a generated service
  zen version              print the version
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
			err = newService(ctx, args[2:], stdin, stdout, stderr)
		default:
			err = fmt.Errorf("unknown template %q", args[1])
		}
	case "add":
		err = addCommand(ctx, args[1:], stdout, stderr)
	case "check":
		err = checkCommand(ctx, args[1:], stdout, stderr)
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
		_, _ = fmt.Fprintln(stderr, "zen:", err)
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
