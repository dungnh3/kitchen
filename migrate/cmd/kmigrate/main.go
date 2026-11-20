// Command kmigrate applies golang-migrate style SQL migrations to PostgreSQL
// using the same code path as the github.com/dungnh3/kitchen/migrate library.
//
// Usage:
//
//	kmigrate [flags] up
//	kmigrate [flags] down N
//	kmigrate [flags] to VERSION
//	kmigrate [flags] version
//	kmigrate [flags] force VERSION
//
// Flags:
//
//	-database URL     PostgreSQL URL (default $DATABASE_URL)
//	-path DIR         directory with *.up.sql / *.down.sql (default ".")
//	-table NAME       history table (default schema_migrations)
//	-lock-timeout D   advisory lock wait (default 1m)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dungnh3/kitchen/migrate"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "kmigrate:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("kmigrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbURL := fs.String("database", os.Getenv("DATABASE_URL"), "PostgreSQL URL (default $DATABASE_URL)")
	dir := fs.String("path", ".", "directory containing migration files")
	table := fs.String("table", "", "history table (default schema_migrations)")
	lockTimeout := fs.Duration("lock-timeout", time.Minute, "advisory lock wait")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return errors.New("missing command: up | down N | to VERSION | version | force VERSION")
	}
	if *dbURL == "" {
		return errors.New("-database or $DATABASE_URL is required")
	}
	argInt := func() (int, error) {
		if len(rest) != 2 {
			return 0, fmt.Errorf("%s needs exactly one numeric argument", rest[0])
		}
		return strconv.Atoi(rest[1])
	}

	m, err := migrate.Open(ctx, *dbURL, migrate.Config{
		Source:      os.DirFS(*dir),
		Table:       *table,
		LockTimeout: *lockTimeout,
		Logger:      slog.New(slog.NewTextHandler(stderr, nil)),
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	switch rest[0] {
	case "up":
		return m.Up(ctx)
	case "down":
		n, err := argInt()
		if err != nil {
			return err
		}
		return m.Down(ctx, n)
	case "to":
		v, err := argInt()
		if err != nil {
			return err
		}
		if v < 0 {
			return errors.New("to: version must be >= 0")
		}
		return m.To(ctx, uint(v))
	case "force":
		v, err := argInt()
		if err != nil {
			return err
		}
		return m.Force(ctx, v)
	case "version":
		v, dirty, err := m.Version(ctx)
		if errors.Is(err, migrate.ErrNilVersion) {
			_, err := fmt.Fprintln(stdout, "no migration applied")
			return err
		}
		if err != nil {
			return err
		}
		if dirty {
			_, err = fmt.Fprintf(stdout, "%d (dirty)\n", v)
		} else {
			_, err = fmt.Fprintln(stdout, v)
		}
		return err
	default:
		return fmt.Errorf("unknown command %q", rest[0])
	}
}
