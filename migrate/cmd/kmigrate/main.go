// Command kmigrate applies goose-format SQL migrations to PostgreSQL or MySQL
// using the same code path as the github.com/dungnh3/kitchen/migrate library.
//
// Usage:
//
//	kmigrate [flags] up
//	kmigrate [flags] down N
//	kmigrate [flags] to VERSION      (0 reverts everything)
//	kmigrate [flags] version
//	kmigrate [flags] status
//	kmigrate [flags] create NAME     (writes <timestamp>_NAME.sql into -path)
//
// Flags:
//
//	-dialect D        postgres (default) or mysql
//	-database DSN     PostgreSQL URL or MySQL DSN (default $DATABASE_URL)
//	-path DIR         directory with *.sql migrations (default ".")
//	-table NAME       history table (default goose_db_version)
//	-lock-timeout D   migration lock wait (default 1m)
//	-allow-out-of-order  apply missing older migrations
//
// Connections use kitchen's pg/mysqlx defaults: TLS is required unless the
// DSN disables it (sslmode=disable, tls=false).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dungnh3/kitchen/log"
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

var validName = regexp.MustCompile(`^[a-z0-9_]+$`)

const template = "-- +goose Up\n\n\n-- +goose Down\n\n"

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("kmigrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dialect := fs.String("dialect", "postgres", "postgres or mysql")
	dsn := fs.String("database", os.Getenv("DATABASE_URL"), "PostgreSQL URL or MySQL DSN (default $DATABASE_URL)")
	dir := fs.String("path", ".", "directory containing migration files")
	table := fs.String("table", "", "history table (default goose_db_version)")
	lockTimeout := fs.Duration("lock-timeout", time.Minute, "migration lock wait")
	outOfOrder := fs.Bool("allow-out-of-order", false, "apply missing older migrations")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return errors.New("missing command: up | down N | to VERSION | version | status | create NAME")
	}
	if rest[0] == "create" { // needs no database
		if len(rest) != 2 || !validName.MatchString(rest[1]) {
			return errors.New("create needs one NAME of [a-z0-9_]")
		}
		path := filepath.Join(*dir, time.Now().UTC().Format("20060102150405")+"_"+rest[1]+".sql")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // path built from a validated name.
		if err != nil {
			return err
		}
		if _, err := io.WriteString(f, template); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, path)
		return err
	}
	if *dsn == "" {
		return errors.New("-database or $DATABASE_URL is required")
	}
	argInt := func() (int64, error) {
		if len(rest) != 2 {
			return 0, fmt.Errorf("%s needs exactly one numeric argument", rest[0])
		}
		return strconv.ParseInt(rest[1], 10, 64)
	}

	m, err := migrate.Open(ctx, migrate.Dialect(*dialect), *dsn, migrate.Config{
		Source:          os.DirFS(*dir),
		Table:           *table,
		LockTimeout:     *lockTimeout,
		AllowOutOfOrder: *outOfOrder,
		Logger:          cliLogger(stdout, stderr),
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
		return m.Down(ctx, int(n))
	case "to":
		v, err := argInt()
		if err != nil {
			return err
		}
		return m.To(ctx, v)
	case "version":
		v, err := m.Version(ctx)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, v)
		return err
	case "status":
		st, err := m.Status(ctx)
		if err != nil {
			return err
		}
		for _, s := range st {
			at := ""
			if !s.AppliedAt.IsZero() {
				at = s.AppliedAt.UTC().Format(time.RFC3339)
			}
			if _, err := fmt.Fprintf(stdout, "%-8s %-20s %s\n", s.State, at, filepath.Base(s.Source.Path)); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown command %q", rest[0])
	}
}

// cliLogger installs the process logger from LOG_* variables: text unless
// LOG_FORMAT says otherwise, on stderr unless LOG_OUTPUT says otherwise
// (stdout carries command output).
func cliLogger(stdout, stderr io.Writer) *log.Logger {
	opts, err := log.OptionsFromEnv()
	if os.Getenv("LOG_FORMAT") == "" {
		opts.Format = log.FormatText
	}
	if opts.Output == nil {
		opts.Output = stderr
		// OptionsFromEnv leaves Output nil for an explicit LOG_OUTPUT=stdout too.
		if strings.EqualFold(strings.TrimSpace(os.Getenv("LOG_OUTPUT")), "stdout") {
			opts.Output = stdout
		}
	}
	l, serr := log.Setup(opts)
	if serr != nil { // invalid LOG_BACKEND/LOG_FORMAT: fall back to defaults
		l, _ = log.Setup(log.Options{Format: log.FormatText, Output: stderr})
	}
	if err = errors.Join(err, serr); err != nil {
		l.Warn("kmigrate: invalid logging environment", log.Err(err))
	}
	return l
}
