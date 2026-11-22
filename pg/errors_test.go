package pg_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/pg"
)

func TestMapError(t *testing.T) {
	pgErr := func(code string) error {
		return fmt.Errorf("insert user: %w", &pgconn.PgError{Code: code, ConstraintName: "users_email_key"})
	}
	other := errors.New("boom")
	already := errs.ErrPermissionDenied.With()
	tests := []struct {
		name string
		in   error
		want *errs.Error // nil: returned unchanged
	}{
		{"no rows", fmt.Errorf("get: %w", pgx.ErrNoRows), errs.ErrNotFound},
		{"unique", pgErr("23505"), errs.ErrAlreadyExists},
		{"exclusion", pgErr("23P01"), errs.ErrAlreadyExists},
		{"foreign key", pgErr("23503"), errs.ErrFailedPrecondition},
		{"check", pgErr("23514"), errs.ErrFailedPrecondition},
		{"not null", pgErr("23502"), errs.ErrFailedPrecondition},
		{"invalid text", pgErr("22P02"), errs.ErrInvalidArgument},
		{"serialization", pgErr("40001"), errs.ErrAborted},
		{"deadlock", pgErr("40P01"), errs.ErrAborted},
		{"statement timeout", pgErr("57014"), errs.ErrDeadlineExceeded},
		{"too many conns", pgErr("53300"), errs.ErrUnavailable},
		{"admin shutdown", pgErr("57P01"), errs.ErrUnavailable},
		{"connection failure", pgErr("08006"), errs.ErrUnavailable},
		{"syntax error", pgErr("42601"), nil},
		{"plain", other, nil},
		{"context", context.Canceled, nil},
		{"already errs", already, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pg.MapError(tt.in)
			if tt.want == nil {
				if got != tt.in { //nolint:errorlint // identity: the input must come back unchanged.
					t.Fatalf("got %v, want input unchanged", got)
				}
				return
			}
			if !errors.Is(got, tt.want) {
				t.Fatalf("got %v (code %v), want %v", got, errs.Code(got), tt.want)
			}
			if !errors.Is(got, tt.in) && !errors.Is(got, pgx.ErrNoRows) {
				if _, ok := errors.AsType[*pgconn.PgError](got); !ok {
					t.Fatal("cause lost")
				}
			}
		})
	}
	if pg.MapError(nil) != nil {
		t.Fatal("nil must map to nil")
	}
}

func TestMapErrorHidesConstraint(t *testing.T) {
	err := pg.MapError(&pgconn.PgError{Code: "23505", ConstraintName: "users_email_key", Message: "duplicate key"})
	st, _ := errs.ToStatus(err)
	if msg := st.Message(); msg != "already exists" {
		t.Fatalf("client message = %q", msg)
	}
}
