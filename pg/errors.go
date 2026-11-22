package pg

import (
	"errors"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dungnh3/kitchen/errs"
)

// MapError translates a pgx or Postgres error into the matching errs
// sentinel, keeping err as the cause (for logs and errors.Is/As). Clients
// only see the sentinel's generic message, so constraint, table and column
// names never leak. Errors that are already an *errs.Error, nil, and errors
// with no meaningful mapping (including context errors, which errs already
// classifies) are returned unchanged.
//
//	pgx.ErrNoRows                         → errs.ErrNotFound
//	23505 unique, 23P01 exclusion         → errs.ErrAlreadyExists
//	other class 23 (FK, check, not null)  → errs.ErrFailedPrecondition
//	class 22 data exception               → errs.ErrInvalidArgument
//	40001 serialization, 40P01 deadlock   → errs.ErrAborted (retry the tx)
//	57014 query_canceled (timeout)        → errs.ErrDeadlineExceeded
//	class 08, 53300, 57P01–57P03          → errs.ErrUnavailable
func MapError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*errs.Error](err); ok {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return errs.Wrap(err, errs.ErrNotFound)
	}
	pe, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		return err
	}
	if s := sentinelOf(pe.Code); s != nil {
		return errs.Wrap(err, s)
	}
	return err
}

func sentinelOf(code string) *errs.Error {
	switch code {
	case pgerrcode.UniqueViolation, pgerrcode.ExclusionViolation:
		return errs.ErrAlreadyExists
	case pgerrcode.SerializationFailure, pgerrcode.DeadlockDetected:
		return errs.ErrAborted
	case pgerrcode.QueryCanceled:
		return errs.ErrDeadlineExceeded
	case pgerrcode.TooManyConnections, pgerrcode.AdminShutdown,
		pgerrcode.CrashShutdown, pgerrcode.CannotConnectNow:
		return errs.ErrUnavailable
	}
	switch {
	case pgerrcode.IsIntegrityConstraintViolation(code):
		return errs.ErrFailedPrecondition
	case pgerrcode.IsDataException(code):
		return errs.ErrInvalidArgument
	case pgerrcode.IsConnectionException(code):
		return errs.ErrUnavailable
	}
	return nil
}
