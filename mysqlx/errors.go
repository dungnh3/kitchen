package mysqlx

import (
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/go-sql-driver/mysql"

	"github.com/dungnh3/kitchen/errs"
)

// MySQL server error numbers used by MapError.
const (
	erDupEntry              = 1062
	erDupEntryWithKeyName   = 1586
	erDupUnique             = 1169
	erRowIsReferenced       = 1451
	erNoReferencedRow       = 1452
	erRowIsReferenced2      = 1217
	erNoReferencedRow2      = 1216
	erBadNull               = 1048
	erCheckConstraint       = 3819
	erDataTooLong           = 1406
	erWarnDataOutOfRange    = 1264
	erTruncatedWrongValue   = 1366
	erTruncatedWrongValue2  = 1292
	erDivisionByZero        = 1365
	erLockDeadlock          = 1213
	erLockWaitTimeout       = 1205
	erQueryTimeout          = 3024
	erQueryInterrupted      = 1317
	erConCount              = 1040
	erServerShutdown        = 1053
	erTooManyUserConnection = 1203
	erReadOnly              = 1290
	erOptionPreventsStmt    = 1836
)

// MapError translates a database/sql or MySQL error into the matching errs
// sentinel, keeping err as the cause (for logs and errors.Is/As). Clients
// only see the sentinel's generic message, so key, table and column names
// never leak. Errors that are already an *errs.Error, nil, and errors with no
// meaningful mapping (including context errors, which errs already
// classifies) are returned unchanged.
//
//	sql.ErrNoRows                               → errs.ErrNotFound
//	1062/1586/1169 duplicate key                → errs.ErrAlreadyExists
//	1451/1452/1216/1217 FK, 1048 null, 3819 check → errs.ErrFailedPrecondition
//	1406/1264/1366/1292/1365 bad value          → errs.ErrInvalidArgument
//	1213 deadlock, 1205 lock wait timeout       → errs.ErrAborted (retry the tx)
//	3024 max_execution_time, 1317 interrupted   → errs.ErrDeadlineExceeded
//	1040/1203 too many connections, 1053 shutdown,
//	1290/1836 read-only, driver.ErrBadConn,
//	mysql.ErrInvalidConn                        → errs.ErrUnavailable
func MapError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*errs.Error](err); ok {
		return err
	}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return errs.Wrap(err, errs.ErrNotFound)
	case errors.Is(err, driver.ErrBadConn), errors.Is(err, mysql.ErrInvalidConn):
		return errs.Wrap(err, errs.ErrUnavailable)
	}
	me, ok := errors.AsType[*mysql.MySQLError](err)
	if !ok {
		return err
	}
	if s := sentinelOf(me.Number); s != nil {
		return errs.Wrap(err, s)
	}
	return err
}

func sentinelOf(n uint16) *errs.Error {
	switch n {
	case erDupEntry, erDupEntryWithKeyName, erDupUnique:
		return errs.ErrAlreadyExists
	case erRowIsReferenced, erNoReferencedRow, erRowIsReferenced2, erNoReferencedRow2,
		erBadNull, erCheckConstraint:
		return errs.ErrFailedPrecondition
	case erDataTooLong, erWarnDataOutOfRange, erTruncatedWrongValue, erTruncatedWrongValue2, erDivisionByZero:
		return errs.ErrInvalidArgument
	case erLockDeadlock, erLockWaitTimeout:
		return errs.ErrAborted
	case erQueryTimeout, erQueryInterrupted:
		return errs.ErrDeadlineExceeded
	case erConCount, erTooManyUserConnection, erServerShutdown, erReadOnly, erOptionPreventsStmt:
		return errs.ErrUnavailable
	}
	return nil
}
