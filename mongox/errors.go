package mongox

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"

	"github.com/dungnh3/kitchen/errs"
)

// Server error codes used by MapError.
const (
	codeWriteConflict              = 112
	codeDocumentValidationFailure  = 121
	codeMaxTimeMSExpired           = 50
	codeNotWritablePrimary         = 10107
	codeInterruptedAtShutdown      = 11600
	codeShutdownInProgress         = 91
	codePrimarySteppedDown         = 189
	codeNotPrimaryNoSecondaryOk    = 13435
	codeInterruptedDueToReplChange = 11602
)

// MapError translates a driver error into the matching errs sentinel, keeping
// err as the cause (for logs and errors.Is/As). Clients only see the
// sentinel's generic message, so key values and index names never leak.
// Errors that are already an *errs.Error, nil, context errors (errs already
// classifies them) and errors with no meaningful mapping are returned
// unchanged.
//
//	mongo.ErrNoDocuments                       → errs.ErrNotFound
//	duplicate key (11000, 11001, 12582)        → errs.ErrAlreadyExists
//	121 document validation failed             → errs.ErrFailedPrecondition
//	112 write conflict, TransientTransactionError → errs.ErrAborted (retry the tx)
//	50 MaxTimeMSExpired, client-side timeout   → errs.ErrDeadlineExceeded
//	network errors, not primary, shutdown,
//	server selection, client disconnected      → errs.ErrUnavailable
func MapError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*errs.Error](err); ok {
		return err
	}
	// No reachable server within the selection timeout is an outage, even
	// though the driver wraps it around a context deadline.
	if _, ok := errors.AsType[topology.ServerSelectionError](err); ok {
		return errs.Wrap(err, errs.ErrUnavailable)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if s := sentinelOf(err); s != nil {
		return errs.Wrap(err, s)
	}
	return err
}

func sentinelOf(err error) *errs.Error {
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return errs.ErrNotFound
	case mongo.IsDuplicateKeyError(err):
		return errs.ErrAlreadyExists
	case errors.Is(err, mongo.ErrClientDisconnected), mongo.IsNetworkError(err):
		return errs.ErrUnavailable
	}
	if se, ok := errors.AsType[mongo.ServerError](err); ok {
		switch {
		case se.HasErrorCode(codeDocumentValidationFailure):
			return errs.ErrFailedPrecondition
		case se.HasErrorCode(codeWriteConflict), se.HasErrorLabel("TransientTransactionError"):
			return errs.ErrAborted
		case se.HasErrorCode(codeMaxTimeMSExpired):
			return errs.ErrDeadlineExceeded
		case se.HasErrorCode(codeNotWritablePrimary), se.HasErrorCode(codeInterruptedAtShutdown),
			se.HasErrorCode(codeShutdownInProgress), se.HasErrorCode(codePrimarySteppedDown),
			se.HasErrorCode(codeNotPrimaryNoSecondaryOk), se.HasErrorCode(codeInterruptedDueToReplChange):
			return errs.ErrUnavailable
		}
	}
	if mongo.IsTimeout(err) {
		return errs.ErrDeadlineExceeded
	}
	return nil
}
