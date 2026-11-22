package errs

import "google.golang.org/grpc/codes"

// Standard reasons common to API servers. Their domain is empty, so they
// match errors with the same reason from any domain, and a Handler fills in
// its Config.Domain on the wire. Package i18n ships English and Vietnamese
// messages for each reason. Derive specific errors with With or Wrap:
//
//	return errs.ErrVersionMismatch.With(errs.Meta("current_etag", etag))
var (
	// Request shape (400).

	ErrMalformedRequest     = New(codes.InvalidArgument, "MALFORMED_REQUEST", Message("request body cannot be parsed"))
	ErrUnsupportedMediaType = New(codes.InvalidArgument, "UNSUPPORTED_MEDIA_TYPE", Message("unsupported content type"))
	ErrPayloadTooLarge      = New(codes.InvalidArgument, "PAYLOAD_TOO_LARGE", Message("request payload too large"))
	ErrInvalidPageToken     = New(codes.InvalidArgument, "INVALID_PAGE_TOKEN", Message("invalid page token"))
	ErrInvalidPageSize      = New(codes.InvalidArgument, "INVALID_PAGE_SIZE", Message("invalid page size"))

	// Authentication (401).

	ErrTokenMissing  = New(codes.Unauthenticated, "TOKEN_MISSING", Message("missing bearer token"))
	ErrTokenInvalid  = New(codes.Unauthenticated, "TOKEN_INVALID", Message("invalid token"))
	ErrTokenExpired  = New(codes.Unauthenticated, "TOKEN_EXPIRED", Message("token expired"))
	ErrAPIKeyInvalid = New(codes.Unauthenticated, "API_KEY_INVALID", Message("invalid API key"))

	// Authorization (403).

	ErrForbidden         = New(codes.PermissionDenied, "FORBIDDEN", Message("permission denied"))
	ErrInsufficientScope = New(codes.PermissionDenied, "INSUFFICIENT_SCOPE", Message("token lacks a required scope"))
	ErrAccountDisabled   = New(codes.PermissionDenied, "ACCOUNT_DISABLED", Message("account disabled"))

	// Resource state (409 for Aborted, 400 for FailedPrecondition).

	ErrResourceConflict         = New(codes.Aborted, "RESOURCE_CONFLICT", Message("resource was modified concurrently"))
	ErrVersionMismatch          = New(codes.Aborted, "VERSION_MISMATCH", Message("resource version (etag) does not match"))
	ErrPreconditionFailed       = New(codes.FailedPrecondition, "PRECONDITION_FAILED", Message("request precondition failed"))
	ErrInvalidState             = New(codes.FailedPrecondition, "INVALID_STATE", Message("operation not allowed in the current state"))
	ErrClientVersionUnsupported = New(codes.FailedPrecondition, "CLIENT_VERSION_UNSUPPORTED", Message("client version no longer supported"))
	ErrFeatureDisabled          = New(codes.FailedPrecondition, "FEATURE_DISABLED", Message("feature disabled"))

	// Idempotency.

	ErrIdempotencyKeyReused = New(codes.FailedPrecondition, "IDEMPOTENCY_KEY_REUSED", Message("idempotency key reused with a different request"))
	ErrRequestInProgress    = New(codes.Aborted, "REQUEST_IN_PROGRESS", Message("a request with this idempotency key is in progress"))

	// Limits (429).

	ErrRateLimited        = New(codes.ResourceExhausted, "RATE_LIMITED", Message("rate limit exceeded"))
	ErrQuotaExceeded      = New(codes.ResourceExhausted, "QUOTA_EXCEEDED", Message("quota exceeded"))
	ErrConcurrencyLimited = New(codes.ResourceExhausted, "CONCURRENCY_LIMITED", Message("too many concurrent requests"))

	// Server side (503, 504).

	ErrServiceDraining       = New(codes.Unavailable, "SERVICE_DRAINING", Message("server is shutting down"))
	ErrMaintenance           = New(codes.Unavailable, "MAINTENANCE", Message("service under maintenance"))
	ErrDependencyUnavailable = New(codes.Unavailable, "DEPENDENCY_UNAVAILABLE", Message("a dependency is unavailable"))
	ErrUpstreamTimeout       = New(codes.DeadlineExceeded, "UPSTREAM_TIMEOUT", Message("upstream call timed out"))
)

// Catalog returns every sentinel of this package, code sentinels first. Use
// it to check that a message catalog covers all reasons.
func Catalog() []*Error {
	return []*Error{
		ErrCanceled, ErrUnknown, ErrInvalidArgument, ErrDeadlineExceeded, ErrNotFound,
		ErrAlreadyExists, ErrPermissionDenied, ErrResourceExhausted, ErrFailedPrecondition,
		ErrAborted, ErrOutOfRange, ErrUnimplemented, ErrInternal, ErrUnavailable, ErrDataLoss,
		ErrUnauthenticated,

		ErrMalformedRequest, ErrUnsupportedMediaType, ErrPayloadTooLarge, ErrInvalidPageToken,
		ErrInvalidPageSize,
		ErrTokenMissing, ErrTokenInvalid, ErrTokenExpired, ErrAPIKeyInvalid,
		ErrForbidden, ErrInsufficientScope, ErrAccountDisabled,
		ErrResourceConflict, ErrVersionMismatch, ErrPreconditionFailed, ErrInvalidState,
		ErrClientVersionUnsupported, ErrFeatureDisabled,
		ErrIdempotencyKeyReused, ErrRequestInProgress,
		ErrRateLimited, ErrQuotaExceeded, ErrConcurrencyLimited,
		ErrServiceDraining, ErrMaintenance, ErrDependencyUnavailable, ErrUpstreamTimeout,
	}
}
