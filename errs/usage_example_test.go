package errs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/i18n"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/server"
	"golang.org/x/text/language"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Errors in this file are the examples of errs/USAGE.md.

func ExampleError_With() {
	// With returns a copy: the shared sentinel is never modified.
	err := errs.ErrRateLimited.With(
		errs.Meta("limit", "100"),
		errs.RetryAfter(30*time.Second),
	)
	fmt.Println(errors.Is(err, errs.ErrRateLimited), errs.Code(err), errs.HTTPStatus(err))
	fmt.Println(err.Metadata()["limit"], len(errs.ErrRateLimited.Metadata()))
	// Output:
	// true ResourceExhausted 429
	// 100 0
}

func ExampleHTTPStatus() {
	// The HTTP status follows the gRPC code; 413 and 415 are the only
	// exceptions (INVALID_ARGUMENT with those reasons).
	fmt.Println(errs.HTTPStatus(errs.ErrInvalidArgument))
	fmt.Println(errs.HTTPStatus(errs.ErrPayloadTooLarge))
	fmt.Println(errs.HTTPStatus(errs.ErrUnsupportedMediaType))
	fmt.Println(errs.HTTPStatus(errors.New("boom")))
	// Output:
	// 400
	// 413
	// 415
	// 500
}

func ExampleBadRequest() {
	err := errs.BadRequest(
		errs.FieldViolation{Field: "agent.name", Description: "must not be empty", Reason: "string.min_len"},
	)
	rec := httptest.NewRecorder()
	errs.NewHandler(errs.Config{Domain: "agents.example.com", Logger: log.Discard()}).
		WriteHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/agents", nil), err)
	fmt.Println(rec.Code)
	fmt.Println(rec.Body.String())
	// Output:
	// 400
	// {"error":{"code":400,"message":"invalid argument","status":"INVALID_ARGUMENT","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"INVALID_ARGUMENT","domain":"agents.example.com"},{"@type":"type.googleapis.com/google.rpc.BadRequest","fieldViolations":[{"field":"agent.name","description":"must not be empty","reason":"string.min_len"}]}]}}
}

func ExampleToStatus() {
	// A plain Go error never reaches the client: it becomes INTERNAL.
	st, sanitized := errs.ToStatus(fmt.Errorf("query users: %w", errors.New("dial tcp 10.0.0.7:5432: refused")))
	fmt.Println(st.Code(), st.Message(), sanitized)

	// A downstream service's client error keeps its code and message.
	st, sanitized = errs.ToStatus(status.Error(codes.NotFound, "user u1 not found"))
	fmt.Println(st.Code(), st.Message(), sanitized)

	// A downstream server fault keeps its code with the generic message.
	st, sanitized = errs.ToStatus(status.Error(codes.Unavailable, "redis 10.0.0.9:6379 down"))
	fmt.Println(st.Code(), st.Message(), sanitized)

	// A downstream 401 is about this service's credentials, not the client's.
	st, _ = errs.ToStatus(status.Error(codes.Unauthenticated, "bad service token"))
	fmt.Println(st.Code())
	// Output:
	// Internal internal error true
	// NotFound user u1 not found false
	// Unavailable service unavailable true
	// Internal
}

func ExampleNewHandler_localized() {
	bundle, err := i18n.New(language.English, i18n.WithDefaults(),
		i18n.WithMessages(language.English, map[string]string{"AGENT_NOT_FOUND": "Agent {{.agent_id}} does not exist."}))
	if err != nil {
		panic(err)
	}
	eh := errs.NewHandler(errs.Config{
		Logger:    log.Discard(),
		Localizer: bundle,
		Domain:    "agents.example.com",
	})
	st := eh.Status(context.Background(), ErrAgentNotFound.With(errs.Meta("agent_id", "a1")))
	fmt.Println(st.Code(), st.Message())
	fmt.Println(len(st.Details()))
	// Output:
	// NotFound agent not found
	// 2
}

func Example_server() {
	bundle, err := i18n.New(language.English, i18n.WithDefaults())
	if err != nil {
		panic(err)
	}
	// Default handler, localized: the usual setup.
	srv, err := server.New(server.WithI18n(bundle))
	if err != nil {
		panic(err)
	}
	_ = srv

	// A handler you configure yourself (domain, logger, localizer).
	eh := errs.NewHandler(errs.Config{Localizer: bundle, Domain: "agents.example.com"})
	srv, err = server.New(server.WithErrors(eh))
	if err != nil {
		panic(err)
	}
	_ = srv
}

func Example_decodeEnvelope() {
	rec := httptest.NewRecorder()
	errs.WriteHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/agents/a1", nil), ErrAgentNotFound)

	// What a client (or a test) reads back.
	var body struct {
		Error struct {
			Code    int    `json:"code"`
			Status  string `json:"status"`
			Details []struct {
				Type   string `json:"@type"`
				Reason string `json:"reason"`
			} `json:"details"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		panic(err)
	}
	fmt.Println(body.Error.Code, body.Error.Status, body.Error.Details[0].Reason)
	// Output: 404 NOT_FOUND AGENT_NOT_FOUND
}

func Example_retryAfter() {
	rec := httptest.NewRecorder()
	errs.WriteHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil),
		errs.ErrDependencyUnavailable.With(errs.RetryAfter(5*time.Second)))
	fmt.Println(rec.Code, rec.Header().Get("Retry-After"))
	// Output: 503 5
}
