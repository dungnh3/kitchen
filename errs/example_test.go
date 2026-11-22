package errs_test

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/dungnh3/kitchen/errs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrAgentNotFound = errs.New(codes.NotFound, "AGENT_NOT_FOUND",
	errs.Domain("agents.example.com"), errs.Message("agent not found"))

func getAgent(id string) error {
	// The database error is kept for logs but never sent to clients.
	return errs.Wrap(sql.ErrNoRows, ErrAgentNotFound, errs.Meta("agent_id", id))
}

func Example() {
	err := getAgent("a1")

	fmt.Println(errors.Is(err, ErrAgentNotFound), errors.Is(err, sql.ErrNoRows))
	fmt.Println(errs.Code(err), errs.HTTPStatus(err))
	st := status.Convert(err)
	fmt.Println(st.Message())
	// Output:
	// true true
	// NotFound 404
	// agent not found
}

func ExampleHandler_WriteHTTP() {
	eh := errs.NewHandler(errs.Config{Domain: "svc.example.com"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/agents/a1", nil)
	eh.WriteHTTP(rec, req, errs.Wrap(errors.New("db down"), ErrAgentNotFound, errs.Meta("agent_id", "a1")))
	fmt.Println(rec.Code)
	fmt.Println(rec.Body.String())
	// Output:
	// 404
	// {"error":{"code":404,"message":"agent not found","status":"NOT_FOUND","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"AGENT_NOT_FOUND","domain":"agents.example.com","metadata":{"agent_id":"a1"}}]}}
}

func ExampleFromStatus() {
	// A client receives a status error and matches it against a shared sentinel.
	received := status.Convert(ErrAgentNotFound).Err()
	fmt.Println(errors.Is(errs.FromError(received), ErrAgentNotFound))
	// Output: true
}

func Example_codeSentinels() {
	// Code sentinels match by gRPC code; catalog sentinels by reason.
	err := errs.ErrTokenExpired.With(errs.Meta("user", "u1"))
	fmt.Println(errors.Is(err, errs.ErrUnauthenticated), errors.Is(err, errs.ErrTokenExpired), errors.Is(err, errs.ErrTokenInvalid))
	// Output: true true false
}
