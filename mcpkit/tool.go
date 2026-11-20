package mcpkit

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ToolOption configures one tool registered with [AddTool].
type ToolOption func(*toolConfig)

type toolConfig struct {
	timeout time.Duration
}

// Timeout sets the tool's deadline, overriding [WithToolTimeout]. Zero
// disables the default deadline for this tool.
func Timeout(d time.Duration) ToolOption {
	return func(c *toolConfig) { c.timeout = d }
}

// AddTool registers a typed tool handler on s.
//
// The SDK infers the input and output JSON schemas from In and Out (use
// `json` and `jsonschema` struct tags), validates arguments before the call,
// returns Out as structured content, and turns a returned error into a
// result with IsError set, so the model can read it and recover. Return a
// *jsonrpc.Error only for protocol-level failures.
//
// A deadline exceeded because of the tool timeout is reported as a tool
// error naming the timeout. Like the SDK's AddTool, it panics if t is
// invalid or the schemas cannot be inferred: register tools at startup.
func AddTool[In, Out any](s *Server, t *mcp.Tool, h mcp.ToolHandlerFor[In, Out], opts ...ToolOption) {
	cfg := toolConfig{timeout: s.toolTimeout}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.timeout < 0 {
		panic(fmt.Sprintf("mcpkit: tool %q: negative timeout", t.Name))
	}
	if cfg.timeout == 0 {
		mcp.AddTool(s.sdk, t, h)
		return
	}
	name, timeout := t.Name, cfg.timeout
	mcp.AddTool(s.sdk, t, func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
		ctx, cancel := context.WithTimeoutCause(ctx, timeout, errToolTimeout)
		defer cancel()
		res, out, err := h(ctx, req, in)
		if err != nil && errors.Is(context.Cause(ctx), errToolTimeout) && errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("tool %q timed out after %s", name, timeout)
		}
		return res, out, err
	})
}

var errToolTimeout = errors.New("mcpkit: tool timeout")

// recoverMiddleware turns a panic in any handler into an error instead of
// crashing the process. For tools/call it returns an IsError result with a
// generic message; the panic value and stack go only to the log.
func (s *Server) recoverMiddleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (res mcp.Result, err error) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			s.logger.ErrorContext(ctx, "mcpkit: handler panicked",
				"method", method, "panic", r, "stack", string(debug.Stack()))
			if method == "tools/call" {
				out := &mcp.CallToolResult{}
				out.SetError(errors.New("internal error"))
				res, err = out, nil
				return
			}
			res, err = nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "internal error"}
		}()
		return next(ctx, method, req)
	}
}
