package mcpkit

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/semconv/v1.41.0/mcpconv"
	"go.opentelemetry.io/otel/trace"

	"github.com/dungnh3/kitchen/otelx"
)

const instrumentationName = "github.com/dungnh3/kitchen/mcpkit"

// errorTypeToolError is the error.type the MCP semantic conventions use for
// a tools/call that returned a result with IsError set.
const errorTypeToolError = "tool_error"

type telemetry struct {
	tracer   trace.Tracer
	duration mcpconv.ServerOperationDuration
}

func newTelemetry(tp trace.TracerProvider, mp metric.MeterProvider) (*telemetry, error) {
	d, err := mcpconv.NewServerOperationDuration(mp.Meter(instrumentationName))
	if err != nil {
		return nil, err
	}
	return &telemetry{tracer: tp.Tracer(instrumentationName), duration: d}, nil
}

// middleware records one SERVER span and one mcp.server.operation.duration
// sample per received request, named "{mcp.method.name} {target}".
func (t *telemetry) middleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		attrs := []attribute.KeyValue{semconv.McpMethodNameKey.String(method)}
		name := method
		switch r := req.(type) {
		case *mcp.CallToolRequest:
			if r.Params != nil {
				name += " " + r.Params.Name
				attrs = append(attrs,
					semconv.GenAIToolName(r.Params.Name),
					semconv.GenAIOperationNameExecuteTool)
			}
		case *mcp.GetPromptRequest:
			if r.Params != nil {
				name += " " + r.Params.Name
				attrs = append(attrs, semconv.GenAIPromptName(r.Params.Name))
			}
		case *mcp.ReadResourceRequest:
			if r.Params != nil {
				attrs = append(attrs, semconv.McpResourceURI(r.Params.URI))
			}
		}
		if ss, ok := req.GetSession().(*mcp.ServerSession); ok && ss != nil {
			if id := ss.ID(); id != "" {
				attrs = append(attrs, semconv.McpSessionID(id))
			}
		}

		start := time.Now()
		ctx, span := t.tracer.Start(ctx, name,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attrs...))
		res, err := next(ctx, method, req)

		var errType string
		switch {
		case err != nil:
			errType = "_OTHER"
			var we *jsonrpc.Error
			if errors.As(err, &we) {
				errType = strconv.FormatInt(we.Code, 10)
				attrs = append(attrs, semconv.RPCResponseStatusCode(errType))
			}
			otelx.RecordError(span, err)
		case isToolError(res):
			errType = errorTypeToolError
			span.SetStatus(codes.Error, errType)
		}
		if errType != "" {
			attrs = append(attrs, semconv.ErrorTypeKey.String(errType))
			span.SetAttributes(semconv.ErrorTypeKey.String(errType))
		}
		span.End()
		t.duration.Record(ctx, time.Since(start).Seconds(), mcpconv.MethodNameAttr(method), attrs[1:]...)
		return res, err
	}
}

func isToolError(res mcp.Result) bool {
	r, ok := res.(*mcp.CallToolResult)
	return ok && r != nil && r.IsError
}
