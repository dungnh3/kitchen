package mongox

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/dbconv"
	"go.opentelemetry.io/otel/trace"

	"github.com/dungnh3/kitchen/otelx"
)

const scope = "github.com/dungnh3/kitchen/mongox"

// instrumentation turns driver command and pool events into OTel spans and
// metrics following the database semantic conventions. Command bodies are
// never recorded (they hold user data); only the operation, database,
// collection and server are.
type instrumentation struct {
	tracer   trace.Tracer
	duration dbconv.ClientOperationDuration
	conns    dbconv.ClientConnectionCount
	spans    sync.Map // reqKey → trace.Span
}

type reqKey struct {
	conn string
	id   int64
}

func newInstrumentation(tp trace.TracerProvider, mp metric.MeterProvider) (*instrumentation, error) {
	meter := mp.Meter(scope)
	d, err := dbconv.NewClientOperationDuration(meter)
	if err != nil {
		return nil, err
	}
	c, err := dbconv.NewClientConnectionCount(meter)
	if err != nil {
		return nil, err
	}
	return &instrumentation{tracer: tp.Tracer(scope), duration: d, conns: c}, nil
}

func (in *instrumentation) commandMonitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: in.started,
		Succeeded: func(ctx context.Context, e *event.CommandSucceededEvent) {
			in.finished(ctx, &e.CommandFinishedEvent, nil)
		},
		Failed: func(ctx context.Context, e *event.CommandFailedEvent) {
			in.finished(ctx, &e.CommandFinishedEvent, e.Failure)
		},
	}
}

func (in *instrumentation) started(ctx context.Context, e *event.CommandStartedEvent) {
	coll := collectionOf(e.CommandName, e.Command)
	name := e.CommandName
	if coll != "" {
		name += " " + coll
	} else if e.DatabaseName != "" {
		name += " " + e.DatabaseName
	}
	attrs := append(commonAttrs(e.CommandName, e.DatabaseName, coll, e.ConnectionID), semconv.DBSystemNameMongoDB)
	_, span := in.tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	in.spans.Store(reqKey{e.ConnectionID, e.RequestID}, startedSpan{span: span, coll: coll})
}

type startedSpan struct {
	span trace.Span
	coll string
}

func (in *instrumentation) finished(ctx context.Context, e *event.CommandFinishedEvent, failure error) {
	v, ok := in.spans.LoadAndDelete(reqKey{e.ConnectionID, e.RequestID})
	var coll string
	var span trace.Span
	if ok {
		s := v.(startedSpan) //nolint:forcetypeassert // only startedSpan is stored.
		span, coll = s.span, s.coll
	}
	attrs := commonAttrs(e.CommandName, e.DatabaseName, coll, e.ConnectionID)
	if failure != nil {
		errType, code := classify(failure)
		errAttrs := []attribute.KeyValue{semconv.ErrorTypeKey.String(errType)}
		if code != "" {
			errAttrs = append(errAttrs, semconv.DBResponseStatusCode(code))
		}
		attrs = append(attrs, errAttrs...)
		if span != nil {
			span.SetAttributes(errAttrs...)
			otelx.RecordError(span, failure)
		}
	}
	if span != nil {
		span.End()
	}
	in.duration.Record(ctx, e.Duration.Seconds(), dbconv.SystemNameMongoDB, attrs...)
}

func (in *instrumentation) poolMonitor() *event.PoolMonitor {
	return &event.PoolMonitor{Event: func(e *event.PoolEvent) {
		ctx := context.Background()
		add := func(n int64, state dbconv.ClientConnectionStateAttr) {
			in.conns.Add(ctx, n, e.Address, state)
		}
		// Every connection is created idle, is checked out and in, and is
		// closed only after being checked back in (or while idle).
		switch e.Type {
		case event.ConnectionCreated:
			add(1, dbconv.ClientConnectionStateIdle)
		case event.ConnectionCheckedOut:
			add(-1, dbconv.ClientConnectionStateIdle)
			add(1, dbconv.ClientConnectionStateUsed)
		case event.ConnectionCheckedIn:
			add(-1, dbconv.ClientConnectionStateUsed)
			add(1, dbconv.ClientConnectionStateIdle)
		case event.ConnectionClosed:
			add(-1, dbconv.ClientConnectionStateIdle)
		}
	}}
}

func commonAttrs(op, db, coll, connID string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 6)
	attrs = append(attrs, semconv.DBOperationName(op))
	if db != "" {
		attrs = append(attrs, semconv.DBNamespace(db))
	}
	if coll != "" {
		attrs = append(attrs, semconv.DBCollectionName(coll))
	}
	if host, port := serverOf(connID); host != "" {
		attrs = append(attrs, semconv.ServerAddress(host))
		if port > 0 {
			attrs = append(attrs, semconv.ServerPort(port))
		}
	}
	return attrs
}

// collectionOf returns the collection a command targets: the string value of
// its first element (find, insert, update, delete, aggregate, ...), or the
// "collection" field of getMore.
func collectionOf(cmd string, body bson.Raw) string {
	if body == nil {
		return ""
	}
	if cmd == "getMore" {
		if v, err := body.LookupErr("collection"); err == nil {
			s, _ := v.StringValueOK()
			return s
		}
		return ""
	}
	elems, err := body.Elements()
	if err != nil || len(elems) == 0 {
		return ""
	}
	s, _ := elems[0].Value().StringValueOK()
	return s
}

// serverOf extracts host and port from a driver connection ID such as
// "db.example.com:27017[-12]".
func serverOf(connID string) (string, int) {
	if i := strings.LastIndexByte(connID, '['); i > 0 {
		connID = connID[:i]
	}
	host, p, err := net.SplitHostPort(connID)
	if err != nil {
		return "", 0
	}
	port, _ := strconv.Atoi(p)
	return host, port
}

// classify returns the error.type (server code name, "timeout",
// "network" or "_OTHER") and the server error code, if any.
func classify(err error) (string, string) {
	if ce, ok := errors.AsType[mongo.CommandError](err); ok {
		code := strconv.Itoa(int(ce.Code))
		if ce.Name != "" {
			return ce.Name, code
		}
		return code, code
	}
	if we, ok := errors.AsType[mongo.WriteException](err); ok && len(we.WriteErrors) > 0 {
		code := strconv.Itoa(we.WriteErrors[0].Code)
		return code, code
	}
	switch {
	case mongo.IsTimeout(err):
		return "timeout", ""
	case mongo.IsNetworkError(err):
		return "network", ""
	}
	return "_OTHER", ""
}
