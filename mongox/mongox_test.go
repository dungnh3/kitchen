package mongox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/mongox"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestClientOptionsDefaults(t *testing.T) {
	co, err := mongox.ClientOptions(mongox.Config{URI: "mongodb://localhost:27017", AppName: "svc", MaxPoolSize: 20})
	if err != nil {
		t.Fatal(err)
	}
	if *co.AppName != "svc" || *co.MaxPoolSize != 20 || *co.ConnectTimeout != 5*time.Second ||
		*co.ServerSelectionTimeout != 5*time.Second || *co.MaxConnIdleTime != 5*time.Minute || co.Timeout != nil {
		t.Fatalf("options = %+v", co)
	}
}

func TestClientOptionsErrors(t *testing.T) {
	for i, cfg := range []mongox.Config{
		{},
		{URI: "http://nope"},
		{URI: "mongodb://localhost", MinPoolSize: 10, MaxPoolSize: 5},
	} {
		if _, err := mongox.ClientOptions(cfg); err == nil {
			t.Errorf("case %d: want error", i)
		}
	}
}

func TestOpenUnreachable(t *testing.T) {
	_, err := mongox.Open(context.Background(), mongox.Config{URI: "mongodb://127.0.0.1:1", ServerSelectionTimeout: 300 * time.Millisecond})
	if err == nil || !strings.Contains(err.Error(), "mongox:") {
		t.Fatalf("err = %v", err)
	}
	if !errors.Is(mongox.MapError(err), errs.ErrUnavailable) {
		t.Fatalf("unreachable → %v (code %v)", err, errs.Code(mongox.MapError(err)))
	}
}

func TestMapError(t *testing.T) {
	for _, tt := range []struct {
		in   error
		want *errs.Error
	}{
		{fmt.Errorf("get: %w", mongo.ErrNoDocuments), errs.ErrNotFound},
		{mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "E11000 dup key: { email: \"a@b\" }"}}}, errs.ErrAlreadyExists},
		{mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 121}}}, errs.ErrFailedPrecondition},
		{mongo.CommandError{Code: 112, Name: "WriteConflict"}, errs.ErrAborted},
		{mongo.CommandError{Code: 251, Labels: []string{"TransientTransactionError"}}, errs.ErrAborted},
		{mongo.CommandError{Code: 50, Name: "MaxTimeMSExpired"}, errs.ErrDeadlineExceeded},
		{mongo.CommandError{Code: 10107, Name: "NotWritablePrimary"}, errs.ErrUnavailable},
		{mongo.CommandError{Code: 6, Labels: []string{"NetworkError"}}, errs.ErrUnavailable},
		{mongo.ErrClientDisconnected, errs.ErrUnavailable},
	} {
		got := mongox.MapError(tt.in)
		if !errors.Is(got, tt.want) {
			t.Errorf("%v → %v, want %v", tt.in, got, tt.want)
		}
	}
	for _, in := range []error{fmt.Errorf("op: %w", mongo.CommandError{Code: 2}), context.Canceled, errors.New("boom"), errs.ErrInternal.With()} {
		if got := mongox.MapError(in); got != in { //nolint:errorlint // identity: unchanged.
			t.Errorf("%v changed to %v", in, got)
		}
	}
	st, _ := errs.ToStatus(mongox.MapError(mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "E11000 dup key: { email: \"a@b\" }"}}}))
	if strings.Contains(st.Message(), "a@b") {
		t.Fatalf("key value leaked: %q", st.Message())
	}
}

func TestOpenIntegration(t *testing.T) {
	if testing.Short() || os.Getenv("KITCHEN_SKIP_DOCKER") != "" {
		t.Skip("docker tests disabled")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	c, err := tcmongo.Run(ctx, "mongo:8")
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	client, err := mongox.Open(ctx, mongox.Config{URI: uri}, mongox.WithTracerProvider(tp), mongox.WithMeterProvider(mp))
	if err != nil {
		t.Fatal(err)
	}
	coll := client.Database("app").Collection("users")
	if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "email", Value: 1}}, Options: nil}); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@b"}}); err != nil {
		t.Fatal(err)
	}
	_, err = coll.InsertOne(ctx, bson.D{{Key: "_id", Value: "u1"}})
	if !errors.Is(mongox.MapError(err), errs.ErrAlreadyExists) {
		t.Fatalf("duplicate: %v", err)
	}
	err = coll.FindOne(ctx, bson.D{{Key: "_id", Value: "none"}}).Err()
	if !errors.Is(mongox.MapError(err), errs.ErrNotFound) {
		t.Fatalf("not found: %v", err)
	}
	if err := client.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}

	var insert, find bool
	for _, s := range sr.Ended() {
		attrs := attribute.NewSet(s.Attributes()...)
		if v, _ := attrs.Value("db.collection.name"); v.AsString() != "users" {
			continue
		}
		for _, kv := range s.Attributes() {
			if kv.Key == "db.query.text" {
				t.Fatalf("query text recorded on %q", s.Name())
			}
		}
		switch s.Name() {
		case "insert users":
			insert = true
		case "find users":
			find = true
		}
	}
	if !insert || !find {
		t.Fatalf("spans: insert %v find %v (%d ended)", insert, find, len(sr.Ended()))
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}
	if !names["db.client.operation.duration"] || !names["db.client.connection.count"] {
		t.Fatalf("metrics = %v", names)
	}
	_ = tp.Shutdown(ctx)
	_ = mp.Shutdown(ctx)
}
