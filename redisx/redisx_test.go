package redisx_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/redisx"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestOpenSingleNode(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireAuth("secret")
	rdb, err := redisx.Open(context.Background(), redisx.Config{Addrs: []string{mr.Addr()}, Password: "secret"},
		redisx.WithTracerProvider(tracenoop.NewTracerProvider()), redisx.WithMeterProvider(noop.NewMeterProvider()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rdb.Close() }()
	if _, ok := rdb.(*redis.Client); !ok {
		t.Fatalf("got %T, want *redis.Client", rdb)
	}
	if err := rdb.Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenErrors(t *testing.T) {
	if _, err := redisx.Open(context.Background(), redisx.Config{}); err == nil {
		t.Fatal("no addrs: want error")
	}
	mr := miniredis.RunT(t)
	mr.RequireAuth("secret")
	if _, err := redisx.Open(context.Background(), redisx.Config{Addrs: []string{mr.Addr()}, Password: "wrong"}, redisx.WithoutTracing()); err == nil {
		t.Fatal("bad password: want error")
	}
}

func TestUniversalOptionsModes(t *testing.T) {
	tests := []struct {
		cfg  redisx.Config
		want string
	}{
		{redisx.Config{Addrs: []string{"a:1"}}, "*redis.Client"},
		{redisx.Config{Addrs: []string{"a:1", "b:1"}}, "*redis.ClusterClient"},
		{redisx.Config{Addrs: []string{"a:1"}, Cluster: true}, "*redis.ClusterClient"},
		{redisx.Config{Addrs: []string{"s:26379"}, MasterName: "mymaster"}, "*redis.Client"},
	}
	for _, tt := range tests {
		uo, err := tt.cfg.UniversalOptions()
		if err != nil {
			t.Fatal(err)
		}
		c := redis.NewUniversalClient(uo)
		if got := fmt.Sprintf("%T", c); got != tt.want {
			t.Errorf("%+v → %s, want %s", tt.cfg, got, tt.want)
		}
		_ = c.Close()
	}
}

func ExampleOpen() {
	mr, _ := miniredis.Run()
	defer mr.Close()

	rdb, err := redisx.Open(context.Background(), redisx.Config{Addrs: []string{mr.Addr()}})
	if err != nil {
		panic(err)
	}
	defer func() { _ = rdb.Close() }()
	fmt.Println(rdb.Ping(context.Background()).Val())
	// Output: PONG
}
