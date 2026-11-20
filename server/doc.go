// Package server runs a gRPC service together with its REST gateway, health
// checks and an optional admin port.
//
// A Server listens on three ports:
//
//   - gRPC (default ":10000"), native grpc-go transport;
//   - HTTP (default ":8080", HTTP/1.1 and h2c): the grpc-gateway REST mapping,
//     plain net/http handlers from [HTTPRegistrar] services and the legacy
//     probe paths /health, /ready, /api/status and /version;
//   - admin (off by default): /livez, /readyz, and opt-in pprof and metrics.
//
// The REST gateway reaches gRPC through an in-process connection, so every
// interceptor (recovery, request info, logging, auth, validation) runs once
// for both REST and gRPC calls, and server-streaming RPCs work over REST.
//
// [Server.Run] serves until its context is cancelled, then shuts down in
// order within one budget (see [WithShutdownTimeout]): mark not ready, wait
// the shutdown delay, close the [Draining] channel, drain HTTP and gRPC,
// close services. The server does not handle OS signals; use
// signal.NotifyContext in main.
//
// Client-sent identity metadata (reqctx.IdentityHeaders) is stripped from
// untrusted callers on both ports; see [WithTrustedIdentityPeers].
package server
