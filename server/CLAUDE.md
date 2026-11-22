# server: package rules

Every service starts through this package. A change here changes the
behaviour of every service. Read `doc.go` and `docs/design/transport.md`
first.

- **All errors go through `errs.Handler`.** That includes gRPC errors, REST
  gateway errors, unknown routes, panics and oversized bodies. Every one of
  them must render as the same AIP-193 status. A new error path must not
  write its own body or status code.
- **Interceptors run once.** REST reaches gRPC over an in-process connection,
  so recovery, request info, logging, errors, auth, validation and rate
  limiting run once for both transports. Do not add REST-only or gRPC-only
  copies of a cross-cutting concern. Keep the interceptor order stable, and
  test it.
- **Fail closed.**
  - `WithAuth` covers gRPC, REST and plain HTTP routes.
  - Request validation is on by default.
  - Identity headers from untrusted callers are stripped on both ports; see
    `WithTrustedIdentityPeers`.
  - The client IP comes only from trusted proxies.
- **Probes.**
  - Public `/livez` and `/readyz` never expose check details.
  - Details, `/version`, pprof and metrics live only on the admin port, which
    is off by default.
  - gRPC health (`grpc.health.v1`) follows the same readiness checks.
- **Shutdown order is a contract.** It runs inside one budget
  (`WithShutdownTimeout`):
  1. mark not ready;
  2. wait the shutdown delay;
  3. close `Draining`;
  4. drain HTTP and gRPC;
  5. close services.

  The package never handles OS signals; `main` uses `signal.NotifyContext`.
- **Ports.** `WithHTTPAddr("")` and `WithGRPCAddr("")` turn that port off.
  HTTP-only hosts, such as `mcpkit`, depend on this.
- **Canonical package (D7).** This is the only place that builds a
  `grpc.NewServer`, an `http.Server` or grpc-gateway. Other packages plug in
  through `WithServices` and the registrar interfaces.
