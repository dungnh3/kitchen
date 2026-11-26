// Package grpcx is kitchen's client for calling other services over gRPC.
//
// Build one [Conn] per downstream service with [New] and share it; it
// implements grpc.ClientConnInterface, so generated stubs take it as is:
//
//	conn, err := grpcx.New("dns:///billing.internal:443", grpcx.WithLogger(logger))
//	if err != nil { ... }
//	defer conn.Close(ctx)
//	billing := billingv1.NewBillingServiceClient(conn)
//
// New never dials: the connection is made on the first call, so a service
// starts while its downstream is down.
//
// Safe defaults, each replaceable with an option:
//
//   - TLS 1.2+ with the system roots. Plaintext needs [WithInsecure], and
//     credentials ([WithTokenSource], [WithForwardAuthorization]) are
//     refused over plaintext.
//   - round_robin load balancing over every address the resolver returns
//     (use a dns:/// target and a headless Kubernetes service).
//   - Retries of UNAVAILABLE for every method through gRPC's service config,
//     with backoff and retry throttling ([RetryPolicy], [WithoutRetry]).
//   - A 10s deadline on unary calls whose context has none ([WithTimeout]).
//   - Keepalive pings while calls are active, accepted by kitchen's server
//     ([WithKeepalive]).
//   - 4 MiB messages in both directions, as kitchen's server.
//   - Downstream statuses become [errs] errors ([DefaultErrorMapper]):
//     NOT_FOUND stays NotFound, timeouts become errs.ErrUpstreamTimeout,
//     UNAVAILABLE and RESOURCE_EXHAUSTED become errs.ErrDependencyUnavailable
//     and every other code errs.ErrInternal. The downstream message never
//     reaches our own clients; it stays in the [*StatusError] cause.
//   - Request values (request id, user id, ...) are propagated with
//     kitchen/context, and calls are traced through otelx.GRPCClientHandler.
//
// A circuit breaker is opt-in ([WithCircuitBreaker]). Package grpcxtest
// connects a Conn to an in-memory server for tests.
package grpcx
