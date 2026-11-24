// Package server runs a gRPC service together with its REST gateway, health
// checks and an optional admin port.
//
// A Server listens on three ports:
//
//   - gRPC (default ":10000"), native grpc-go transport;
//   - HTTP (default ":8080", HTTP/1.1 and h2c): the grpc-gateway REST mapping,
//     plain net/http handlers from [HTTPRegistrar] services and the probes
//     /livez and /readyz (no details; see [WithoutPublicProbes]);
//   - admin (off by default): /livez, /readyz with failure details,
//     /version, and opt-in pprof and metrics.
//
// The REST gateway reaches gRPC through an in-process connection, so every
// interceptor (recovery, request info, logging, errors, auth, validation)
// runs once for both REST and gRPC calls, and server-streaming RPCs work over
// REST.
//
// Every error the server returns goes through one errs.Handler
// ([WithErrors]): gRPC errors, REST gateway errors, unknown routes, panics and
// oversized bodies all become the same google.rpc.Status with an ErrorInfo
// (and a LocalizedMessage when the handler has a localizer), rendered on
// HTTP as the AIP-193 JSON envelope.
//
// The wiring of a service:
//
//	opts, _ := log.OptionsFromEnv()
//	log.Setup(opts) // the one logger of the process (server, errs, auth, ...)
//	bundle, _ := i18n.New(language.English, i18n.WithDefaults())
//	authn, _ := auth.New(auth.Config{...})
//	srv, _ := server.New(
//		server.WithErrors(errs.NewHandler(errs.Config{Localizer: bundle, Domain: "agents.example.com"})),
//		server.WithAuth(authn), // fail-closed on gRPC, REST and HTTP routes; sets the user id
//		server.WithServices(svc),
//	)
//
// Records logged with a request context carry request_id and user_id.
//
// [Server.Run] serves until its context is cancelled, then shuts down in
// order within one budget (see [WithShutdownTimeout]): mark not ready, wait
// the shutdown delay, close the [Draining] channel, drain HTTP and gRPC,
// close services. The server does not handle OS signals; use
// signal.NotifyContext in main. A Server that will not run (main failed
// after New) is released with [Server.Close].
//
// Client-sent identity metadata (xcontext.IdentityHeaders) is stripped from
// untrusted callers on both ports; see [WithTrustedIdentityPeers].
package server
