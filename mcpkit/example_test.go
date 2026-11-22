package mcpkit_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	kauth "github.com/dungnh3/kitchen/auth"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/mcpkit"
	"github.com/dungnh3/kitchen/server"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"google.golang.org/grpc/codes"
)

// Serve one typed tool over Streamable HTTP on kitchen's server, HTTP only.
func Example() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts, _ := log.OptionsFromEnv() // LOG_LEVEL, LOG_FORMAT, LOG_OUTPUT, ...
	logger, _ := log.Setup(opts)

	srv, err := mcpkit.New(&mcp.Implementation{Name: "greeter", Version: "v1.0.0"},
		mcpkit.WithLogger(logger))
	if err != nil {
		logger.ErrorContext(ctx, "mcp server", log.Err(err))
		return
	}

	type GreetIn struct {
		Name string `json:"name" jsonschema:"the name of the person to greet"`
	}
	type GreetOut struct {
		Greeting string `json:"greeting"`
	}
	mcpkit.AddTool(srv, &mcp.Tool{Name: "greet", Description: "Greet someone by name."},
		func(_ context.Context, _ *mcp.CallToolRequest, in GreetIn) (*mcp.CallToolResult, GreetOut, error) {
			return nil, GreetOut{Greeting: "Hello, " + in.Name + "!"}, nil
		})

	hs, err := server.New(
		server.WithServices(srv.Service("/mcp")),
		server.WithGRPCAddr(""), // HTTP only: no public gRPC port
		server.WithLogger(logger),
	)
	if err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
		return
	}
	if err := hs.Run(ctx); err != nil {
		logger.ErrorContext(ctx, "server stopped", log.Err(err))
	}
}

func ExampleAddTool() {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "greeter"})
	if err != nil {
		panic(err)
	}
	type In struct {
		Name string `json:"name"`
	}
	type Out struct {
		Message string `json:"message"`
	}
	mcpkit.AddTool(srv, &mcp.Tool{Name: "greet"},
		func(_ context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
			return nil, Out{Message: "hello " + in.Name}, nil
		})

	// Call it over an in-memory transport (mcpkittest.Connect does this in tests).
	ctx, cancel := context.WithCancel(context.Background())
	serverT, clientT := mcp.NewInMemoryTransports()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, serverT, nil) }()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "client"}, nil).Connect(ctx, clientT, nil)
	if err != nil {
		panic(err)
	}
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "greet", Arguments: map[string]any{"name": "ana"}})
	if err != nil {
		panic(err)
	}
	fmt.Println(res.Content[0].(*mcp.TextContent).Text)
	_ = cs.Close()
	cancel()
	if err := <-done; err != nil {
		panic(err)
	}
	// Output: {"message":"hello ana"}
}

// Only *errs.Error messages with a client-fault code reach the model.
// Anything else is logged with the tool name and shown as "internal error".
func ExampleAddTool_errors() {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "calendar"})
	if err != nil {
		panic(err)
	}
	type In struct {
		Date string `json:"date" jsonschema:"day to book, YYYY-MM-DD"`
	}
	book := func(context.Context, time.Time) error { return errors.New("pq: connection refused") }

	mcpkit.AddTool(srv, &mcp.Tool{Name: "book_day"},
		func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
			day, err := time.Parse(time.DateOnly, in.Date)
			if err != nil {
				// The model reads this message and can retry with a fixed argument.
				return nil, nil, errs.New(codes.InvalidArgument, "BAD_DATE",
					errs.Message("date must be YYYY-MM-DD"))
			}
			if err := book(ctx, day); err != nil {
				// Logged with the tool name; the model sees "internal error".
				return nil, nil, fmt.Errorf("book %s: %w", in.Date, err)
			}
			return nil, map[string]string{"booked": in.Date}, nil
		})
}

// A default deadline for every tool, overridden per tool. Handlers must
// honour ctx for the deadline to take effect.
func ExampleTimeout() {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "reports"},
		mcpkit.WithToolTimeout(30*time.Second)) // default for every tool
	if err != nil {
		panic(err)
	}
	buildReport := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() } // slow work

	mcpkit.AddTool(srv, &mcp.Tool{Name: "build_report"},
		func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
			return nil, nil, buildReport(ctx) // must honour ctx
		},
		mcpkit.Timeout(5*time.Minute)) // this tool may take longer
}

// stdio: stdout carries JSON-RPC, so logs go to stderr and the upstream
// credential comes from the environment.
func ExampleServer_RunStdio() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts, _ := log.OptionsFromEnv() // LOG_OUTPUT=stderr also works
	if opts.Output == nil {
		opts.Output = os.Stderr // never stdout in stdio mode
	}
	logger, _ := log.Setup(opts)

	srv, err := mcpkit.New(&mcp.Implementation{Name: "slack"}, mcpkit.WithLogger(logger))
	if err != nil {
		logger.ErrorContext(ctx, "mcp server", log.Err(err))
		return
	}
	// The token is read once at start and returned by mcpkit.TokenFrom in tools.
	if err := srv.RunStdio(ctx, mcpkit.EnvToken("SLACK_TOKEN")); err != nil {
		logger.ErrorContext(ctx, "stdio", log.Err(err))
	}
}

// Verify tokens from your own IdP with kitchen's auth package, read the
// caller in tools, and serve on kitchen's server with one errs handler.
func ExampleVerifier() {
	ctx := context.Background()
	logger := log.Default()

	jwks, err := kauth.NewJWKS(ctx, kauth.JWKSConfig{
		Issuer: "https://idp.example.com",
		URL:    "https://idp.example.com/.well-known/jwks.json",
		Types:  []string{"at+jwt"},
	})
	if err != nil {
		logger.ErrorContext(ctx, "jwks", log.Err(err))
		return
	}
	defer func() { _ = jwks.Close(ctx) }()

	v, err := mcpkit.Verifier(jwks, "https://mcp.example.com/mcp") // aud must name this server
	if err != nil {
		logger.ErrorContext(ctx, "verifier", log.Err(err))
		return
	}

	eh := errs.NewHandler(errs.Config{Logger: logger, Domain: "mcp.example.com"})
	srv, err := mcpkit.New(&mcp.Implementation{Name: "agent-tools"},
		mcpkit.WithLogger(logger),
		mcpkit.WithErrors(eh),
		mcpkit.WithAuth(mcpkit.Auth{
			Verifier: v,
			Scopes:   []string{"tools.call"},
			Metadata: &oauthex.ProtectedResourceMetadata{
				Resource:             "https://mcp.example.com/mcp",
				AuthorizationServers: []string{"https://idp.example.com"},
				ScopesSupported:      []string{"tools.call"},
			},
		}))
	if err != nil {
		logger.ErrorContext(ctx, "mcp server", log.Err(err))
		return
	}

	mcpkit.AddTool(srv, &mcp.Tool{Name: "whoami"},
		func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
			p, ok := kauth.FromContext(ctx) // set by mcpkit.Verifier
			if !ok {
				return nil, nil, errs.ErrTokenMissing
			}
			return nil, map[string]string{"subject": p.Subject}, nil
		})

	hs, err := server.New(server.WithServices(srv.Service("/mcp")), server.WithGRPCAddr(""),
		server.WithErrors(eh), server.WithLogger(logger)) // same errs handler
	if err == nil {
		err = hs.Run(ctx)
	}
	if err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
	}
}

// Accept an upstream API's OAuth token (Google here) and forward it from
// the tool; the upstream rejects bad tokens.
func ExamplePassThrough() {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "mail"},
		mcpkit.WithAuth(mcpkit.Auth{
			Verifier:               mcpkit.PassThrough(),
			AllowMissingExpiration: true,
			Metadata: &oauthex.ProtectedResourceMetadata{
				Resource:             "https://mcp.example.com/mcp",
				AuthorizationServers: []string{"https://accounts.google.com"},
			},
		}))
	if err != nil {
		panic(err)
	}
	listMail := func(context.Context, string) ([]string, error) { return []string{"m1"}, nil } // Gmail API via httpx

	mcpkit.AddTool(srv, &mcp.Tool{Name: "list_mail"},
		func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
			ids, err := listMail(ctx, mcpkit.TokenFrom(ctx)) // the caller's Google token
			return nil, map[string][]string{"ids": ids}, err
		})

	fmt.Println(srv.Service("/mcp").Patterns())
	// Output: [/mcp GET /.well-known/oauth-protected-resource/mcp]
}

// Mount the handler on your own mux when kitchen's server is not an option.
func ExampleServer_Handler() {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "notes"})
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/mcp", srv.Handler())
	if p := srv.MetadataPath(); p != "" {
		mux.Handle("GET "+p, srv.MetadataHandler())
	}
	// No WriteTimeout: it would cut long-running streamed tool responses.
	hs := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	_ = hs.ListenAndServe()
}

// Hardening options for browser clients, large payloads and sessions.
func ExampleNew_http() {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "notes"},
		mcpkit.WithAllowedOrigins("https://app.example.com"), // CORS allow-list for browser clients
		mcpkit.WithMaxRequestBodyBytes(1<<20),                // 1 MiB; default 4 MiB
		mcpkit.WithStateful(15*time.Minute),                  // sessions; default stateless
	)
	if err != nil {
		panic(err)
	}
	_ = srv.Service("/mcp")
}

// Receiving middleware runs inside mcpkit's telemetry and panic recovery.
func ExampleWithMiddleware() {
	logger := log.Default()
	audit := func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if r, ok := req.(*mcp.CallToolRequest); ok {
				logger.InfoContext(ctx, "tool called", log.String("tool", r.Params.Name))
			}
			return next(ctx, method, req)
		}
	}
	srv, err := mcpkit.New(&mcp.Implementation{Name: "notes"}, mcpkit.WithMiddleware(audit))
	if err != nil {
		panic(err)
	}
	_ = srv
}

// Use the SDK for features mcpkit does not wrap: resources, prompts,
// completions, notifications.
func ExampleServer_SDK() {
	srv, err := mcpkit.New(&mcp.Implementation{Name: "notes"})
	if err != nil {
		panic(err)
	}
	srv.SDK().AddResource(&mcp.Resource{URI: "notes://readme", Name: "readme", MIMEType: "text/plain"},
		func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{
				{URI: req.Params.URI, MIMEType: "text/plain", Text: "Start here."},
			}}, nil
		})
}
