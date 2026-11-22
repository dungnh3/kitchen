package mcpkit

import (
	"net/http"
	"strings"

	"google.golang.org/grpc"
)

// Service mounts the MCP endpoint on kitchen's server package, which then
// provides the listener, timeouts (no WriteTimeout, so streams are not cut),
// graceful drain, /livez and /readyz, access logs and HTTP telemetry:
//
//	srv, _ := mcpkit.New(impl, mcpkit.WithAuth(...))
//	s, _ := server.New(
//		server.WithServices(srv.Service("/mcp")),
//		server.WithGRPCAddr(""), // HTTP only: no public gRPC port
//	)
//	err := s.Run(ctx)
//
// pattern is the endpoint path ("/mcp"); every method on it goes to
// [Server.Handler]. When [Auth.Metadata] is set, its well-known document is
// mounted at [Server.MetadataPath] too.
//
// mcpkit authenticates the endpoint itself (with RFC 9728 challenges). If
// the kitchen server also has an auth.Authenticator, list these patterns as
// public there, or leave [WithAuth] out and let the server authenticate.
func (s *Server) Service(pattern string) *Service {
	if pattern == "" || !strings.HasPrefix(pattern, "/") {
		pattern = "/" + pattern
	}
	return &Service{s: s, pattern: pattern}
}

// Service is a server.Service and server.HTTPRegistrar; see
// [Server.Service].
type Service struct {
	s       *Server
	pattern string
}

// RegisterGRPC registers nothing: MCP is served over HTTP only.
func (*Service) RegisterGRPC(grpc.ServiceRegistrar) {}

// RegisterHTTP mounts the MCP endpoint and, when configured, the
// protected-resource metadata.
func (v *Service) RegisterHTTP(mux *http.ServeMux) {
	mux.Handle(v.pattern, v.s.Handler())
	if v.s.metaHandler != nil {
		mux.Handle("GET "+v.s.metaPath, v.s.metaHandler)
	}
}

// Patterns returns the mounted patterns, e.g. to list them as public in a
// kitchen auth.Authenticator.
func (v *Service) Patterns() []string {
	p := []string{v.pattern}
	if v.s.metaHandler != nil {
		p = append(p, "GET "+v.s.metaPath)
	}
	return p
}
