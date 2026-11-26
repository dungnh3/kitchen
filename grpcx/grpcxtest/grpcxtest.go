// Package grpcxtest connects a grpcx.Conn to an in-memory gRPC server
// (bufconn), so tests exercise the real client with every kitchen default
// and no network:
//
//	srv := grpcxtest.NewServer(t, func(r grpc.ServiceRegistrar) {
//		billingv1.RegisterBillingServiceServer(r, fake)
//	})
//	conn := srv.Conn(t) // plaintext; NewTLSServer for TLS and credentials
//	client := billingv1.NewBillingServiceClient(conn)
//
// The server and the connections are closed with t.Cleanup.
package grpcxtest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dungnh3/kitchen/grpcx"
)

// ServerName is the TLS server name of a NewTLSServer server.
const ServerName = "grpcxtest.local"

const bufSize = 1 << 20

// Server is a gRPC server on an in-memory listener.
type Server struct {
	lis   *bufconn.Listener
	roots *x509.CertPool
}

// NewServer starts a plaintext server with the services that register adds.
// opts are passed to grpc.NewServer (interceptors, for example).
func NewServer(t testing.TB, register func(grpc.ServiceRegistrar), opts ...grpc.ServerOption) *Server {
	t.Helper()
	return start(t, register, nil, opts)
}

// NewTLSServer is NewServer with TLS, using a certificate for ServerName
// made for the test. Its Conn trusts that certificate, so credentials
// (grpcx.WithTokenSource, grpcx.WithForwardAuthorization) can be tested.
func NewTLSServer(t testing.TB, register func(grpc.ServiceRegistrar), opts ...grpc.ServerOption) *Server {
	t.Helper()
	cert, roots, err := selfSigned()
	if err != nil {
		t.Fatalf("grpcxtest: certificate: %v", err)
	}
	creds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	s := start(t, register, roots, append([]grpc.ServerOption{grpc.Creds(creds)}, opts...))
	return s
}

func start(t testing.TB, register func(grpc.ServiceRegistrar), roots *x509.CertPool, opts []grpc.ServerOption) *Server {
	t.Helper()
	s := &Server{lis: bufconn.Listen(bufSize), roots: roots}
	srv := grpc.NewServer(opts...)
	if register != nil {
		register(srv)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(s.lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	return s
}

// DialOption connects a client to the server's in-memory listener; pass it
// to grpcx.WithDialOptions with the target "passthrough:///bufnet".
func (s *Server) DialOption() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return s.lis.DialContext(ctx)
	})
}

// Conn returns a grpcx.Conn to the server, built with opts. It adds the
// in-memory dialer and, for a NewServer server, grpcx.WithInsecure; for a
// NewTLSServer server, a TLS configuration that trusts its certificate.
// It is closed with t.Cleanup.
func (s *Server) Conn(t testing.TB, opts ...grpcx.Option) *grpcx.Conn {
	t.Helper()
	base := []grpcx.Option{grpcx.WithDialOptions(s.DialOption())}
	if s.roots == nil {
		base = append(base, grpcx.WithInsecure())
	} else {
		base = append(base, grpcx.WithTLSConfig(&tls.Config{RootCAs: s.roots, ServerName: ServerName, MinVersion: tls.VersionTLS12}))
	}
	conn, err := grpcx.New("passthrough:///bufnet", append(base, opts...)...)
	if err != nil {
		t.Fatalf("grpcxtest: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// selfSigned returns a certificate for ServerName and a pool that trusts
// it.
func selfSigned() (tls.Certificate, *x509.CertPool, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: ServerName},
		DNSNames:              []string{ServerName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, roots, nil
}
