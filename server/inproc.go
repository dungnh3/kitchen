package server

import (
	"context"
	"net"

	"google.golang.org/grpc/test/bufconn"
)

// inprocNetwork names the in-process gateway connection. Peers on it are the
// gateway, which has already sanitised the HTTP request.
const inprocNetwork = "kitchen-inproc"

type inprocAddr struct{}

func (inprocAddr) Network() string { return inprocNetwork }
func (inprocAddr) String() string  { return inprocNetwork }

// inprocListener is an in-memory listener whose connections report
// inprocAddr as their addresses.
type inprocListener struct {
	*bufconn.Listener
}

func newInprocListener() *inprocListener {
	return &inprocListener{Listener: bufconn.Listen(1 << 20)}
}

func (l *inprocListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return inprocConn{c}, nil
}

func (l *inprocListener) Addr() net.Addr { return inprocAddr{} }

func (l *inprocListener) dial(ctx context.Context, _ string) (net.Conn, error) {
	c, err := l.DialContext(ctx)
	if err != nil {
		return nil, err
	}
	return inprocConn{c}, nil
}

type inprocConn struct{ net.Conn }

func (inprocConn) LocalAddr() net.Addr  { return inprocAddr{} }
func (inprocConn) RemoteAddr() net.Addr { return inprocAddr{} }
