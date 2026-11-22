package context

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/textproto"
	"strings"

	"github.com/realclientip/realclientip-go"
)

// TrustedProxies resolves the real client IP behind a chain of proxies that
// append to X-Forwarded-For. Only addresses inside the configured prefixes
// are trusted to report a client address. A nil *TrustedProxies trusts
// nothing.
//
// List every hop between the client and the server, otherwise the client IP
// is the address of the first unlisted hop. Behind Cloudflare and a GCP
// external Application Load Balancer that is:
//
//   - the Google Front End ranges that connect to backends
//     (35.191.0.0/16, 130.211.0.0/22),
//   - the load balancer's own IP address (it appends "<client>,<lb-ip>"),
//   - the Cloudflare ranges (https://www.cloudflare.com/ips/),
//   - any in-cluster ingress or proxy.
//
// CF-Connecting-IP is deliberately not used: a client that reaches the load
// balancer directly can set it. With the Cloudflare ranges listed, the
// X-Forwarded-For walk gives the same answer and cannot be spoofed.
//
// The walk is realclientip-go's RightmostTrustedRangeStrategy (the reference
// implementation of "the perils of the real client IP"), wrapped with
// kitchen's netip API and used the same way for HTTP requests and gRPC
// metadata.
type TrustedProxies struct {
	prefixes []netip.Prefix
	header   string
	strategy realclientip.RightmostTrustedRangeStrategy
}

// NewTrustedProxies parses CIDRs ("10.0.0.0/8") or single addresses of
// proxies that append to X-Forwarded-For.
func NewTrustedProxies(cidrs ...string) (*TrustedProxies, error) {
	return NewTrustedProxiesHeader(HeaderForwardedFor, cidrs...)
}

// NewTrustedProxiesHeader is NewTrustedProxies for proxies that append to
// header instead: "X-Forwarded-For" or "Forwarded" (RFC 7239). Use the header
// your proxies actually write: a header they pass through untouched is
// client-controlled.
func NewTrustedProxiesHeader(header string, cidrs ...string) (*TrustedProxies, error) {
	t := &TrustedProxies{header: textproto.CanonicalMIMEHeaderKey(header)}
	nets := make([]net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		var p netip.Prefix
		if !strings.Contains(c, "/") {
			a, err := netip.ParseAddr(c)
			if err != nil {
				return nil, fmt.Errorf("context: trusted proxy %q: %w", c, err)
			}
			p = netip.PrefixFrom(a.Unmap(), a.Unmap().BitLen())
		} else {
			var err error
			if p, err = netip.ParsePrefix(c); err != nil {
				return nil, fmt.Errorf("context: trusted proxy %q: %w", c, err)
			}
			p = p.Masked()
		}
		t.prefixes = append(t.prefixes, p)
		nets = append(nets, net.IPNet{IP: p.Addr().AsSlice(), Mask: net.CIDRMask(p.Bits(), p.Addr().BitLen())})
	}
	var err error
	if t.strategy, err = realclientip.NewRightmostTrustedRangeStrategy(t.header, nets); err != nil {
		return nil, fmt.Errorf("context: trusted proxies: %w", err)
	}
	return t, nil
}

// Header returns the forwarding header the proxies append to.
func (t *TrustedProxies) Header() string {
	if t == nil {
		return HeaderForwardedFor
	}
	return t.header
}

// Contains reports whether addr is a trusted proxy.
func (t *TrustedProxies) Contains(addr netip.Addr) bool {
	if t == nil || !addr.IsValid() {
		return false
	}
	addr = addr.Unmap()
	for _, p := range t.prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// ClientIP returns the client address for a connection from remote that
// carried the given values of the forwarding header (Header).
//
// When remote is not a trusted proxy, remote is the client. Otherwise the
// header is walked from the right, skipping trusted proxies, and the first
// untrusted address is the client. When that entry is malformed or every
// entry is trusted, remote is returned.
func (t *TrustedProxies) ClientIP(remote netip.Addr, values []string) netip.Addr {
	remote = remote.Unmap()
	if !t.Contains(remote) || len(values) == 0 {
		return remote
	}
	ip := t.strategy.ClientIP(http.Header{t.header: values}, "")
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return remote
	}
	return a.WithZone("").Unmap()
}

func (t *TrustedProxies) clientIPFromRequest(r *http.Request) netip.Addr {
	return t.ClientIP(AddrFromHostPort(r.RemoteAddr), r.Header.Values(t.Header()))
}

// AddrFromHostPort parses "host:port", "[v6]:port" or a bare address (as in
// http.Request.RemoteAddr or a gRPC peer address); it returns the zero Addr
// when s is none of them.
func AddrFromHostPort(s string) netip.Addr {
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return ap.Addr().Unmap()
	}
	if host, _, err := net.SplitHostPort(s); err == nil {
		s = host
	}
	a, err := netip.ParseAddr(strings.Trim(s, "[]"))
	if err != nil {
		return netip.Addr{}
	}
	return a.Unmap()
}
