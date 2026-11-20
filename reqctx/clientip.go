package reqctx

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// TrustedProxies resolves the real client IP behind a chain of proxies that
// append to X-Forwarded-For. Only addresses inside the configured prefixes
// are trusted to report a client address. A nil *TrustedProxies trusts
// nothing.
type TrustedProxies struct {
	prefixes []netip.Prefix
}

// NewTrustedProxies parses CIDRs ("10.0.0.0/8") or single addresses.
func NewTrustedProxies(cidrs ...string) (*TrustedProxies, error) {
	t := &TrustedProxies{}
	for _, c := range cidrs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if !strings.Contains(c, "/") {
			a, err := netip.ParseAddr(c)
			if err != nil {
				return nil, fmt.Errorf("reqctx: trusted proxy %q: %w", c, err)
			}
			t.prefixes = append(t.prefixes, netip.PrefixFrom(a.Unmap(), a.Unmap().BitLen()))
			continue
		}
		p, err := netip.ParsePrefix(c)
		if err != nil {
			return nil, fmt.Errorf("reqctx: trusted proxy %q: %w", c, err)
		}
		t.prefixes = append(t.prefixes, p.Masked())
	}
	return t, nil
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
// carried the given X-Forwarded-For values and CF-Connecting-IP value.
//
// When remote is not trusted, remote is the client. Otherwise
// CF-Connecting-IP is used when valid; else X-Forwarded-For is walked from
// the right, skipping trusted proxies, and the first untrusted address is
// the client.
func (t *TrustedProxies) ClientIP(remote netip.Addr, xff []string, cfConnectingIP string) netip.Addr {
	remote = remote.Unmap()
	if !t.Contains(remote) {
		return remote
	}
	if a, err := netip.ParseAddr(strings.TrimSpace(cfConnectingIP)); err == nil {
		return a.Unmap()
	}
	var hops []netip.Addr
	for _, v := range xff {
		for part := range strings.SplitSeq(v, ",") {
			a, err := netip.ParseAddr(strings.TrimSpace(part))
			if err != nil {
				// A malformed entry breaks the chain: nothing left of it
				// can be trusted.
				hops = hops[:0]
				continue
			}
			hops = append(hops, a.Unmap())
		}
	}
	for i := len(hops) - 1; i >= 0; i-- {
		if !t.Contains(hops[i]) {
			return hops[i]
		}
	}
	if len(hops) > 0 {
		return hops[0]
	}
	return remote
}

// ClientIPFromRequest applies ClientIP to an HTTP request.
func (t *TrustedProxies) ClientIPFromRequest(r *http.Request) netip.Addr {
	return t.ClientIP(AddrFromString(r.RemoteAddr), r.Header.Values(HeaderForwardedFor), r.Header.Get(HeaderCFConnectingIP))
}

// AddrFromString parses "host:port" or a bare address; it returns the zero
// Addr when s is neither.
func AddrFromString(s string) netip.Addr {
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
