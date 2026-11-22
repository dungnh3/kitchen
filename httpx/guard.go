package httpx

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/dungnh3/kitchen/errs"
)

// OriginGuard is a RequestHook that refuses an absolute request URL whose
// origin (scheme, host, port) differs from the client's base URL, so
// client-level credentials and headers never go to a host the client was
// not built for. Clients without a base URL are not restricted. New
// installs it first unless WithoutOriginGuard is given.
func OriginGuard(r *Request) error {
	b := r.c.base
	if b == nil {
		return nil
	}
	u, err := url.Parse(r.path)
	if err != nil || !u.IsAbs() {
		return nil // relative, or invalid (reported when sent)
	}
	if !sameOrigin(u, b) {
		return errs.Wrap(fmt.Errorf("httpx: %s is not on base origin %s", RedactURL(u), b.Scheme+"://"+b.Host),
			ErrDestinationNotAllowed)
	}
	return nil
}

func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(hostPort(a), hostPort(b))
}

func hostPort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return u.Host
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return u.Hostname() + ":443"
	case "http":
		return u.Hostname() + ":80"
	}
	return u.Host
}

// redirectHeaderAllow lists the headers kept on a cross-origin redirect.
var redirectHeaderAllow = []string{
	"Accept", "Accept-Encoding", "Accept-Language", "Content-Type", "Content-Length", "User-Agent",
}

// SafeRedirectPolicy follows at most maxRedirects redirects, refuses a downgrade
// from https to http, and on a redirect that leaves the origin of the first
// request drops every header except Accept*, Content-Type, Content-Length
// and User-Agent (trace headers are injected again by the transport). It is
// the default of New.
func SafeRedirectPolicy(maxRedirects int) RedirectPolicy {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return fmt.Errorf("httpx: stopped after %d redirects", maxRedirects)
		}
		if strings.EqualFold(via[len(via)-1].URL.Scheme, "https") && !strings.EqualFold(req.URL.Scheme, "https") {
			return errs.Wrap(fmt.Errorf("httpx: redirect from https to %s refused", RedactURL(req.URL)), ErrDestinationNotAllowed)
		}
		if !sameOrigin(req.URL, via[0].URL) {
			for k := range req.Header {
				if !slices.Contains(redirectHeaderAllow, k) {
					delete(req.Header, k)
				}
			}
		}
		return nil
	}
}

// NoRedirects is a RedirectPolicy that never follows a redirect: the 3xx
// response is returned as is.
func NoRedirects(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// SSRFConfig configures the SSRF guard.
type SSRFConfig struct {
	// Ports allowed as destination. Default 80 and 443.
	Ports []int
	// Allow lists prefixes that are reachable even though they are not
	// public (for example a known internal service). Default none.
	Allow []netip.Prefix
}

// nonPublic lists special-purpose ranges (RFC 6890 and successors) that the
// SSRF guard refuses on top of loopback, private, link-local, multicast and
// unspecified addresses.
var nonPublic = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // CGNAT, also some cloud metadata
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),   // NAT64 can reach any IPv4
	netip.MustParsePrefix("64:ff9b:1::/48"), // local-use NAT64
	netip.MustParsePrefix("2001::/32"),      // Teredo
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"), // 6to4
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("fec0::/10"),
}

// PublicAddr reports whether ip is a public unicast address.
func PublicAddr(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false // also covers loopback, link-local, multicast, unspecified
	}
	for _, p := range nonPublic {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

// SSRFControl returns a net.Dialer Control function that refuses
// connections to addresses that are not public (see PublicAddr) or not on
// an allowed port. It runs after DNS resolution, for every connection
// (redirects included), so DNS rebinding cannot bypass it. The error wraps
// ErrDestinationNotAllowed.
func SSRFControl(cfg SSRFConfig) func(network, address string, c syscall.RawConn) error {
	ports := cfg.Ports
	if len(ports) == 0 {
		ports = []int{80, 443}
	}
	return func(network, address string, _ syscall.RawConn) error {
		if !strings.HasPrefix(network, "tcp") {
			return blocked(address, "network "+network)
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return blocked(address, err.Error())
		}
		p, err := strconv.Atoi(port)
		if err != nil || !slices.Contains(ports, p) {
			return blocked(address, "port not allowed")
		}
		ip, err := netip.ParseAddr(host)
		if err != nil {
			return blocked(address, err.Error())
		}
		ip = ip.Unmap()
		for _, a := range cfg.Allow {
			if a.Contains(ip) {
				return nil
			}
		}
		if !PublicAddr(ip) {
			return blocked(address, "address is not public")
		}
		return nil
	}
}

func blocked(address, why string) error {
	return errs.Wrap(errors.New("httpx: dial "+address+": "+why), ErrDestinationNotAllowed)
}
