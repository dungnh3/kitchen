package webmeta

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"syscall"
)

// ErrForbiddenAddress is returned when a URL resolves to an address that is
// not publicly routable (loopback, private, link-local, metadata, ...).
var ErrForbiddenAddress = errors.New("webmeta: forbidden address")

var blockedPrefixes = func() []netip.Prefix {
	var out []netip.Prefix
	for _, s := range []string{
		"0.0.0.0/8",       // "this" network
		"100.64.0.0/10",   // CGNAT
		"192.0.0.0/24",    // IETF protocol assignments
		"192.0.2.0/24",    // TEST-NET-1
		"198.18.0.0/15",   // benchmarking
		"198.51.100.0/24", // TEST-NET-2
		"203.0.113.0/24",  // TEST-NET-3
		"240.0.0.0/4",     // reserved, incl. broadcast
		"64:ff9b::/96",    // NAT64 can reach IPv4 private space
		"64:ff9b:1::/48",  // local-use NAT64
		"2001:db8::/32",   // documentation
		"2002::/16",       // 6to4 can embed private IPv4
	} {
		out = append(out, netip.MustParsePrefix(s))
	}
	return out
}()

// forbidden reports whether ip must not be dialled.
func forbidden(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() || ip.IsInterfaceLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return true
	}
	for _, p := range blockedPrefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// dialControl runs after DNS resolution, for every address actually dialled
// (including redirects), so DNS rebinding cannot bypass it.
func dialControl(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrForbiddenAddress, address)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || forbidden(ip) {
		return fmt.Errorf("%w: %s", ErrForbiddenAddress, host)
	}
	return nil
}
