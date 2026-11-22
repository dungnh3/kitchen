package ratelimit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"
	"strings"

	xcontext "github.com/dungnh3/kitchen/context"
)

// By names the identity a Limit counts requests against.
//
// Every request resolves to its most trusted identity, its tier: a verified
// user, else a verified anonymous subject, else an unverified device id,
// else only the client IP.
type By int

const (
	// ByCaller counts the request against its tier's identity: user, anon,
	// device or client IP. One rule for everyone, one bucket per caller.
	ByCaller By = iota
	// ByUser counts requests of verified users; other tiers skip it.
	ByUser
	// ByAnon counts requests of verified anonymous subjects (Config.Anonymous);
	// other tiers skip it.
	ByAnon
	// ByDevice counts requests that carry only an unverified device id; other
	// tiers skip it. Pair it with ByIP so rotating fake ids does not help.
	ByDevice
	// ByIP counts unauthenticated requests (device and IP tiers) by client
	// IP. Authenticated users behind a shared NAT are not affected.
	ByIP
	// ByAnyIP counts every request by client IP, authenticated or not.
	ByAnyIP
	// ByGlobal counts every request in one bucket shared by all callers.
	ByGlobal

	// byCallerDeviceIP is added for every ByCaller limit: the client IP of a
	// request whose tier is device.
	byCallerDeviceIP
)

var byNames = [...]string{"caller", "user", "anon", "device", "ip", "any_ip", "global", "caller_ip"}

func (b By) String() string {
	if b < 0 || int(b) >= len(byNames) {
		return fmt.Sprintf("By(%d)", int(b))
	}
	return byNames[b]
}

func (b By) valid() bool { return b >= ByCaller && b <= ByGlobal }

// maxIDLen bounds subject and device ids so a client cannot inflate keys.
const maxIDLen = 256

// identity is what the limiter knows about one request.
type identity struct {
	user, anon, device string // device is already hashed
	ip                 string // bucketIP form; "" when unknown
	addr               netip.Addr
}

func (l *Limiter) identify(ctx context.Context, remote string) identity {
	var id identity
	if l.anonymous != nil {
		if a, ok := l.anonymous(ctx); ok && a != "" && len(a) <= maxIDLen {
			id.anon = a
		}
	}
	if id.anon == "" {
		if u, ok := l.subject(ctx); ok && u != "" && len(u) <= maxIDLen {
			id.user = u
		}
	}
	if l.device != nil {
		if d, ok := l.device(ctx); ok && d != "" && len(d) <= maxIDLen {
			id.device = hashID(d)
		}
	}
	if a, ok := clientIP(ctx, remote); ok {
		id.addr, id.ip = a, bucketIP(a)
	}
	return id
}

func (id identity) verified() bool { return id.user != "" || id.anon != "" }

// key returns the bucket key of id for a limit of kind b, or false when the
// limit does not apply to this request.
func (id identity) key(b By) (string, bool) {
	switch b {
	case ByCaller:
		switch {
		case id.user != "":
			return "user:" + id.user, true
		case id.anon != "":
			return "anon:" + id.anon, true
		case id.device != "":
			return "device:" + id.device, true
		case id.ip != "":
			return "ip:" + id.ip, true
		}
	case ByUser:
		return "user:" + id.user, id.user != ""
	case ByAnon:
		return "anon:" + id.anon, id.anon != ""
	case ByDevice:
		return "device:" + id.device, !id.verified() && id.device != ""
	case ByIP:
		return "ip:" + id.ip, !id.verified() && id.ip != ""
	case ByAnyIP:
		return "ip:" + id.ip, id.ip != ""
	case ByGlobal:
		return "global", true
	case byCallerDeviceIP:
		return "ip:" + id.ip, !id.verified() && id.device != "" && id.ip != ""
	}
	return "", false
}

// keys returns every identity key of id, for Exempt and Overrides lookups.
func (id identity) keys() []string {
	var ks []string
	for _, k := range []struct{ prefix, v string }{{"user:", id.user}, {"anon:", id.anon}, {"device:", id.device}, {"ip:", id.ip}} {
		if k.v != "" {
			ks = append(ks, k.prefix+k.v)
		}
	}
	return ks
}

// boundKey hashes keys over maxIDLen so bucket keys stay bounded.
func boundKey(k string) string {
	if len(k) <= maxIDLen {
		return k
	}
	return "h:" + hashID(k)
}

// hashID hashes an unverified client-supplied id so keys stay bounded.
func hashID(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:12])
}

func userID(ctx context.Context) (string, bool) {
	id := xcontext.UserID(ctx)
	return id, id != ""
}

func deviceID(ctx context.Context) (string, bool) {
	info, ok := xcontext.FromContext(ctx)
	return info.DeviceID, ok && info.DeviceID != ""
}

// identityRef is a parsed "<kind>:<value>" entry of Exempt or Overrides.
type identityRef struct {
	key    string       // normalized key ("ip:1.2.3.4", "device:<hash>")
	prefix netip.Prefix // set for an "ip:" CIDR
}

// parseRef parses "user:<id>", "anon:<id>", "device:<raw id>" or
// "ip:<address or CIDR>". CIDRs are only allowed when cidr is true.
func parseRef(s string, cidr bool) (identityRef, error) {
	kind, v, ok := strings.Cut(strings.TrimSpace(s), ":")
	if !ok || v == "" {
		return identityRef{}, fmt.Errorf("%q: want \"<user|anon|device|ip>:<value>\"", s)
	}
	switch kind {
	case "user", "anon":
		return identityRef{key: kind + ":" + v}, nil
	case "device":
		return identityRef{key: "device:" + hashID(v)}, nil
	case "ip":
		if strings.Contains(v, "/") {
			if !cidr {
				return identityRef{}, fmt.Errorf("%q: a CIDR is not allowed here", s)
			}
			p, err := netip.ParsePrefix(v)
			if err != nil {
				return identityRef{}, fmt.Errorf("%q: %w", s, err)
			}
			return identityRef{prefix: p.Masked()}, nil
		}
		a, err := netip.ParseAddr(v)
		if err != nil {
			return identityRef{}, fmt.Errorf("%q: %w", s, err)
		}
		a = a.Unmap()
		// key matches the bucket (an IPv6 /64, for Overrides); prefix is the
		// exact address (for Exempt).
		return identityRef{key: "ip:" + bucketIP(a), prefix: netip.PrefixFrom(a, a.BitLen())}, nil
	}
	return identityRef{}, fmt.Errorf("%q: unknown kind %q", s, kind)
}
