package server

import (
	"context"
	"net/netip"
	"time"

	"github.com/dungnh3/kitchen/log"
)

// identityWarnEvery bounds how often stripped identity metadata is logged.
const identityWarnEvery = time.Minute

// warnStrippedIdentity logs, at most once per identityWarnEvery, that
// identity metadata from an untrusted peer was dropped. Without it a
// service-to-service caller that is missing from WithTrustedIdentityPeers
// would silently see an empty xcontext.UserID.
func (s *Server) warnStrippedIdentity(ctx context.Context, transport string, peer netip.Addr, key string) {
	now := time.Now().UnixNano()
	last := s.identityWarnAt.Load()
	if last != 0 && now-last < int64(identityWarnEvery) {
		return
	}
	if !s.identityWarnAt.CompareAndSwap(last, now) {
		return
	}
	s.log.LogAttrs(ctx, log.LevelWarn, "server: dropped identity metadata from untrusted peer; add the caller to WithTrustedIdentityPeers if it is a trusted service",
		log.String("transport", transport),
		log.String("peer", peer.String()),
		log.String("key", key),
		log.Duration("suppressed_for", identityWarnEvery),
	)
}
