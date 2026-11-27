// Package webhook signs, sends and verifies webhooks in the Standard
// Webhooks format (https://www.standardwebhooks.com).
//
// A webhook is an HTTP POST whose body is the payload and whose headers
// carry the message id, the attempt time and one or more signatures:
//
//	webhook-id: msg_2KWPBgLlAfxdpx2AI54pPJ85f4W
//	webhook-timestamp: 1674087231
//	webhook-signature: v1,K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4=
//
// The signed content is "id.timestamp.body". A "v1" signature is
// HMAC-SHA256 with a symmetric secret ("whsec_" + base64); a "v1a" signature
// is ed25519 with a key pair ("whsk_" secret key to sign, "whpk_" public key
// to verify). A request may carry several space-separated signatures, so a
// sender can sign with an old and a new secret while receivers rotate.
//
// Receiving: [NewMiddleware] wraps a handler. It limits and reads the body,
// verifies it with a [Verifier] (by default [NewVerifier]: constant-time
// comparison, a five-minute timestamp tolerance in both directions), and
// optionally drops repeated deliveries of one message id with a [Dedup]
// ([NewRedisDedup]). Every failure is refused: the handler only ever sees
// verified requests. The raw body stays readable from r.Body and from
// [FromContext].
//
// Sending: [Signer] produces the headers for a payload. Durable delivery
// with retries over hours is in [github.com/dungnh3/kitchen/webhook/delivery].
package webhook
