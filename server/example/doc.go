// Package example holds the compiled, end-to-end wiring of a kitchen
// service that server/USAGE.md quotes. It is its own module so the example
// can use the adapter submodules (auth, otelx/sdk, ratelimit/redis) without
// the core module depending on them (D1). Nothing imports it, and it is
// never tagged.
package example
