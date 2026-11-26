# USAGE.md template

Every top-level kitchen package has a `USAGE.md` next to its `doc.go`. It
answers one question: how do I use this package, from the defaults to
advanced setups, together with the rest of kitchen?

Adapter subpackages are documented in their parent's `USAGE.md`, in the
"Backends and adapters" section. Examples: `pubsub/gcp` is covered in
`pubsub/USAGE.md`, and `objstore/s3` in `objstore/USAGE.md`.

## Rules

- **Runnable code.** Every snippet must compile against the current API.
  Copy snippets from `Example*` tests. If a snippet has no matching example,
  add the example first.
- **Defaults first.** The reader should get working code after the first
  screen. Advanced setups come later.
- **No API reference.** Do not repeat godoc's list of types and functions;
  link to it instead. USAGE.md shows how pieces fit together.
- **Wire it the kitchen way.** Integration snippets use the canonical
  packages from D7 and the logging rule from D5:
  - `log.Setup` in `main`;
  - `*log.Logger` passed in;
  - errors through `errs`;
  - telemetry through `otelx`;
  - and so on.
- **Respect the security defaults.** Never show a security default turned
  off without a warning that says why it exists.
- **Length.** Aim for 150 to 400 lines. Cut anything that does not help
  someone ship.
- **Language.** English, with short sentences, consistent with the rest of
  `docs/`.

## Sections, in this order

Leave out a section that does not apply, rather than writing filler.

```markdown
# <package>

One or two sentences: what this package does, and when to use it rather than
something else. Link to the D7 row if this is a canonical package.

## Install

go get line(s): the core module and any adapter submodules.

## Quick start

The smallest working program, using defaults. Then a short list of what the
defaults are, especially the safe ones.

## Configuration

A table of options and env vars: name, default, and when to change it.

## Integration with kitchen

How this package plugs into the others, with one snippet each where it
applies:
- logger;
- errs and HTTP/gRPC status;
- otelx spans and metrics;
- server wiring (WithServices, checks, WithRateLimit, ...);
- auth / xcontext identity;
- redisx, pgx, and so on.

## Advanced

Real scenarios, one per subsection: multi-instance, high load, rotation,
custom hooks, and so on. Each subsection says when you would need it.

## Backends and adapters

One subsection per adapter submodule: install, open, and its specific
options and limits.

## Testing

In-memory backends, fakes, test helpers, and how to assert on errors and
logs.

## Observability

The spans, metrics and log attributes this package emits, and the errs
reasons it returns.

## Production checklist

A short bullet list of things to set and things to avoid.

## Migrating from the old kitchen

One or two lines plus a link to `docs/migration/<area>.md`.
```
