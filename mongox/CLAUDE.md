# mongox: package rules

`mongox` is the canonical MongoDB entry point (D7). Its defaults must stay
aligned with `pg` and `mysqlx`.

## TLS

TLS is required unless one of these applies:
- the URI chooses explicitly (`tls=true/false`, or `mongodb+srv`, which
  implies TLS);
- every host is loopback.

The server certificate is always verified.

## Telemetry

- This package contains its own OTel bridge, because driver v2 has no
  official integration yet. Replace the bridge with the official one when it
  ships; the owner made this choice.
- Spans follow the database semantic conventions.
- Command bodies are never recorded.
- Span errors go through `otelx.RecordError`.

## Errors

`MapError` turns driver errors into `errs` sentinels. Driver error types never
reach clients.

## API

`Open` returns a plain `*mongo.Client`. Do not wrap the driver API.
