# i18n

`i18n` translates messages, mostly the reasons of `errs` errors, into the
language of the request. Bundles are built on go-i18n catalogs. The language
is picked by standard Accept-Language negotiation.

`i18n` is the canonical package for translations
([D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)). Do not import
go-i18n elsewhere; the D7 lint forbids it.

## Install

```sh
go get github.com/dungnh3/kitchen
```

## Quick start

```go
b, err := i18n.New(language.English, i18n.WithDefaults(),
	i18n.WithMessages(language.Vietnamese, map[string]string{"AGENT_NOT_FOUND": "Không tìm thấy agent."}))
if err != nil {
	panic(err)
}
ctx := i18n.WithAcceptLanguage(context.Background(), "vi-VN,vi;q=0.9,en;q=0.5")
fmt.Println(b.Localize(ctx, &errdetails.ErrorInfo{Reason: "AGENT_NOT_FOUND"}))
fmt.Println(b.Message("NOT_FOUND", nil, "en-US"))
// Output:
// vi Không tìm thấy agent. true
// The requested resource was not found. en true
```

In a service you rarely call the bundle yourself. Pass it to the server with
`server.WithI18n(b)`, and every error response carries a
`google.rpc.LocalizedMessage` in the client's language. See
[Integration with kitchen](#integration-with-kitchen).

What the defaults give you:

- **`WithDefaults()`** loads English and Vietnamese text for every generic
  code (`NOT_FOUND`, `INTERNAL`, ...) and for every reason in
  `errs.Catalog()`, including the auth and rate-limit reasons.
- **Fallback.** The first argument of `New` is the fallback language. It is
  used when the request asks for a language the bundle does not have.
- **Safe after `New`.** A `Bundle` is immutable and safe for concurrent use.
- **No `<no value>`.** A message whose template data is missing is reported
  as "no message". Users never see a broken text.

## Configuration

| Option | Effect |
|---|---|
| `New(def, ...)` | `def` is the fallback language. |
| `WithDefaults()` | kitchen's built-in English and Vietnamese catalogs. |
| `WithFS(fsys, patterns...)` | Loads catalog files. The file name gives the language: `vi.json`, `active.en-US.json`. A pattern that matches nothing is an error. |
| `WithMessages(tag, map)` | Adds messages in code. Handy for a few reasons, and for tests. |

Options apply in order. A later message with the same ID and language
replaces the earlier one, so your catalogs can override the defaults.

There are no environment variables.

## Integration with kitchen

### server and errs

`server.WithI18n(b)` localizes the server's default `errs.Handler`. If you
build the handler yourself, set `errs.Config.Localizer` instead:

```go
b, err := i18n.New(language.English, i18n.WithDefaults(),
	i18n.WithMessages(language.Vietnamese, map[string]string{"AGENT_NOT_FOUND": "Không tìm thấy agent {{.agent_id}}."}))
if err != nil {
	panic(err)
}
eh := errs.NewHandler(errs.Config{Localizer: b, Domain: "agents.example.com", Logger: log.Discard()})

// kitchen/server records Accept-Language for you; a bare handler sets it.
r := httptest.NewRequest(http.MethodGet, "/v1/agents/a1", nil)
r = r.WithContext(i18n.WithAcceptLanguage(context.Background(), "vi"))
rec := httptest.NewRecorder()
eh.WriteHTTP(rec, r, ErrAgentNotFound.With(errs.Meta("agent_id", "a1")))
fmt.Println(rec.Body.String())
// Output:
// {"error":{"code":404,"message":"agent not found","status":"NOT_FOUND","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"AGENT_NOT_FOUND","domain":"agents.example.com","metadata":{"agent_id":"a1"}},{"@type":"type.googleapis.com/google.rpc.LocalizedMessage","locale":"vi","message":"Không tìm thấy agent a1."}]}}
```

Note what the snippet shows:

- `message` stays English, for developers and logs.
- The `LocalizedMessage` detail is the text to show the user.
- The error's metadata (`agent_id`) fills the template.

See [errs/USAGE.md](../errs/USAGE.md) and the service wiring in
[server/USAGE.md](../server/USAGE.md).

### Where the request language comes from

`AcceptLanguage(ctx)` reads the first non-empty source, in this order:

1. A value stored with `i18n.WithAcceptLanguage`.
2. `AcceptLanguage` in the request `Info` from `kitchen/context`. The server
   fills it from the HTTP header, or from the `accept-language` or
   `grpcgateway-accept-language` metadata.
3. The raw incoming gRPC metadata.
4. The `X-Locale` header captured in the `Info`.

Behind `kitchen/server` you get this for free. Call `WithAcceptLanguage`
only in code that does not run behind it, such as tests, CLIs or workers.

## Advanced

### Ship your own catalogs

Keep one JSON file per language next to your service, embed it, and load it
after the defaults:

```go
// In a service: //go:embed langs/*.json, then pass that embed.FS.
langs := fstest.MapFS{
	"langs/en.json": {Data: []byte(`{"AGENT_NOT_FOUND": "Agent not found."}`)},
	"langs/vi.json": {Data: []byte(`{"AGENT_NOT_FOUND": "Không tìm thấy agent."}`)},
}
b, err := i18n.New(language.English, i18n.WithDefaults(), i18n.WithFS(langs, "langs/*.json"))
if err != nil {
	panic(err)
}
fmt.Println(b.Languages())
msg, tag, _ := b.Message("AGENT_NOT_FOUND", nil, "vi")
fmt.Println(tag, msg)
// Output:
// [en vi]
// vi Không tìm thấy agent.
```

A file is a flat JSON object that maps a message ID (the `errs` reason) to
its text. The go-i18n plural forms also work.

### Templates with error metadata

Messages are Go templates. For errors, the template data is the
`ErrorInfo` metadata set with `errs.Meta`:

```go
b, err := i18n.New(language.English, i18n.WithMessages(language.English, map[string]string{
	"QUOTA_EXCEEDED": "You used all {{.limit}} requests of this month.",
}))
if err != nil {
	panic(err)
}
msg, _, ok := b.Message("QUOTA_EXCEEDED", map[string]string{"limit": "1000"})
fmt.Println(msg, ok)
// A placeholder without data is reported as missing, never "<no value>".
msg, _, ok = b.Message("QUOTA_EXCEEDED", nil)
fmt.Printf("%q %v\n", msg, ok)
// Output:
// You used all 1000 requests of this month. true
// "" false
```

When a template cannot be filled, the error response carries no
`LocalizedMessage`. Clients then fall back to their own text for the reason.
Only put values in metadata that are safe to show a user.

### Language negotiation

`Match` returns the best supported language for an Accept-Language value.
Use it to choose the language of emails or notifications:

```go
b, err := i18n.New(language.English, i18n.WithDefaults())
if err != nil {
	panic(err)
}
fmt.Println(b.Match("vi-VN,vi;q=0.9,en;q=0.5"))
fmt.Println(b.Match("fr-FR")) // unsupported: the default language
// Output:
// vi
// en
```

Header values that are too long or malformed are ignored, so a client cannot
make negotiation expensive.

## Testing

- Build a small bundle with `WithMessages` in the test.
- Put the language in the context with `WithAcceptLanguage`.
- Assert on `Message` or `Localize`. `ok == false` means no translation.

To check that every reason your service defines has a translation, loop over
your sentinels and call `b.Message(reason, nil, "vi")`.

## Observability

`i18n` emits no logs, spans or metrics. A missing translation is not an
error: the response simply has no `LocalizedMessage`.

## Production checklist

- Load `WithDefaults()` first, then your catalogs, so the defaults cover
  generic and catalog reasons.
- Add English and Vietnamese text for every reason you define.
- Choose the fallback language on purpose: it is what unsupported clients
  see.
- Keep catalogs in the binary with `embed`. Do not read them from a path at
  run time.
- Never put secrets or internal identifiers in metadata used by templates.

## Migrating from the old kitchen

`localization` is replaced by `i18n`, and message IDs are now `errs` reasons.
See [docs/migration/errors-auth.md](../docs/migration/errors-auth.md).
