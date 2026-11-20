# Review: mcpkit, nest, gen, vulgen-mcp, vulgen-service

Paths are under `/Users/dungnh/works/sources/github.com/vulcan/kitchen`. I changed no files. One note: running `go build ./...` in `vulgen-mcp` dropped a binary named `vulgen-mcp/mcpgen` in the repo. I deleted it, and `git status` is clean again.

## 1. Purpose and public API

- **mcpkit**: a builder and lifecycle wrapper around `modelcontextprotocol/go-sdk` v1.8.0.
  - Main pieces: `NewServer()` builder, `Server.Start/Serve/Stop/Wait/HTTPHandler/SDK()`, the `Tool`/`Resource`/`Prompt` wrappers (middleware, timeout, tags, resource TTL cache), `HTTPTransportConfig` (CORS/TLS/gzip/stateless), and thread-safe registries in `tools/`, `resources/` and `prompts/`.
  - Downstream use is mostly `mcpkit.Tool` (106 refs) and `mcpkit.NewTool` (83 refs).
  - Downstream is on older versions: `mcp-*-server` repos pin go-sdk v1.0.0, v1.1.0 and v1.5.0, with mcpkit pseudo-versions from 2025-10 to 2026-03.
- **nest**: a fluent HTTP client.
  - API: `nest.New(opts...)`, `Get/Post(...)` returning a `*Request` with `.JSON/.Bearer/.Query/.WithContext/.SendJSON`.
  - Middleware: retry, auth with refresh, timeout, metrics, rate-limit and circuit-breaker interfaces, plus an OTel tracer adapter and a decoder registry.
  - The only real users are Google OAuth token clients: `mcp-mail-server` and `mcp-calendar-server` (`internal/external/tokens/google_oauth.go`) and `ms-smith-briefing`.
- **gen** (binary `vulgen`): an interactive tool, using survey, that adds CRUD code for a model into an existing scaffolded service. It edits `internal/repositories/{repositories,filters}.go`, creates `postgres/<model>.go`, appends to `proto/<proj>_{message,service}.proto`, and creates `internal/services/gen_<model>.go`.
- **vulgen-mcp** (module `mcpgen`, go 1.21): a one-shot scaffold for an MCP server built on mcpkit, gin and viper, modelled on `mcp-mail-server`.
- **vulgen-service** (module `github.com/vulcanlabsvn/vulgen`, go 1.21): a one-shot scaffold for a gRPC + grpc-gateway service. Templates are an `embed.FS` copy of a reference "vulcan-service", rewritten by string replacement.

## 2. Design review

### mcpkit
- **Thin over the SDK, and drops its best features.** Tools are registered with the raw `sdkServer.AddTool(tool.Definition, tool.WrapHandler())` (`builder.go:588`).
  - The SDK's own doc for that function (go-sdk v1.8.0 `mcp/server.go:300-315`) says unmarshalling, input-schema validation, output schema, `StructuredContent` and `IsError` are all left to the caller.
  - Result: no schema generated from Go types, no input validation, no structured output, and hand-written `map[string]any` schemas everywhere (`SimpleTool`, `wrap.go:589`; the obj/str/arr helpers in the vulgen-mcp template at `templates.go:918-948`).
  - Errors returned by handlers, including timeouts (`wrap.go:207-212`), come back as JSON-RPC errors rather than `isError` results the model can read and recover from. The typed `mcp.AddTool[In,Out]` wraps errors as `IsError` (SDK `server.go:423`).
- **Transports** (`config.go:73-90`): stdio, legacy SSE (`http_server.go:156`) and Streamable HTTP (`:167`), stateless by default (`http_config.go:190`). They are well mapped, including `PropagateRequestCancellation` and a body-limit fix (`:182-187`).
- **Auth: none.** The only auth is a commented example in `wrap.go:26`. `mcp-atlassian-server` writes its own `BearerAuthMiddleware` (`internal/tools/auth.go:22`).
  - The SDK already ships `auth.RequireBearerToken` (`go-sdk@v1.8.0/auth/auth.go:97`) and `ProtectedResourceMetadataHandler` (`:188`, RFC 9728), and mcpkit exposes neither.
- **Bugs and risks:**
  - `WriteTimeout` defaults to 60s (`http_config.go:178`) and is applied to `http.Server` (`http_server.go:246`). That cuts SSE-streamed POST responses and the GET notification stream for any tool running over 60s.
  - The timeout wrapper starts a goroutine per call (`wrap.go:188-215`). A handler that ignores ctx leaks that goroutine, and this is documented rather than fixed.
  - The resource cache (`wrap.go:350-389`) is an unbounded map keyed only by URI, with lazy eviction. If a resource handler is per-user (for example token-scoped), one user's data is served to another.
  - CORS with an empty origin list reflects any origin (`http_server.go:410-414`). OPTIONS returns 204 even for disallowed origins (`:433`).
  - gzip is skipped whenever `Accept` contains `text/event-stream` (`:479`), and spec clients always send that on POST, so compression never runs. On 202/204 responses it would also write gzip trailer bytes. It is dead weight.
  - Registry `All()`/`Names()` iterate a map, so order is random (e.g. `tools/registry.go:112`). Tool-list order then changes between restarts, which hurts prompt caching.
  - `EnableLogging` defaults to true (`config.go:124`) for a feature the current spec deprecates.
- **Tests:** `go test` passes with 73.2% coverage on the root package (the subpackage coverage run failed only because the `covdata` tool is missing locally). There are integration and compat tests, and `deprecated.go` keeps an old API shim.
- **Verdict on value:** the wrapper adds lifecycle handling, HTTP hardening and per-tool middleware. Net value is low, and negative on schema and structured output.

### nest
- **Data race / corruption** (`nest.go:522-528`): the request body is written into a pooled buffer, wrapped with `bytes.NewReader(buf.Bytes())` (the "Copy data" comment is wrong; nothing is copied), and the buffer goes back to the pool via `defer` before the request is sent. Under concurrency, request bodies get corrupted.
- **Context leak** (`nest.go:536`): `_ = cancel` discards the cancel func; `go vet` flags this (lostcancel).
- **Retry is broken for any request with a body:** `cloneRequestWithBody` (`retry.go:128-148`) needs a `Rewind()` method on `req.Body`. `http.NewRequest` wraps the reader in `io.NopCloser`, which has no `Rewind`, so even the first attempt errors. It should use `req.GetBody`.
- The auth middleware's 401 refresh path re-sends an already-consumed body (`middleware.go:146`).
- When the context is cancelled, retry returns `lastResp` with its body already closed (`retry.go:93-103`).
- `New()` ignores `WithLogger` (`nest.go:140` vs `opts.go:258`) and can panic.
- `WithInsecureSkipVerify` is exposed (`opts.go:168`).
- **Build and tests:** there are no tests. `go.mod` lists every dependency as `// indirect`, and `go build` fails because of missing go.sum entries (otelhttp semconv, gorm via `../log`).

### gen
- Uses `text/template` with sprig v2.22 (`utils.go:18`), sprig being an incompatible pre-modules release; v3 is current.
- **Edits are not idempotent:** text is appended or inserted before the last `}` (`repository.go:32`, `service.go:96`), so a re-run duplicates code. The output is not gofmt'd.
- **Bugs:**
  - `Transform: survey.Title` turns `user_profile` into `User_profile` (`qa.go:25`), which feeds file names (`repository.go:60`).
  - The deferred block always logs "succeeded", even after an error (`main.go:18`).
  - `strings.ReplaceAll(project, "ms-", "")` (`service.go:38`) mangles names such as `items-api`. vulgen-service never strips `ms-`, so for `ms-foo` gen looks for `foo_message.proto` while vulgen-service created `ms_foo_message.proto`.
  - Uses the legacy `kitchen/l` logger.
- The test file only covers string helpers (`utils_test.go`).

### vulgen-mcp
- `text/template` with `{% %}` delimiters plus a `tick` function, templates kept as Go string constants (`templates.go`, 1187 lines). It overwrites existing files without checking (`generate.go:87-92`).
- **Dead flags:** `IncludeAuth`, `IncludeResources` and `IncludePrompts` are prompted for (`main.go:93-95`) but never used (`generate.go:95-116`).
- **Pins:** an mcpkit pseudo-version and `kitchen/log v1.0.20251210` (`templates.go:16-19`); the Makefile runs `GOSUMDB=off go get -u` (`:66-71`).
- **Dockerfile:**
  - `alpine:3.18` (`:1177`), which went end-of-life 2025-05-09.
  - Builder is `golang:1.25-alpine` (`:1161`), which is also end-of-life.
  - Sets the non-existent variable `GONOSUMCHECK` and passes `GITHUB_TOKEN` without a git `insteadOf` rule (`:1167-1170`), so private modules probably fail to download.
- The generator's own module declares go 1.21 while the project it generates declares 1.25. There are no tests.

### vulgen-service
- Copies the `embed.FS` tree (`generator.go:13,60-101`) with ordered `strings.ReplaceAll` (`:105-150`). No real templating.
- **Breaking bug:** the rule `{srcOrg, cfg.OrgName}` (`:134`) rewrites every `vulcanlabsvn` in the output, including `github.com/vulcanlabsvn/kitchen/*`, `central-proto` and `devops-reusable-workflows`. Any org other than vulcanlabsvn produces a project that cannot resolve its imports.
- The Go version swap only touches `go 1.26.0` in go.mod; the Dockerfile `golang:1.26-alpine` is left alone.
- `.go` templates sit inside the generator's own module, so `go build ./...` and `go vet ./...` fail (confirmed).
- **Generated stack:**
  - grpc + grpc-gateway v2.28 with buf v2: `protoc-gen-go v1.34.1` (`buf.gen.yaml`), which mismatches protobuf v1.36.11 in the generated go.mod.
  - `protoc-gen-validate`, now superseded by protovalidate.
  - Data and infra: gorm v1.31.1 + postgres driver v1.5.11 + zapgorm2, go-redis v9, viper, golang-migrate-style SQL migrations, and GCP Pub/Sub (`pkg/gcppubsub`).
  - Kitchen modules: log (17 refs), errors, context, tracing, server, rlimit, localization, guard, grpc, pubsub, ecode, healthcheck. It also uses both `l` and `log`, two different logger modules.
- **Dockerfile:**
  - Runtime is `alpine:3.12`, end-of-life since 2022-05-01.
  - `ENV GOSUMDB off`.
  - The token goes into a builder layer through `git config`; this does not reach the final image.
- **CI:** `.github/workflows/*.yml` call `vulcanlabsvn/devops-reusable-workflows/...@master`, which is unpinned.
- `go.mod.tmpl` lists kitchen dependencies at mixed pseudo-versions, with `kitchen/log` as indirect even though the templates import it directly.

### Overlap between the generators
- vulgen-service creates the service and gen adds models into it. They share layout assumptions but no code, which is where the proto naming mismatch above comes from.
- vulgen-mcp is a separate stack. All three use different prompt approaches (survey, raw `bufio`, raw `bufio`) and different templating (sprig templates, `{% %}` templates, string replacement).

## 3. Dependency freshness (checked 2026-11-20)

| Item | In repo | Latest | Source |
|---|---|---|---|
| go-sdk | mcpkit v1.8.0; downstream v1.0–v1.5 | v1.8.0 is the latest stable; v1.9.0-pre.1 exists. It negotiates spec 2026-07-28 and falls back to 2025-11-25 | https://github.com/modelcontextprotocol/go-sdk/releases |
| MCP spec | code mentions 2026-07-28 (`config.go:62`) | 2026-07-28 is current. It is stateless per request (`_meta` protocolVersion, `server/discover`). Deprecated: Roots, Sampling, Logging, DCR (replaced by Client ID Metadata Documents), HTTP+SSE | https://modelcontextprotocol.io/specification/versioning , https://modelcontextprotocol.io/specification/2026-07-28/deprecated.md |
| survey | v2.3.7 | Archived 2024-04-19; its README points to Bubble Tea | https://github.com/AlecAivazis/survey |
| huh | – | v2 line, module path `charm.land/huh/v2`; I saw v2.0.3 but did not confirm it is the latest | https://newreleases.io/project/github/charmbracelet/huh/release/v2.0.0 |
| Go | templates 1.25/1.26; generators 1.21/1.22.5 | 1.27.4 (2026-11-10); 1.26 still supported; 1.25 end-of-life | https://go.dev/doc/devel/release |
| Alpine | 3.12, 3.18 | 3.24 (supported to 2028-06) | https://alpinelinux.org/releases/ |
| sprig | v2.22 | v3 exists (not verified as latest) | – |
| gonew | – | Still under x/tools; status not verified | – |

## 4. Modern-trend assessment and recommendations

- **mcpkit: REDESIGN as a thin layer.**
  - Use typed `mcp.AddTool[In,Out]` so schemas come from Go types, inputs are validated, and structured output and `IsError` work.
  - Wire in `auth.RequireBearerToken` and protected-resource metadata; support Client ID Metadata Documents rather than DCR.
  - Keep Streamable HTTP stateless (it fits the 2026-07-28 model) and drop the legacy SSE transport.
  - Put the middleware hooks on the SDK's own receiving middleware, and use OpenTelemetry instead of MCP logging.
  - Expose elicitation (`ServerSession.Elicit`).
  - Fix `WriteTimeout`, sort registry output, and remove gzip.
  - Plan a migration for the 83 `NewTool` call sites.
- **nest: DROP**, or rewrite it as a set of `http.RoundTripper` middlewares. Current users only need `golang.org/x/oauth2` (Google token source) plus `otelhttp`. The data race alone rules out keeping it as is.
- **gen + vulgen-service + vulgen-mcp: MERGE** into one generator CLI with subcommands (`new service`, `new mcp`, `add model`).
  - Templates in `embed.FS` with `text/template` (or a gonew-style copy of a reference repo that is kept compiling and under CI).
  - Prompts via huh v2, plus non-interactive flags for CI.
  - Refuse to overwrite files, run gofmt/goimports on output, and edit Go code with AST tools rather than text insertion.
  - Golden-file tests that check the generated project builds.
  - Template updates: Go 1.26/1.27, a distroless or alpine 3.24 runtime, `GOPRIVATE` plus a BuildKit secret, workflows pinned to a commit SHA, protovalidate, and current buf plugins.
  - Drop the legacy `l` logger.

## 5. Open questions for the owner
1. Should the generated stack stay gRPC + grpc-gateway + gorm, or move (for example to connect-go or sqlc/pgx)?
2. Is GCP Pub/Sub still the standard broker, and should the scaffold use the new pubsub layer from the SSE rewrite?
3. Which MCP auth model is intended: an external IdP issuing JWTs, an internal gateway, or pass-through Google OAuth (as mail and calendar do)?
4. Do any downstream MCP clients still need the legacy SSE transport or the 2025-11-25 handshake?
5. Will the new repo keep `devops-reusable-workflows` and the `vulcanlabsvn` org in templates, or must templates be org-neutral?
6. Is `gen` (one downstream user) still used, and is `vulgen` meant to own the service layout long term?
7. Should nest's circuit-breaker and rate-limit interfaces be kept anywhere, or are they unused?
