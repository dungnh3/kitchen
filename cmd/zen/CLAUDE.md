# cmd/zen: generator rules

`zen` generates new services from `templates/`. The code it generates is
what teams copy into production, so templates must follow every kitchen rule
in the root `CLAUDE.md` and `docs/decisions.md`.

## What generated code must do

- Call `log.Setup` first in `main`. Use `LOG_OUTPUT=stderr` for stdio
  programs, such as MCP servers over stdio.
- Use the canonical packages (D7) and never raw libraries:
  - `server`
  - `errs`
  - `auth` (fail-closed)
  - `otelx/sdk`
  - `redisx`
  - `pg` / `mysqlx`
  - `httpx`
  - others as they come up
- Ship a `.golangci.yml` with the same depguard/forbidigo D5/D7 rules as the
  repo, so generated services keep following them.

## Golden files

`testdata/golden/` holds the expected output for every variant. When you
change a template:

1. Regenerate the golden files with `go test ./... -update`.
2. Review the golden diff the way you would review real code.
3. Check that the generated module builds and passes its own tests and lint.

## Template syntax

Templates use `{% %}` delimiters so they don't collide with Go's `{{ }}`.
Keep the template logic minimal; put decisions in Go code (`newmcp.go`,
`internal/`), not in the templates.
