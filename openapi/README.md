# LavinMQ HTTP API OpenAPI spec

The HTTP API reference is rendered by a small custom renderer
(`static/docs/docs.js`) that consumes a bundled, single-file copy of the
OpenAPI 3.0.3 spec and styles every endpoint with the same `static/main.css`
design tokens used by the rest of the management UI. There is no third-party
docs renderer (no Stoplight Elements, Scalar, Redoc) — the goal is pixel-level
consistency with the management UI.

## Files

- `static/docs/openapi.yaml` — entry point for the modular spec.
- `static/docs/paths/*.yaml`, `static/docs/schemas/*.yaml` — modular spec
  fragments referenced from `openapi.yaml` via `$ref`.
- `static/docs/openapi.bundle.json` — generated single-file bundle consumed by
  the browser. Built from the modular sources by `openapi/bundle.cr`.
  Gitignored.
- `static/docs/index.html`, `static/docs/docs.css`, `static/docs/docs.js` —
  custom renderer.
- `openapi/bundle.cr` — Crystal stdlib-only bundler (no external deps). Inlines
  external `$ref`s and rewrites inward refs to local anchors.
- `openapi/openapi.rb` — original spec scaffolding generator (one-off).
- `openapi/.spectral.json` — Spectral lint ruleset (`make lint-openapi`).

## Editing the spec

Edit the modular YAML in `static/docs/`. After changes, regenerate the bundle:

    make docs

(or, equivalently: `crystal run openapi/bundle.cr -- static/docs/openapi.yaml static/docs/openapi.bundle.json`)

The `bin/lavinmq` build target depends on the bundle, so a regular `make`
also keeps it up to date.

## Linting

    make lint-openapi

Runs Spectral against `static/docs/openapi.yaml`.

## OpenAPI conventions

- `summary` is the short description.
- `description` is the longer description (limited Markdown supported by the
  custom renderer: paragraphs, inline `code`, **bold**, links).

## One-off scaffolding

The original spec scaffolding was generated with:

    ruby openapi.rb
