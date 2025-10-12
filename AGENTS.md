# Repository Guidelines

## Project Structure & Module Organization
- `cmd/` holds runnable entrypoints such as `chi_server` for the public API and `development` for local orchestration.
- `internal/` contains pipeline modules (`apool`, `chainfeeder`, `txingester`, `streamfeeder`, etc.); keep their APIs private to preserve clear boundaries.
- `shared/` provides reusable primitives (event bus, monitoring, dblib) shared by multiple modules.
- `server/` wraps the chi HTTP server and routing helpers, while `web/` serves the dashboard and module UI assets.
- `cfg/` stores configuration wiring; long-lived data and fixtures live under `production_storage/`, `stream_storage/`, `streambus/`, and `testing_enviroment/`.

## Build, Test, and Development Commands
- `go build ./...` compiles every Go package; run before landing sizable refactors.
- `go run ./cmd/chi_server` boots the HTTP API with `/ui` assets and `/api` routes.
- `go run ./cmd/development` starts the local development harness with mocked feeds.
- `go test ./...` executes the full test suite; add `-race` when investigating concurrency.
- `docker compose up` provisions Kafka and storage dependencies defined in `docker-compose.yml` for integration tests.

## Coding Style & Naming Conventions
- Target Go 1.23, formatting via `go fmt ./...` and sanity checks with `go vet ./...`.
- Keep package names lowercase, exported identifiers in UpperCamelCase, locals in camelCase or snake_case when mirroring protocol names.
- Prefer table-driven tests and explicit context parameters; document non-obvious business rules with concise comments.
- Front-end scripts in `web/` stick to ES modules, camelCase functions, and colocated styles in `style.css`.

## Testing Guidelines
- Place Go tests beside sources with the `_test.go` suffix (e.g., `internal/streamfeeder/app/tx_stream_feeder_test.go`).
- Cover new ingestion logic with constructor/unit tests plus integration flows driven by `testing_enviroment/` fixtures.
- Keep `go test ./... -race` passing before merging; monitor metrics using helpers under `shared/monitoring`.
- Record expected Kafka topics or storage keys when adding stream processors to ease reproducibility.

## Commit & Pull Request Guidelines
- Mirror the existing convention `TYPE: summary.` with capitalized prefixes (`Design`, `FIX`, `DEL`, `Refact`) followed by a concise sentence.
- One concern per commit; avoid mixing housekeeping with behavioural changes.
- Pull requests must outline scope, impacted modules, and verification steps; attach UI screenshots when touching `web/`.
- Reference related issues and note rollback steps for schema or storage migrations.

## Configuration & Security Notes
- Keep secrets in environment variables or local `.env`; never commit credentials under `cfg/` or storage directories.
- Update `cfg/env.go` when introducing new runtime flags and document defaults in the PR description.
