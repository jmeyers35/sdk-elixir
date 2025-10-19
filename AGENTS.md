# Repository Guidelines

## Work Tracking
We track work in Beads instead of Markdown. Run `bd quickstart` to see how.

## Project Structure & Module Organization
Source code lives in `lib/temporal`, with boundary modules (`client.ex`, `payload_converter.ex`, `native.ex`) delegating into the matching subdirectories. Protobuf definitions sit in `lib/temporal/protobuf`, and runnable samples in `examples/`. Runtime assets and configs are under `priv/` and `dynamicconfig/`. Rust NIF code is in `native/temporal_nif`, the Temporal core workspace is vendored in `sdk-core/`, and integration tests reside in `test/` with helpers in `test/support/temporal_test_container.ex`.

## Build, Test & Development Commands
Run `mix deps.get` to pull Elixir dependencies and prime the Rust compilation toolchain. `mix compile` builds both Elixir modules and the NIF. Use `mix test` for the full suite; add `--trace` when debugging specific cases. The Makefile mirrors these steps: `make setup` (deps + compile), `make dev` (compile + test), and `make clean` to reset `_build/`, `deps/`, and Rust artifacts.

## Coding Style & Naming Conventions
Always run `mix format` before committing; the default formatter enforces two-space indentation and alphabetized aliases/imports. Modules should live under the `Temporal.*` namespace and use PascalCase; public functions stay in snake_case with descriptive verbs (`start_worker/1`, `register_converter/2`). Guard internal structs with `@enforce_keys` and declare specs for public APIs. Rust files in `native/temporal_nif` follow the workspace `rustfmt.toml`.

## Testing Guidelines
Tests rely on ExUnit plus Testcontainers. Mirror source filenames when adding tests (`temporal/client/my_feature_test.exs`) and group behavior with `describe` blocks. Prefer `assert_receive` and helper functions from `test/support/temporal_test_container.ex` for workflow assertions. Ensure Docker is running before invoking `mix test`, and use `mix test --cover` when touching core workflow paths.

## Commit & Pull Request Guidelines
Recent history favors task-centric messages (`Complete Task 3.1.1: Implement Worker Creation NIF`); keep that format or supply a concise `scope: summary`. Include only formatted, linted changes per commit. Pull requests should describe the workflow impact, list commands executed (`mix test`, `make dev`), link related issue trackers, and wait for CI plus another maintainer’s review before merging.

## Native & Core Notes
If you modify `native/temporal_nif` or integrate new Temporal capabilities, run `cargo test` from within `native/temporal_nif` and `sdk-core/` to validate the Rust layers. Synchronize NIF interface updates with the corresponding Elixir wrappers, and record any breaking API shifts in `README.dev.md`.
