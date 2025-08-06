# CRUSH.md

Build / Setup
- Prereqs: Elixir >= 1.18, Rust toolchain (for NIFs), Docker (tests use Testcontainers)
- Install deps: mix deps.get
- Compile Elixir + NIFs: mix compile
- Make targets (shortcut): make setup | make dev | make test | make clean

Test
- All tests: mix test
- Single file: mix test test/temporal/client_test.exs
- Single test by name: mix test test/temporal/client_test.exs:42
- Filter by pattern: mix test --only focus
- With coverage: mix test --cover

Lint / Format / Type checks
- Format check: mix format --check-formatted
- Auto-format: mix format
- Credo (if added later): mix credo --strict
- Rust subcrates (sdk-core, core, etc.): cargo fmt --all -- --check | cargo clippy --all -D warnings | cargo test --all

Repo structure (high level)
- Elixir app in lib/, tests in test/
- Rust core in sdk-core/, core/, client/, etc. (Cargo workspaces)
- NIFs in native/temporal_nif/

Code style guidelines
- Formatting: run mix format (uses .formatter.exs). For Rust, cargo fmt.
- Imports/Aliases: prefer alias for long module names; group std libs, third-party, then local; avoid unused aliases.
- Types: use @type/@opaque for public types; spec all public funcs with @spec; prefer non-nil returns or {:ok, t} | {:error, term} tuples.
- Naming: snake_case for functions/vars, PascalCase for modules; predicate? functions end with ?; mutating/side-effectful with ! when appropriate.
- Errors: don’t raise for control flow; return {:error, reason}; log minimally, let callers decide; in tests, use assert {:ok, _} patterns.
- NIF boundary: never leak secrets to logs; validate inputs before calling native; handle {:error, reason} from NIFs explicitly.
- Concurrency: avoid global state; supervise processes; use GenServer for client processes; link/monitor appropriately.

Notes
- Tests spin up Temporal via Docker automatically; ensure Docker is running.
- If Copilot/Cursor rules are added later (.github/copilot-instructions.md or .cursor/), mirror them here.
