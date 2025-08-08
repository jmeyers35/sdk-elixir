# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Temporal Elixir SDK - an Elixir SDK for Temporal workflow orchestration built with Rustler NIFs for high-performance integration with Temporal's core libraries.

**Architecture**: Elixir API layer using GenServer/OTP patterns with Rust NIFs for core Temporal operations.

## Development Philosophy

- We should ALWAYS strive to match the interfaces/APIs of the Python SDK, except where it makes sense to have Elixir-specific abstractions. This means for core features, like Payload conversion.

## Development Commands

### Build and Test
```bash
# Install dependencies and compile
mix deps.get && mix compile

# Run all tests (requires Docker for testcontainers)  
mix test

# Run specific test file
mix test test/temporal/client_test.exs

# Run single test by line number
mix test test/temporal/client_test.exs --only line:46

# Run tests with trace for debugging
mix test --trace

# Run tests with specific timeout
timeout 60 mix test

# Clean and rebuild
mix clean && mix compile

# Force recompile with specific environment
MIX_ENV=dev mix compile --force

# Development workflow via Makefile
make dev  # compile + test
make setup  # deps + compile
make clean  # clean all build artifacts
```

### Testing with Docker
Tests use Testcontainers to automatically spin up real Temporal server instances. Docker must be running. Containers are automatically cleaned up after tests.

If tests hang or fail to connect:
- Ensure Docker is running: `docker ps`
- Check for stale containers: `docker ps -a | grep temporal`
- Clean up if needed: `docker rm -f <container_id>`

### Run Examples
```bash
# Basic client usage
mix run examples/basic_client.exs

# Signal and query example  
mix run examples/signal_and_query_example.exs

# Protobuf payload converter example
mix run examples/protobuf_example.exs
```

### Rust NIF Development
```bash
# When modifying Rust code, ensure rebuild
mix clean && mix compile

# For Rust debugging, set environment variable
RUST_BACKTRACE=1 mix test

# Check Rust compilation directly
cd native/temporal_nif && cargo check
```

## Architecture Patterns

### NIF Integration
- `lib/temporal/native.ex` - Elixir NIF interface module with function stubs
- `lib/temporal/native/behaviour.ex` - Defines NIF behavior for mocking
- `native/temporal_nif/src/` - Rust implementation:
  - `lib.rs` - NIF module registration and resource definitions
  - `client.rs` - Client operations (connect, start_workflow, signal, query)
  - `worker.rs` - Worker stubs (not yet implemented)
  - `async_helpers.rs` - Tokio runtime management for async operations
- Resources (`ClientResource`, `WorkerResource`) managed by BEAM GC with Drop impls

### Client Architecture
- `Temporal.Client` - GenServer-based client with connection state management
- Connection established via `client_connect` NIF, returns resource reference
- All operations (start_workflow, signal, query) go through GenServer → NIF → Rust
- Telemetry events emitted: `[:temporal, :client, :*]` for observability
- Automatic parameter conversion (atoms → strings) before NIF calls

### Data Flow
1. User calls `Temporal.Client.start_workflow/3` (public API)
2. GenServer handles call, validates state
3. Parameters converted (atoms to strings, maps prepared)
4. NIF function called with dirty scheduler
5. Rust code uses Temporal SDK Core via Tokio runtime
6. Results converted back through Rustler
7. Telemetry event emitted with metadata
8. Response returned to caller

### Testing Strategy
- **Unit tests**: Mock NIFs using Mox (`Temporal.Native.Mock`)
- **Integration tests**: Real Temporal server via Testcontainers
- **Test helpers**: 
  - `TemporalTestContainer` - manages Docker containers
  - `NativeMock` - provides mock NIF implementations
- Test files organized by module under `test/temporal/`

### Error Handling
- Rust errors converted to `{:error, reason}` tuples
- Connection failures return `{:error, "Connection failed: details"}`
- Invalid parameters caught at Elixir layer before NIF call
- NIF panics safely handled, won't crash BEAM

### Configuration
- Client config passed as map to `client_connect`:
  - `target_url` - Temporal server URL (default: "http://localhost:7233")
  - `namespace` - Temporal namespace (default: "default")
  - `tls_config` - Optional TLS configuration map
- Worker config (when implemented) will follow similar pattern

## Current Implementation Status

### ✅ Completed (Phase 1-2)
- Project setup with Rustler integration
- Client connection and lifecycle management
- Workflow operations: start, signal, query
- Integration with Temporal SDK Core
- Testcontainers setup for testing
- Telemetry integration
- Basic examples

### ❌ Not Yet Implemented (Phase 3-4)
- Worker implementation (polling, task processing)
- Workflow definition framework (`use Temporal.Workflow`)
- Activity framework (`use Temporal.Activity`)
- Workflow helper functions (timers, child workflows)
- Deterministic execution guarantees
- Comprehensive configuration management
- Payload converter configuration

### Known Issues & Technical Debt
- **Serialization**: Currently JSON-only, needs composite PayloadConverter for cross-SDK compatibility
- **Security**: TLS cert path validation could be improved (non-blocking for MVP)
- **Performance**: Client cloning in Rust could be optimized with Arc patterns
- **Error Messages**: Could sanitize sensitive information in production

## Code Conventions

### Elixir
- Use GenServer for stateful components
- Emit telemetry events for all public operations
- Convert atom parameters to strings before NIF calls
- Return `{:ok, result}` or `{:error, reason}` consistently
- Add typespecs for all public functions

### Rust NIFs
- Use `DirtyCpu` or `DirtyIo` schedulers for blocking operations
- Wrap resources in Arc for thread safety
- Implement Drop for proper cleanup
- Convert errors to meaningful Elixir terms
- Never panic in NIF code - handle all errors gracefully

### Testing
- Write integration tests for new client operations
- Mock NIFs for unit testing GenServer logic
- Use Testcontainers for real Temporal server testing
- Add property tests for data conversion functions
- Ensure tests clean up resources properly