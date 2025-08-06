# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Temporal Elixir SDK - an Elixir SDK for Temporal workflow orchestration built with Rustler NIFs for high-performance integration with Temporal's core libraries.

**Architecture**: Elixir API layer using GenServer/OTP patterns with Rust NIFs for core Temporal operations.

## Development Commands

### Build and Test
```bash
# Install dependencies and compile
mix deps.get && mix compile

# Run all tests (requires Docker for testcontainers)  
mix test

# Run specific test
mix test test/temporal/client_test.exs

# Run single test by line
mix test --only line:46

# Clean and rebuild
mix clean && mix compile

# Development workflow via Makefile
make dev  # compile + test
```

### Testing with Docker
Tests automatically use Testcontainers to spin up real Temporal server instances. Docker must be running. The containers are automatically cleaned up after tests complete.

### Run Examples
```bash
# Basic client usage
mix run examples/basic_client.exs

# Signal and query example  
mix run examples/signal_and_query_example.exs
```

## Architecture Patterns

### NIF Integration
- `lib/temporal/native.ex` - Elixir NIF interface using Rustler
- `native/temporal_nif/` - Rust crate with actual Temporal SDK Core integration
- Resources managed by BEAM GC with proper cleanup in Drop impls

### Client Architecture
- `Temporal.Client` - GenServer-based client with connection management
- Connection state tracked with automatic reconnection
- All operations go through NIF layer to Rust/Temporal Core
- Telemetry events emitted for observability

### Key Modules
- `lib/temporal/client.ex` - Main client GenServer API
- `lib/temporal/native.ex` - NIF function stubs  
- `lib/temporal/native/behaviour.ex` - NIF behavior definition
- `native/temporal_nif/src/client.rs` - Rust client NIF implementations

### Data Flow
1. Elixir API calls → GenServer → NIF function calls
2. Rust NIFs interact with Temporal SDK Core  
3. Results converted back through Rustler → Elixir terms
4. Telemetry events emitted at API boundaries

### Testing Approach
- Unit tests with mocked NIFs using Mox
- Integration tests against real Temporal server via Testcontainers
- Test helpers in `test/support/` for container management and mocks

### Configuration
Uses standard Mix configuration with defaults in GenServer init. TLS and connection options passed through to Rust layer.

## Development Notes

- NIFs use dirty schedulers for blocking operations
- All string parameters must be converted from atoms to strings before NIF calls
- Resource cleanup handled automatically by BEAM GC calling Rust Drop impls
- Telemetry integrated throughout for observability
- Error handling propagates meaningful errors from Rust → Elixir

## Current Implementation Status

Based on IMPLEMENTATION_PLAN.md:
- ✅ Client connection and workflow operations (start, signal, query)
- ❌ Worker implementation (Phase 3)
- ❌ Workflow/Activity framework (Phase 4)

The project is currently in Phase 2 - extended client operations are complete.