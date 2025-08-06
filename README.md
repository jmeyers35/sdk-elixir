# Temporal Elixir SDK

Elixir SDK for Temporal workflow orchestration.

## Prerequisites

- Elixir 1.18+
- Rust (for compiling NIFs)
- Docker (for running tests)

## Quick Start

```bash
# Install dependencies
mix deps.get

# Compile the project
mix compile

# Run tests
mix test
```

## Development

### Using Make

```bash
# Setup development environment
make setup

# Development workflow (compile + test)
make dev

# Clean build artifacts
make clean

# Show all available targets
make help
```

### Manual Commands

```bash
# Install dependencies
mix deps.get

# Compile
mix compile

# Run tests
mix test
```

## Testing

All tests use [Testcontainers](https://github.com/testcontainers/testcontainers-elixir) to automatically manage a real Temporal server instance during testing. This provides better coverage than mocking while remaining lightweight and fast.

### Prerequisites
- Docker must be installed and running
- All dependencies installed (`mix deps.get`)

### Running Tests

```bash
# Run all tests
mix test

# Or use Make
make test
```

### How Tests Work

Tests automatically:
1. Pull the `temporalio/auto-setup` Docker image
2. Start a Temporal server with in-memory storage
3. Wait for the server to be ready
4. Run tests against the server
5. Clean up containers after tests complete

No manual setup required!

### Test Structure

```
test/
├── test_helper.exs              # Test configuration
├── temporal_test.exs            # Basic SDK tests
├── temporal/
│   └── client_test.exs          # Client functionality tests
└── support/
    └── temporal_test_container.ex  # Testcontainers setup
```

### Debugging Tests

#### View container logs
The test output shows container startup messages. For more detailed logs, modify the `TEMPORAL_LOG_LEVEL` environment variable in `temporal_test_container.ex`.

#### Inspect running containers
```bash
docker ps | grep temporal
```

#### Access Temporal UI during debugging
The UI port is dynamically assigned and printed in test output:
```
✅ Temporal container started!
   gRPC: localhost:32768
   UI: http://localhost:32769
```

### Common Issues

#### Docker not available
Tests will be skipped if Docker is not available. Make sure Docker is installed and running.

#### Container startup timeout
If containers take too long to start, increase the timeout in the wait strategy in `temporal_test_container.ex`.

#### Resource cleanup
Testcontainers automatically cleans up containers. If interrupted, manually clean up:
```bash
docker ps -a | grep temporal
docker rm -f <container_id>
```

## Architecture

This SDK uses Rust NIFs (Native Implemented Functions) for high-performance integration with Temporal's core libraries.

### Components

- **Client**: Connection management and API interaction
- **Worker**: Workflow and activity execution
- **Native**: Rust NIF implementation

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `temporal` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:temporal, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/temporal>.

