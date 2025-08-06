# Temporal Elixir SDK - Development Setup

## Quick Start with Docker Compose

This repository includes a Docker Compose setup for running Temporal locally during development and testing.

### Starting Temporal

```bash
# Start all services (Temporal server, PostgreSQL, Web UI)
docker-compose up -d

# Check that services are running
docker-compose ps

# View logs
docker-compose logs -f temporal
```

### Services

- **Temporal Server**: `localhost:7233` (gRPC endpoint)
- **Temporal Web UI**: `http://localhost:8080` (browser interface)
- **PostgreSQL**: `localhost:5432` (database)

### Testing the Example

Once Temporal is running, you can test the basic client example:

```bash
# Compile the project
mix compile

# Run the basic client example
./examples/basic_client.exs
```

Expected output:
```
[info] Starting Temporal client example...
[info] Client started, current status: disconnected
[info] Successfully connected to Temporal server!
[info] Starting workflow with ID: example-workflow-123456
[info] Workflow started successfully!
[info]   Workflow ID: example-workflow-123456
[info]   Run ID: abc-def-123-456
[info] Final client status: connected
[info] Client stopped
```

### Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

### Troubleshooting

#### Connection Issues

If you see connection errors:

1. Check that Temporal is running: `docker-compose ps`
2. Check Temporal logs: `docker-compose logs temporal`
3. Ensure port 7233 isn't blocked by firewall

#### Workflow Start Issues

The example will show a message like "This might be because no worker is running" - this is expected since we're only testing the client connection and workflow submission, not execution.

#### Web UI Access

Visit `http://localhost:8080` to see the Temporal Web UI where you can:
- View submitted workflows
- Monitor system health
- Browse namespaces and task queues

### Development Configuration

The setup uses development-friendly configuration in `dynamicconfig/development-sql.yaml`:
- Client version checking disabled
- Eager workflow start enabled
- Higher QPS limits for local development

### Integration Testing

The test suite uses this same Docker setup via the `TestContainer` module for integration tests:

```bash
# Run integration tests (requires Docker)
mix test --only integration
```