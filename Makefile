# Temporal Elixir SDK Development Makefile

.PHONY: test clean setup deps compile help

# Default target
all: deps compile test

# Install dependencies
deps:
	mix deps.get

# Compile the project
compile:
	mix compile

# Run all tests (requires Docker for testcontainers)
test:
	mix test

# Clean build artifacts
clean:
	mix clean
	rm -rf _build
	rm -rf deps

# Setup development environment
setup: deps compile

# Development workflow - compile and test
dev: compile test

help:
	@echo "Available targets:"
	@echo "  deps              - Install dependencies"
	@echo "  compile           - Compile the project"
	@echo "  test              - Run all tests (requires Docker)"
	@echo "  clean             - Clean build artifacts"
	@echo "  setup             - Setup development environment"
	@echo "  dev               - Development workflow (compile + test)"
	@echo "  help              - Show this help"