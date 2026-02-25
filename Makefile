PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=openlineage
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

#### Testing targets ####

.PHONY: test-deps test-setup test-all test-integration test-smoke marquez-up marquez-down marquez-logs test-clean test-help

test-help:
	@echo "Testing Targets:"
	@echo "  test-deps        - Install test dependencies (uv sync --extra test)"
	@echo "  test-setup       - Install deps + start Marquez"
	@echo "  test-all         - Run all tests (builds extension first)"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-smoke       - Run quick smoke tests"
	@echo "  marquez-up       - Start Marquez services"
	@echo "  marquez-down     - Stop Marquez services"
	@echo "  marquez-logs     - View Marquez logs"
	@echo "  test-clean       - Clean test artifacts"
	@echo ""
	@echo "Quick start:"
	@echo "  make test-setup  # First time setup"
	@echo "  make test-all    # Run tests"

test-deps:
	@echo "Installing test dependencies..."
	uv sync --extra test

test-setup: test-deps marquez-up
	@echo "✓ Test environment ready!"
	@echo "Run 'make test-all' to run tests"

test-all: release test-deps marquez-up
	@echo "Running all tests..."
	cd test && uv run pytest -v

test-integration: release test-deps marquez-up
	@echo "Running integration tests..."
	cd test && uv run pytest -v -m integration

test-smoke: release test-deps marquez-up
	@echo "Running smoke tests..."
	cd test && uv run pytest -v -m smoke

marquez-up:
	@echo "Starting Marquez..."
	cd test && docker compose down -v
	cd test && docker compose up -d
	@echo "Waiting for Marquez to be ready..."
	@for i in $$(seq 1 60); do \
		if curl -f -s http://localhost:5001/ping >/dev/null 2>&1; then \
			echo "✓ Marquez is ready!"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "✗ Marquez failed to start"; \
	exit 1

marquez-down:
	@echo "Stopping Marquez..."
	cd test && docker compose down

marquez-logs:
	cd test && docker compose logs -f marquez

test-clean:
	@echo "Cleaning test artifacts..."
	cd test && rm -rf .pytest_cache __pycache__
	find test -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find test -type f -name "*.pyc" -delete