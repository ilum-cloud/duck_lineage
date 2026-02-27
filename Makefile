PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duck_lineage
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Override distribution workflow's test target to run our pytest suite
test_release:
	@if ! command -v docker >/dev/null 2>&1; then \
		echo "Skipping tests: docker not available"; \
		exit 0; \
	fi
	@if ! command -v uv >/dev/null 2>&1; then \
		echo "Installing uv..." && curl -LsSf https://astral.sh/uv/install.sh | sh; \
	fi
	$(MAKE) test-deps marquez-up
	cd test && uv run pytest -v -m smoke
	$(MAKE) marquez-down

#### Testing targets ####

.PHONY: test-deps test-setup test-all test-integration test-smoke test-ducklake marquez-up marquez-down marquez-logs ducklake-up ducklake-down test-clean test-help

test-help:
	@echo "Testing Targets:"
	@echo "  test-deps        - Install test dependencies (uv sync --extra test)"
	@echo "  test-setup       - Install deps + start Marquez"
	@echo "  test-all         - Run all tests (builds extension first)"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-smoke       - Run quick smoke tests"
	@echo "  test-ducklake    - Run DuckLake postgres+S3 tests"
	@echo "  marquez-up       - Start Marquez services"
	@echo "  marquez-down     - Stop Marquez services"
	@echo "  ducklake-up      - Start Marquez + DuckLake infra (postgres, MinIO)"
	@echo "  ducklake-down    - Stop DuckLake infra"
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

test-all: release test-deps ducklake-up
	@echo "Running all tests..."
	cd test && uv run pytest -v

test-integration: release test-deps marquez-up
	@echo "Running integration tests..."
	cd test && uv run pytest -v -m integration

test-smoke: release test-deps marquez-up
	@echo "Running smoke tests..."
	cd test && uv run pytest -v -m smoke

test-ducklake: release test-deps ducklake-up
	@echo "Running DuckLake postgres+S3 tests..."
	cd test && uv run pytest -v -m ducklake_postgres

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

ducklake-up: marquez-up
	@echo "Starting DuckLake infrastructure (postgres + MinIO)..."
	cd test && docker compose --profile ducklake up -d
	@echo "Waiting for DuckLake postgres to be ready..."
	@for i in $$(seq 1 30); do \
		if docker exec ducklake-postgres pg_isready -U ducklake >/dev/null 2>&1; then \
			echo "✓ DuckLake postgres is ready!"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then echo "✗ DuckLake postgres failed to start"; exit 1; fi; \
		sleep 2; \
	done
	@echo "Waiting for MinIO to be ready..."
	@for i in $$(seq 1 30); do \
		if curl -f -s http://localhost:9000/minio/health/live >/dev/null 2>&1; then \
			echo "✓ MinIO is ready!"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then echo "✗ MinIO failed to start"; exit 1; fi; \
		sleep 2; \
	done

ducklake-down:
	@echo "Stopping DuckLake infrastructure..."
	cd test && docker compose --profile ducklake down

marquez-logs:
	cd test && docker compose logs -f marquez

test-clean:
	@echo "Cleaning test artifacts..."
	cd test && rm -rf .pytest_cache __pycache__
	find test -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find test -type f -name "*.pyc" -delete
