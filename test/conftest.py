"""
Pytest configuration and fixtures for DuckDB OpenLineage extension tests.

Prerequisites:
    uv sync --extra test

Run tests:
    uv run pytest
"""

import os
import pytest
import duckdb
from pathlib import Path

from marquez import TestMaruezClient


@pytest.fixture(scope="session")
def marquez_url():
    """Marquez API base URL."""
    return os.getenv("MARQUEZ_URL", "http://localhost:5000")


@pytest.fixture(scope="session")
def marquez_api_url(marquez_url):
    """Marquez API endpoint URL."""
    return f"{marquez_url}/api/v1"


@pytest.fixture
def extension_path():
    """Path to the compiled DuckDB OpenLineage extension."""
    # Try multiple possible build locations
    possible_paths = [
        Path(__file__).parent.parent
        / "build"
        / "release"
        / "extension"
        / "openlineage"
        / "openlineage.duckdb_extension",
        Path(__file__).parent.parent / "build" / "debug" / "extension" / "openlineage" / "openlineage.duckdb_extension",
    ]

    for path in possible_paths:
        if path.exists():
            return str(path)

    pytest.skip(f"Extension not found. Build it first with 'make'. Tried: {[str(p) for p in possible_paths]}")


@pytest.fixture
def duckdb_with_extension(extension_path, marquez_api_url):
    """
    Create a DuckDB connection with the OpenLineage extension loaded and configured.
    """
    # Create a fresh in-memory database with allow_unsigned_extensions enabled
    conn = duckdb.connect(":memory:", config={'allow_unsigned_extensions': 'true'})

    try:
        # Load the extension
        conn.execute(f"LOAD '{extension_path}'")

        # Configure the extension to point to Marquez
        conn.execute(f"SET openlineage_url = '{marquez_api_url}/lineage'")
        conn.execute("SET openlineage_namespace = 'duckdb_test'")
        conn.execute("SET openlineage_debug = true")

        yield conn

    finally:
        # Clean up
        conn.close()


@pytest.fixture
def clean_marquez_namespace():
    """
    Fixture that provides a clean namespace for testing.
    Note: Marquez doesn't support deleting namespaces, so we use unique names per test.
    """
    import uuid

    namespace = f"test_{uuid.uuid4().hex[:8]}"
    return namespace


@pytest.fixture
def marquez_client(marquez_url):
    """Marquez client for interacting with the Marquez API."""
    return TestMaruezClient(marquez_url)


@pytest.fixture
def sample_table(duckdb_with_extension):
    """Create a sample table for testing."""
    conn = duckdb_with_extension

    conn.execute(
        """
        CREATE TABLE test_employees (
            id INTEGER,
            name VARCHAR,
            department VARCHAR,
            salary DECIMAL(10,2)
        )
    """
    )

    conn.execute(
        """
        INSERT INTO test_employees VALUES
            (1, 'Alice', 'Engineering', 95000),
            (2, 'Bob', 'Sales', 75000),
            (3, 'Carol', 'Engineering', 105000),
            (4, 'Dave', 'Marketing', 65000)
    """
    )

    return conn


@pytest.fixture
def lineage_connection(extension_path, marquez_api_url, clean_marquez_namespace):
    """
    Create a DuckDB connection with OpenLineage extension loaded and configured
    for a specific test namespace. Automatically handles cleanup.
    """
    conn = duckdb.connect(":memory:", config={'allow_unsigned_extensions': 'true'})
    conn.execute(f"LOAD '{extension_path}'")
    conn.execute(f"SET openlineage_url = '{marquez_api_url}/lineage'")
    conn.execute(f"SET openlineage_namespace = '{clean_marquez_namespace}'")
    conn.execute("SET openlineage_debug = true")
    yield conn
    conn.close()
