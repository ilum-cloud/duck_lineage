"""
Pytest configuration and fixtures for DuckDB DuckLineage extension tests.

Prerequisites:
    uv sync --extra test

Run tests:
    uv run pytest
"""

import json
import os
import time
import uuid
import pytest
import duckdb
import requests
from pathlib import Path
from types import SimpleNamespace

from event_helpers import TEST_NAMESPACE, complete_lines

from marquez import MarquezTestClient


@pytest.fixture(scope="session")
def marquez_url():
    """Marquez API base URL."""
    return os.getenv("MARQUEZ_URL", "http://localhost:5000")


@pytest.fixture(scope="session")
def marquez_api_url(marquez_url):
    """Marquez API endpoint URL."""
    return f"{marquez_url}/api/v1"


@pytest.fixture(scope="session", autouse=True)
def _check_marquez(request, marquez_url):
    """Fail fast if Marquez is not reachable (skipped when only running slow/perf tests)."""
    # Skip Marquez check when no integration tests are collected
    items = request.session.items
    needs_marquez = any("slow" not in {m.name for m in item.iter_markers()} for item in items)
    if not needs_marquez:
        return
    try:
        resp = requests.get(f"{marquez_url}/api/v1/namespaces", timeout=5)
        resp.raise_for_status()
    except Exception as e:
        pytest.fail(f"Marquez is not reachable at {marquez_url}: {e}")


@pytest.fixture(scope="session")
def extension_path():
    """Path to the compiled DuckDB DuckLineage extension."""
    # Try multiple possible build locations
    possible_paths = [
        Path(__file__).parent.parent
        / "build"
        / "release"
        / "extension"
        / "duck_lineage"
        / "duck_lineage.duckdb_extension",
        Path(__file__).parent.parent
        / "build"
        / "debug"
        / "extension"
        / "duck_lineage"
        / "duck_lineage.duckdb_extension",
    ]

    for path in possible_paths:
        if path.exists():
            return str(path)

    pytest.skip(f"Extension not found. Build it first with 'make'. Tried: {[str(p) for p in possible_paths]}")


@pytest.fixture
def duckdb_with_extension(extension_path, marquez_api_url):
    """
    Create a DuckDB connection with the DuckLineage extension loaded and configured.
    """
    # Create a fresh in-memory database with allow_unsigned_extensions enabled
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})

    try:
        # Load the extension
        conn.execute(f"LOAD '{extension_path}'")

        # Configure the extension to point to Marquez
        conn.execute(f"SET duck_lineage_url = '{marquez_api_url}/lineage'")
        conn.execute(f"SET duck_lineage_namespace = '{TEST_NAMESPACE}'")
        conn.execute("SET duck_lineage_debug = true")

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


@pytest.fixture(scope="session")
def marquez_client(marquez_url):
    """Marquez client for interacting with the Marquez API."""
    return MarquezTestClient(marquez_url)


@pytest.fixture
def col_conn(duckdb_with_extension):
    """Connection with sample tables for column lineage tests."""
    conn = duckdb_with_extension
    conn.execute(
        """
        CREATE TABLE source_a (
            id INTEGER,
            name VARCHAR,
            value DECIMAL(10,2)
        )
    """
    )
    conn.execute("INSERT INTO source_a VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)")

    conn.execute(
        """
        CREATE TABLE source_b (
            id INTEGER,
            category VARCHAR,
            score DOUBLE
        )
    """
    )
    conn.execute("INSERT INTO source_b VALUES (1, 'X', 0.5), (2, 'Y', 0.8)")
    return conn


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
    Create a DuckDB connection with DuckLineage extension loaded and configured
    for a specific test namespace. Automatically handles cleanup.
    """
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{extension_path}'")
    conn.execute(f"SET duck_lineage_url = '{marquez_api_url}/lineage'")
    conn.execute(f"SET duck_lineage_namespace = '{clean_marquez_namespace}'")
    conn.execute("SET duck_lineage_debug = true")
    yield conn
    conn.close()


@pytest.fixture
def file_lineage(extension_path, tmp_path):
    """
    Connection whose lineage events are written to a file (file transport), so no Marquez is needed.

    Yields a namespace with ``conn``, ``dir`` (a temporary directory) and ``event(sql, event_type)``, which waits
    for and returns the event of that type for the statement with that SQL text.
    """
    path = tmp_path / "events.jsonl"
    namespace = f"file_lineage_{uuid.uuid4().hex[:8]}"
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{extension_path}'")
    # Events left in the process-wide queue by earlier tests are delivered before ours. Without a URL, the ones
    # still bound for an HTTP backend are dropped right away instead of holding ours up.
    conn.execute("SET duck_lineage_url = ''")
    conn.execute("SET duck_lineage_transport = 'file'")
    conn.execute(f"SET duck_lineage_file_path = '{path}'")
    conn.execute(f"SET duck_lineage_namespace = '{namespace}'")

    def event(sql, event_type="COMPLETE", timeout=60.0):
        deadline = time.time() + timeout
        while True:
            for line in complete_lines(path):
                candidate = json.loads(line)
                if (
                    candidate["eventType"] == event_type
                    and candidate["job"]["namespace"] == namespace
                    and candidate["job"].get("facets", {}).get("sql", {}).get("query") == sql
                ):
                    return candidate
            if time.time() > deadline:
                pytest.fail(f"No {event_type} event for {sql!r}")
            time.sleep(0.1)

    try:
        yield SimpleNamespace(conn=conn, dir=tmp_path, event=event)
    finally:
        # The LineageClient is a process-wide singleton: switch back to the HTTP transport for later tests
        conn.execute("RESET duck_lineage_transport")
        conn.execute("RESET duck_lineage_file_path")
        conn.execute("RESET duck_lineage_namespace")
        conn.close()
