"""
Basic tests for DuckDB DuckLineage extension.
"""

import pytest


@pytest.mark.integration
def test_extension_loads(duckdb_with_extension):
    """Test that the extension loads successfully."""
    conn = duckdb_with_extension

    # The extension should be loaded via the fixture
    # Try executing a simple query to verify DuckDB works
    result = conn.execute("SELECT 1 as test").fetchone()
    assert result[0] == 1


@pytest.mark.integration
def test_configuration_set(duckdb_with_extension):
    """Test that DuckLineage configuration can be set."""
    conn = duckdb_with_extension

    # These should not raise errors
    conn.execute("SET duck_lineage_url = 'http://test.example.com/api/v1/lineage'")
    conn.execute("SET duck_lineage_namespace = 'test_namespace'")
    conn.execute("SET duck_lineage_debug = true")
    conn.execute("SET duck_lineage_api_key = 'test-key'")


@pytest.mark.integration
def test_simple_query(sample_table):
    """Test a simple SELECT query."""
    conn = sample_table

    # Execute a simple query
    result = conn.execute("SELECT COUNT(*) FROM test_employees").fetchone()
    assert result[0] == 4

    # Query with filter
    result = conn.execute(
        """
        SELECT name FROM test_employees
        WHERE department = 'Engineering'
        ORDER BY name
    """
    ).fetchall()

    assert len(result) == 2
    assert result[0][0] == 'Alice'
    assert result[1][0] == 'Carol'
