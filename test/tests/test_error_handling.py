"""
Tests for error handling and edge cases.
"""

import pytest
import duckdb


@pytest.mark.integration
def test_invalid_url_configuration(extension_path):
    """Test behavior when setting an invalid OpenLineage URL."""
    conn = duckdb.connect(":memory:", config={'allow_unsigned_extensions': 'true'})
    conn.execute(f"LOAD '{extension_path}'")

    # These should not crash, but may log warnings
    conn.execute("SET duck_lineage_url = 'not-a-valid-url'")
    conn.execute("SET duck_lineage_url = ''")
    conn.execute("SET duck_lineage_url = 'http://'")

    # Should still be able to execute queries
    result = conn.execute("SELECT 1").fetchone()
    assert result[0] == 1

    conn.close()


@pytest.mark.integration
def test_malformed_query_handling(duckdb_with_extension):
    """Test that malformed queries don't crash the extension."""
    conn = duckdb_with_extension

    # Try various malformed queries
    with pytest.raises(Exception):
        conn.execute("SELECT * FROM nonexistent_table")

    with pytest.raises(Exception):
        conn.execute("INSERT INTO no_table VALUES (1)")

    with pytest.raises(Exception):
        conn.execute("CREATE TABLE test (id INT); CREATE TABLE test (id INT)")  # Duplicate table

    # Extension should still work after errors
    result = conn.execute("SELECT 42").fetchone()
    assert result[0] == 42


@pytest.mark.integration
def test_empty_namespace(extension_path, marquez_api_url):
    """Test behavior with empty namespace."""
    conn = duckdb.connect(":memory:", config={'allow_unsigned_extensions': 'true'})
    conn.execute(f"LOAD '{extension_path}'")
    conn.execute(f"SET duck_lineage_url = '{marquez_api_url}/lineage'")
    conn.execute("SET duck_lineage_namespace = ''")

    # Should still execute queries without crashing
    conn.execute("CREATE TABLE test (id INT)")
    conn.execute("INSERT INTO test VALUES (1)")
    result = conn.execute("SELECT * FROM test").fetchone()
    assert result[0] == 1

    conn.close()


@pytest.mark.integration
def test_special_characters_in_names(lineage_connection, marquez_client):
    """Test handling of special characters in table and column names."""
    conn = lineage_connection

    # Get the namespace being used
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    # Table names with special characters (quoted identifiers)
    conn.execute(
        """
        CREATE TABLE "my-special-table" (
            "column-with-dashes" INT,
            "column.with.dots" VARCHAR,
            "column with spaces" DECIMAL(10,2)
        )
    """
    )
    conn.execute("INSERT INTO \"my-special-table\" VALUES (1, 'test', 99.99)")

    result = conn.execute('SELECT COUNT(*) FROM "my-special-table"').fetchone()
    assert result[0] == 1

    # Verify dataset exists in Marquez
    dataset = marquez_client.wait_for_dataset(namespace, "memory.main.my-special-table")
    assert dataset is not None, "Dataset for 'my-special-table' should be registered in Marquez."


@pytest.mark.integration
def test_very_long_query(lineage_connection, marquez_client):
    """Test handling of very long queries."""
    conn = lineage_connection

    # Get the namespace being used
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    # Create a table with many columns
    columns = ", ".join([f"col{i} INT" for i in range(20)])
    conn.execute(f"CREATE TABLE wide_table ({columns})")

    # Insert with many values
    values = ", ".join(["1"] * 20)
    conn.execute(f"INSERT INTO wide_table VALUES ({values})")

    # Select all columns (long query)
    result = conn.execute("SELECT * FROM wide_table").fetchone()
    assert len(result) == 20

    # Verify dataset exists in Marquez
    dataset = marquez_client.wait_for_dataset(namespace, "memory.main.wide_table")
    assert dataset is not None, "Dataset for 'wide_table' should be registered in Marquez."


@pytest.mark.integration
def test_transaction_rollback(lineage_connection):
    """Test lineage behavior with transaction rollback."""
    conn = lineage_connection

    # Create table
    conn.execute(
        """
        CREATE TABLE accounts (
            account_id INT,
            balance DECIMAL(10,2)
        )
    """
    )
    conn.execute("INSERT INTO accounts VALUES (1, 1000.00)")

    # Begin transaction
    conn.execute("BEGIN TRANSACTION")
    conn.execute("UPDATE accounts SET balance = 500.00 WHERE account_id = 1")

    # Rollback
    conn.execute("ROLLBACK")

    # Verify rollback worked
    result = conn.execute("SELECT balance FROM accounts WHERE account_id = 1").fetchone()
    assert result[0] == 1000.00

    # Extension should still work
    conn.execute("SELECT * FROM accounts")


@pytest.mark.integration
def test_concurrent_connections(extension_path, marquez_api_url):
    """Test multiple concurrent connections with the extension."""
    namespace = "test_concurrent"

    # Create multiple connections
    connections = []
    for _ in range(2):
        conn = duckdb.connect(":memory:", config={'allow_unsigned_extensions': 'true'})
        conn.execute(f"LOAD '{extension_path}'")
        conn.execute(f"SET duck_lineage_url = '{marquez_api_url}/lineage'")
        conn.execute(f"SET duck_lineage_namespace = '{namespace}'")
        connections.append(conn)

    # Execute queries on each connection
    for i, conn in enumerate(connections):
        conn.execute(f"CREATE TABLE test_{i} (id INT)")
        conn.execute(f"INSERT INTO test_{i} VALUES ({i})")
        result = conn.execute(f"SELECT * FROM test_{i}").fetchone()
        assert result[0] == i

    # Close all connections
    for conn in connections:
        conn.close()


@pytest.mark.integration
def test_null_values_handling(lineage_connection, marquez_client):
    """Test handling of NULL values in lineage tracking."""
    conn = lineage_connection

    # Get the namespace being used
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    # Create table with nullable columns
    conn.execute(
        """
        CREATE TABLE nullable_data (
            id INT,
            name VARCHAR,
            value INT,
            metadata VARCHAR
        )
    """
    )

    # Insert with NULL values
    conn.execute(
        """
        INSERT INTO nullable_data VALUES
            (1, 'Alice', NULL, NULL),
            (2, NULL, 100, 'meta')
    """
    )

    # Query with NULL handling
    conn.execute(
        """
        CREATE TABLE filtered_data AS
        SELECT
            COALESCE(name, 'Unknown') as name,
            COALESCE(value, 0) as value
        FROM nullable_data
        WHERE id IS NOT NULL
    """
    )

    result = conn.execute("SELECT COUNT(*) FROM filtered_data").fetchone()
    assert result[0] == 2

    # Verify filtered_data dataset exists in Marquez (using wait_for_dataset for CTAS)
    filtered_dataset = marquez_client.wait_for_dataset(namespace, "memory.main.filtered_data")
    assert filtered_dataset is not None, "Dataset for 'filtered_data' should be registered in Marquez."


@pytest.mark.integration
def test_configuration_persistence(extension_path, marquez_api_url):
    """Test that configuration settings persist across queries."""
    conn = duckdb.connect(":memory:", config={'allow_unsigned_extensions': 'true'})
    conn.execute(f"LOAD '{extension_path}'")

    # Set configuration
    test_url = f"{marquez_api_url}/lineage"
    test_namespace = "test_persistence"

    conn.execute(f"SET duck_lineage_url = '{test_url}'")
    conn.execute(f"SET duck_lineage_namespace = '{test_namespace}'")
    conn.execute("SET duck_lineage_debug = true")

    # Execute some queries
    conn.execute("CREATE TABLE test1 (id INT)")
    conn.execute("INSERT INTO test1 VALUES (1)")

    # Configuration should still be active
    conn.execute("CREATE TABLE test2 (id INT)")
    conn.execute("INSERT INTO test2 SELECT * FROM test1")

    result = conn.execute("SELECT * FROM test2").fetchone()
    assert result[0] == 1

    conn.close()
