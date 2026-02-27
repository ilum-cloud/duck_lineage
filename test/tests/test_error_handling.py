"""
Tests for error handling and edge cases.
"""

import pytest
import duckdb
from time import sleep

from event_helpers import (
    assert_valid_dataset,
    assert_dataset_has_fields,
    assert_dataset_has_facet,
    assert_valid_job,
    assert_job_run_completed,
)


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

    with pytest.raises(Exception):
        conn.execute("SELECT * FROM nonexistent_table")

    with pytest.raises(Exception):
        conn.execute("INSERT INTO no_table VALUES (1)")

    with pytest.raises(Exception):
        conn.execute("CREATE TABLE test (id INT); CREATE TABLE test (id INT)")

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
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

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

    dataset = marquez_client.wait_for_dataset_with_fields(namespace, "memory.main.my-special-table")
    assert dataset is not None, "Dataset for 'my-special-table' should be registered in Marquez."

    assert_valid_dataset(dataset, namespace, "my-special-table")
    assert_dataset_has_fields(
        dataset,
        {
            "column-with-dashes": "INTEGER",
            "column.with.dots": "VARCHAR",
            "column with spaces": "DECIMAL",
        },
    )
    assert_dataset_has_facet(dataset, "schema")
    assert_dataset_has_facet(dataset, "datasetType", {"datasetType": "TABLE"})


@pytest.mark.integration
def test_very_long_query(lineage_connection, marquez_client):
    """Test handling of very long queries with many columns."""
    conn = lineage_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    NUM_COLUMNS = 20
    columns = ", ".join([f"col{i} INT" for i in range(NUM_COLUMNS)])
    conn.execute(f"CREATE TABLE wide_table ({columns})")

    values = ", ".join(["1"] * NUM_COLUMNS)
    conn.execute(f"INSERT INTO wide_table VALUES ({values})")

    result = conn.execute("SELECT * FROM wide_table").fetchone()
    assert len(result) == NUM_COLUMNS

    dataset = marquez_client.wait_for_dataset_with_fields(namespace, "memory.main.wide_table")
    assert dataset is not None, "Dataset for 'wide_table' should be registered in Marquez."

    assert_valid_dataset(dataset, namespace, "wide_table")
    expected_fields = {f"col{i}": "INTEGER" for i in range(NUM_COLUMNS)}
    assert_dataset_has_fields(dataset, expected_fields)
    assert_dataset_has_facet(dataset, "schema")
    assert_dataset_has_facet(dataset, "catalog", {"type": "memory", "framework": "duckdb"})


@pytest.mark.integration
def test_transaction_rollback(lineage_connection, marquez_client):
    """Test lineage behavior with transaction rollback."""
    conn = lineage_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    conn.execute(
        """
        CREATE TABLE accounts (
            account_id INT,
            balance DECIMAL(10,2)
        )
    """
    )
    conn.execute("INSERT INTO accounts VALUES (1, 1000.00)")

    conn.execute("BEGIN TRANSACTION")
    conn.execute("UPDATE accounts SET balance = 500.00 WHERE account_id = 1")
    conn.execute("ROLLBACK")

    # Verify rollback worked
    result = conn.execute("SELECT balance FROM accounts WHERE account_id = 1").fetchone()
    assert result[0] == 1000.00

    # The CREATE TABLE + INSERT should still have generated lineage
    dataset = marquez_client.wait_for_dataset(namespace, "memory.main.accounts")
    assert dataset is not None, "accounts dataset should exist in Marquez despite the rollback"

    assert_valid_dataset(dataset, namespace, "accounts")
    assert_dataset_has_facet(dataset, "datasetType", {"datasetType": "TABLE"})


@pytest.mark.integration
def test_concurrent_connections(extension_path, marquez_api_url, marquez_client):
    """Test multiple concurrent connections with the extension."""
    import uuid

    NUM_CONNECTIONS = 2
    namespace = f"test_concurrent_{uuid.uuid4().hex[:8]}"

    connections = []
    for _ in range(NUM_CONNECTIONS):
        conn = duckdb.connect(":memory:", config={'allow_unsigned_extensions': 'true'})
        conn.execute(f"LOAD '{extension_path}'")
        conn.execute(f"SET duck_lineage_url = '{marquez_api_url}/lineage'")
        conn.execute(f"SET duck_lineage_namespace = '{namespace}'")
        connections.append(conn)

    for i, conn in enumerate(connections):
        conn.execute(f"CREATE TABLE test_{i} (id INT)")
        conn.execute(f"INSERT INTO test_{i} VALUES ({i})")
        result = conn.execute(f"SELECT * FROM test_{i}").fetchone()
        assert result[0] == i

    for conn in connections:
        conn.close()

    sleep(2)

    for i in range(NUM_CONNECTIONS):
        dataset = marquez_client.wait_for_dataset(namespace, f"memory.main.test_{i}")
        assert dataset is not None, f"Dataset test_{i} should exist in Marquez"
        assert_valid_dataset(dataset, namespace, f"test_{i}")
        assert_dataset_has_facet(dataset, "datasetType", {"datasetType": "TABLE"})


@pytest.mark.integration
def test_null_values_handling(lineage_connection, marquez_client):
    """Test handling of NULL values and verify CTAS lineage with dataset objects."""
    conn = lineage_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

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

    conn.execute(
        """
        INSERT INTO nullable_data VALUES
            (1, 'Alice', NULL, NULL),
            (2, NULL, 100, 'meta')
    """
    )

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

    # Validate source dataset
    source = marquez_client.wait_for_dataset_with_fields(namespace, "memory.main.nullable_data")
    assert source is not None, "Source dataset 'nullable_data' should be registered in Marquez."
    assert_valid_dataset(source, namespace, "nullable_data")
    assert_dataset_has_fields(source, {"id": "INTEGER", "name": "VARCHAR", "value": "INTEGER", "metadata": "VARCHAR"})

    # Validate derived dataset
    derived = marquez_client.wait_for_dataset_with_fields(namespace, "memory.main.filtered_data")
    assert derived is not None, "Derived dataset 'filtered_data' should be registered in Marquez."
    assert_valid_dataset(derived, namespace, "filtered_data")
    assert_dataset_has_fields(derived, {"name": "VARCHAR", "value": "INTEGER"})
    assert_dataset_has_facet(derived, "datasetType", {"datasetType": "TABLE"})
    assert_dataset_has_facet(derived, "catalog", {"type": "memory", "framework": "duckdb"})


@pytest.mark.integration
def test_configuration_persistence(extension_path, marquez_api_url, marquez_client):
    """Test that configuration settings persist across queries and produce correct dataset objects."""
    import uuid

    namespace = f"test_persist_{uuid.uuid4().hex[:8]}"

    conn = duckdb.connect(":memory:", config={'allow_unsigned_extensions': 'true'})
    conn.execute(f"LOAD '{extension_path}'")

    test_url = f"{marquez_api_url}/lineage"
    conn.execute(f"SET duck_lineage_url = '{test_url}'")
    conn.execute(f"SET duck_lineage_namespace = '{namespace}'")
    conn.execute("SET duck_lineage_debug = true")

    conn.execute("CREATE TABLE test1 (id INT)")
    conn.execute("INSERT INTO test1 VALUES (1)")

    conn.execute("CREATE TABLE test2 (id INT)")
    conn.execute("INSERT INTO test2 SELECT * FROM test1")

    result = conn.execute("SELECT * FROM test2").fetchone()
    assert result[0] == 1

    conn.close()

    # Both tables should have been tracked under the same namespace with full metadata
    ds1 = marquez_client.wait_for_dataset(namespace, "memory.main.test1")
    assert ds1 is not None, "test1 should be tracked after initial configuration"
    assert_valid_dataset(ds1, namespace, "test1")

    ds2 = marquez_client.wait_for_dataset(namespace, "memory.main.test2")
    assert ds2 is not None, "test2 should be tracked — config should persist across queries"
    assert_valid_dataset(ds2, namespace, "test2")

    # Validate both have correct facets under the same namespace
    assert_dataset_has_facet(ds1, "datasetType", {"datasetType": "TABLE"})
    assert_dataset_has_facet(ds2, "datasetType", {"datasetType": "TABLE"})

    # Verify DDL/DML jobs were created (filter out SET/SELECT noise)
    sleep(3)
    jobs = marquez_client.list_jobs(namespace)
    ddl_jobs = [j for j in jobs if any(kw in (j.get("name") or "") for kw in ("CREATE", "INSERT"))]
    assert (
        len(ddl_jobs) >= 2
    ), f"Expected at least 2 DDL/DML jobs. Got {len(ddl_jobs)}. All: {[j.get('name') for j in jobs]}"
    for job in ddl_jobs:
        assert_valid_job(job, namespace)
        assert_job_run_completed(job)
