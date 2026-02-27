"""
Basic tests for DuckDB DuckLineage extension.
"""

import pytest
from time import sleep

from event_helpers import (
    assert_valid_dataset,
    assert_dataset_has_fields,
    assert_dataset_has_facet,
    assert_dataset_lifecycle,
    assert_valid_job,
    assert_job_has_io,
    assert_job_run_completed,
    assert_job_has_sql_facet,
)


@pytest.mark.integration
def test_extension_loads(duckdb_with_extension):
    """Test that the extension loads successfully and is visible in DuckDB."""
    conn = duckdb_with_extension

    result = conn.execute("SELECT 1 as test").fetchone()
    assert result[0] == 1

    # Verify the extension is listed as loaded
    extensions = conn.execute(
        "SELECT extension_name, loaded FROM duckdb_extensions() WHERE extension_name = 'duck_lineage'"
    ).fetchone()
    assert extensions is not None, "duck_lineage extension should appear in duckdb_extensions()"
    assert extensions[1] is True, "duck_lineage extension should be marked as loaded"


@pytest.mark.integration
def test_configuration_set(duckdb_with_extension):
    """Test that DuckLineage configuration can be set and read back."""
    conn = duckdb_with_extension

    conn.execute("SET duck_lineage_url = 'http://test.example.com/api/v1/lineage'")
    conn.execute("SET duck_lineage_namespace = 'test_namespace'")
    conn.execute("SET duck_lineage_debug = true")
    conn.execute("SET duck_lineage_api_key = 'test-key'")

    # Verify settings were persisted
    url = conn.execute("SELECT current_setting('duck_lineage_url')").fetchone()[0]
    assert url == 'http://test.example.com/api/v1/lineage'

    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]
    assert namespace == 'test_namespace'

    debug = conn.execute("SELECT current_setting('duck_lineage_debug')").fetchone()[0]
    assert debug in (True, 'true'), f"Expected debug=true, got {debug!r}"


@pytest.mark.integration
def test_simple_query(sample_table, marquez_client):
    """Test that a simple query produces a fully-populated dataset and job in Marquez."""
    conn = sample_table

    result = conn.execute("SELECT COUNT(*) FROM test_employees").fetchone()
    assert result[0] == 4

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

    # ── Validate the dataset object in Marquez ──
    dataset = marquez_client.wait_for_dataset_with_fields("duckdb_test", "memory.main.test_employees")
    assert dataset is not None, "test_employees should be registered as a dataset in Marquez"

    assert_valid_dataset(dataset, "duckdb_test", "test_employees")
    assert_dataset_has_fields(
        dataset,
        {
            "id": "INTEGER",
            "name": "VARCHAR",
            "department": "VARCHAR",
            "salary": "DECIMAL",
        },
    )
    assert_dataset_lifecycle(dataset, "OVERWRITE")
    assert_dataset_has_facet(dataset, "schema")
    assert_dataset_has_facet(dataset, "dataSource")
    assert_dataset_has_facet(dataset, "catalog", {"name": "memory", "type": "memory", "framework": "duckdb"})
    assert_dataset_has_facet(dataset, "datasetType", {"datasetType": "TABLE"})

    # ── Validate job objects in Marquez ──
    sleep(2)
    jobs = marquez_client.list_jobs("duckdb_test")
    assert jobs, "Should have at least one job in Marquez"

    # Find the INSERT job for test_employees
    insert_jobs = [j for j in jobs if "INSERT" in (j.get("name") or "") and "test_employees" in (j.get("name") or "")]
    assert insert_jobs, f"No INSERT job for test_employees. Jobs: {[j.get('name') for j in jobs]}"

    job = insert_jobs[0]
    assert_valid_job(job, "duckdb_test")
    assert_job_has_io(job, expected_output_contains="test_employees")
    assert_job_run_completed(job)
    assert_job_has_sql_facet(job, "test_employees")
