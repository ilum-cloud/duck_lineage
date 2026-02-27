"""
Tests for file operations (CSV, Parquet) lineage.
"""

import pytest
from time import sleep

from event_helpers import (
    assert_valid_dataset,
    assert_dataset_has_fields,
    assert_dataset_has_facet,
    assert_valid_job,
    assert_job_has_io,
    assert_job_run_completed,
    assert_job_has_sql_facet,
)

FILE_NAMESPACE = "file"


@pytest.mark.integration
def test_csv_read_lineage(lineage_connection, marquez_client, tmp_path):
    """Test lineage tracking for reading CSV files into a table."""
    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n")

    conn = lineage_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    conn.execute(
        f"""
        CREATE TABLE imported_data AS
        SELECT * FROM read_csv_auto('{csv_file}')
    """
    )

    result = conn.execute("SELECT COUNT(*) FROM imported_data").fetchone()
    assert result[0] == 2

    # Validate the input CSV file dataset
    csv_dataset = marquez_client.wait_for_dataset(FILE_NAMESPACE, str(csv_file))
    assert csv_dataset is not None, f"Should have created dataset for CSV file {csv_file} in 'file' namespace"
    assert (
        csv_dataset.get("namespace") == FILE_NAMESPACE
    ), f"CSV dataset namespace should be 'file', got: {csv_dataset.get('namespace')!r}"

    # Validate the output table dataset
    output = marquez_client.wait_for_dataset_with_fields(namespace, "memory.main.imported_data")
    assert output is not None, "Output table 'imported_data' should be registered in Marquez"

    assert_valid_dataset(output, namespace, "imported_data")
    assert_dataset_has_fields(output, {"id": "BIGINT", "name": "VARCHAR", "value": "BIGINT"})
    assert_dataset_has_facet(output, "datasetType", {"datasetType": "TABLE"})
    assert_dataset_has_facet(output, "catalog", {"type": "memory", "framework": "duckdb"})


@pytest.mark.integration
def test_csv_write_lineage(lineage_connection, marquez_client, tmp_path):
    """Test lineage tracking for writing CSV files."""
    csv_file = tmp_path / "output_data.csv"
    conn = lineage_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    conn.execute("CREATE TABLE export_data (id INT, name VARCHAR, score DECIMAL(5,2))")
    conn.execute("INSERT INTO export_data VALUES (1, 'Test1', 95.5)")

    conn.execute(f"COPY export_data TO '{csv_file}' (HEADER, DELIMITER ',')")

    assert csv_file.exists()

    # Validate the output CSV file dataset
    csv_dataset = marquez_client.wait_for_dataset(FILE_NAMESPACE, str(csv_file))
    assert csv_dataset is not None, f"Should have created dataset for CSV file {csv_file} in 'file' namespace"
    assert csv_dataset.get("namespace") == FILE_NAMESPACE

    # Validate the source table dataset
    source = marquez_client.wait_for_dataset_with_fields(namespace, "memory.main.export_data")
    assert source is not None, "Source table 'export_data' should be registered in Marquez"
    assert_valid_dataset(source, namespace, "export_data")
    assert_dataset_has_fields(source, {"id": "INTEGER", "name": "VARCHAR", "score": "DECIMAL"})


@pytest.mark.integration
def test_parquet_read_lineage(lineage_connection, marquez_client, tmp_path):
    """Test lineage tracking for reading Parquet files."""
    parquet_file = tmp_path / "test_data.parquet"

    import duckdb

    setup_conn = duckdb.connect(":memory:")
    setup_conn.execute("CREATE TABLE temp_data (id INT, name VARCHAR, value DOUBLE)")
    setup_conn.execute("INSERT INTO temp_data VALUES (1, 'Alpha', 10.5), (2, 'Beta', 20.7)")
    setup_conn.execute(f"COPY temp_data TO '{parquet_file}' (FORMAT PARQUET)")
    setup_conn.close()

    conn = lineage_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    conn.execute(
        f"""
        CREATE TABLE from_parquet AS
        SELECT * FROM read_parquet('{parquet_file}')
    """
    )

    result = conn.execute("SELECT COUNT(*) FROM from_parquet").fetchone()
    assert result[0] == 2

    # Validate input Parquet file dataset
    pq_dataset = marquez_client.wait_for_dataset(FILE_NAMESPACE, str(parquet_file))
    assert pq_dataset is not None, f"Should have created dataset for Parquet file in 'file' namespace"
    assert pq_dataset.get("namespace") == FILE_NAMESPACE

    # Validate output table dataset
    output = marquez_client.wait_for_dataset_with_fields(namespace, "memory.main.from_parquet")
    assert output is not None, "Output table 'from_parquet' should be registered in Marquez"

    assert_valid_dataset(output, namespace, "from_parquet")
    assert_dataset_has_fields(output, {"id": "INTEGER", "name": "VARCHAR", "value": "DOUBLE"})
    assert_dataset_has_facet(output, "datasetType", {"datasetType": "TABLE"})
    assert_dataset_has_facet(output, "schema")
    assert_dataset_has_facet(output, "dataSource")


@pytest.mark.integration
def test_parquet_write_lineage(lineage_connection, marquez_client, tmp_path):
    """Test lineage tracking for writing Parquet files."""
    parquet_file = tmp_path / "output_data.parquet"
    conn = lineage_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    conn.execute(
        """
        CREATE TABLE analytics_results (
            metric_name VARCHAR,
            metric_value DOUBLE,
            timestamp TIMESTAMP
        )
    """
    )
    conn.execute("INSERT INTO analytics_results VALUES ('conversion_rate', 0.125, '2024-01-01 10:00:00')")

    conn.execute(f"COPY analytics_results TO '{parquet_file}' (FORMAT PARQUET)")

    assert parquet_file.exists()

    pq_dataset = marquez_client.wait_for_dataset(FILE_NAMESPACE, str(parquet_file))
    assert pq_dataset is not None, f"Should have created dataset for Parquet file in 'file' namespace"
    assert pq_dataset.get("namespace") == FILE_NAMESPACE

    # Validate the source table
    source = marquez_client.wait_for_dataset_with_fields(namespace, "memory.main.analytics_results")
    assert source is not None, "Source table should be registered in Marquez"
    assert_valid_dataset(source, namespace, "analytics_results")
    assert_dataset_has_fields(source, {"metric_name": "VARCHAR", "metric_value": "DOUBLE", "timestamp": "TIMESTAMP"})


@pytest.mark.integration
def test_multiple_file_operations(lineage_connection, marquez_client, tmp_path):
    """Test lineage tracking for a full ETL pipeline: CSV → transform → Parquet."""
    input_csv = tmp_path / "raw_data.csv"
    output_parquet = tmp_path / "processed_data.parquet"
    input_csv.write_text("id,category,amount\n1,A,100\n2,B,200\n3,A,150\n")

    conn = lineage_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    conn.execute(f"CREATE TABLE raw_data AS SELECT * FROM read_csv_auto('{input_csv}')")

    conn.execute(
        """
        CREATE TABLE processed_data AS
        SELECT
            category,
            COUNT(*) as count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM raw_data
        GROUP BY category
    """
    )

    conn.execute(f"COPY processed_data TO '{output_parquet}' (FORMAT PARQUET)")

    assert output_parquet.exists()
    result = conn.execute("SELECT COUNT(*) FROM processed_data").fetchone()
    assert result[0] == 2

    # Validate input CSV file dataset
    csv_dataset = marquez_client.wait_for_dataset(FILE_NAMESPACE, str(input_csv))
    assert csv_dataset is not None, f"Should have created dataset for input CSV"
    assert csv_dataset.get("namespace") == FILE_NAMESPACE

    # Validate output Parquet file dataset
    pq_dataset = marquez_client.wait_for_dataset(FILE_NAMESPACE, str(output_parquet))
    assert pq_dataset is not None, f"Should have created dataset for output Parquet"
    assert pq_dataset.get("namespace") == FILE_NAMESPACE

    # Validate the intermediate processed_data table deeply
    processed = marquez_client.wait_for_dataset_with_fields(namespace, "memory.main.processed_data")
    assert processed is not None, "processed_data table should be registered in Marquez"

    assert_valid_dataset(processed, namespace, "processed_data")
    assert_dataset_has_fields(processed, {"category": "VARCHAR", "count": "BIGINT", "total_amount": "HUGEINT"})
    assert_dataset_has_facet(processed, "schema")
    assert_dataset_has_facet(processed, "datasetType", {"datasetType": "TABLE"})
    assert_dataset_has_facet(processed, "catalog", {"type": "memory", "framework": "duckdb"})

    # Validate ETL pipeline jobs (filter out SET/SELECT noise)
    sleep(3)
    jobs = marquez_client.list_jobs(namespace)
    etl_jobs = [j for j in jobs if any(kw in (j.get("name") or "") for kw in ("CREATE_TABLE", "COPY"))]
    assert len(etl_jobs) >= 3, (
        f"Expected at least 3 ETL jobs (CTAS raw_data, CTAS processed_data, COPY). "
        f"Got {len(etl_jobs)}. All: {[j.get('name') for j in jobs]}"
    )

    for job in etl_jobs:
        assert_valid_job(job, namespace)
        assert_job_run_completed(job)

    # Find and validate the CTAS job that creates processed_data from raw_data
    ctas_jobs = [
        j for j in etl_jobs if "processed_data" in (j.get("name") or "") and "raw_data" in (j.get("name") or "")
    ]
    if ctas_jobs:
        ctas = ctas_jobs[0]
        assert_job_has_io(ctas, expected_input_contains="raw_data", expected_output_contains="processed_data")
        assert_job_has_sql_facet(ctas, "processed_data")
