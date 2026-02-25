"""
Tests for file operations (CSV, Parquet) lineage.
"""

import pytest

FILE_NAMESPACE = "file"


@pytest.mark.integration
def test_csv_read_lineage(lineage_connection, marquez_client, tmp_path):
    """Test lineage tracking for reading CSV files."""
    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n")

    conn = lineage_connection

    # Read CSV into a table
    conn.execute(
        f"""
        CREATE TABLE imported_data AS
        SELECT * FROM read_csv_auto('{csv_file}')
    """
    )

    # Verify data was imported
    result = conn.execute("SELECT COUNT(*) FROM imported_data").fetchone()
    assert result[0] == 2

    assert (
        marquez_client.get_dataset(FILE_NAMESPACE, str(csv_file)) is not None
    ), f"Should have created dataset for CSV file {csv_file} in 'file' namespace"


@pytest.mark.integration
def test_csv_write_lineage(lineage_connection, marquez_client, tmp_path):
    """Test lineage tracking for writing CSV files."""
    csv_file = tmp_path / "output_data.csv"
    conn = lineage_connection

    # Create source data
    conn.execute("CREATE TABLE export_data (id INT, name VARCHAR, score DECIMAL(5,2))")
    conn.execute("INSERT INTO export_data VALUES (1, 'Test1', 95.5)")

    # Export to CSV
    conn.execute(f"COPY export_data TO '{csv_file}' (HEADER, DELIMITER ',')")

    # Verify file was created
    assert csv_file.exists()
    assert (
        marquez_client.get_dataset(FILE_NAMESPACE, str(csv_file)) is not None
    ), f"Should have created dataset for CSV file {csv_file} in 'file' namespace"


@pytest.mark.integration
def test_parquet_read_lineage(lineage_connection, marquez_client, tmp_path):
    """Test lineage tracking for reading Parquet files."""
    parquet_file = tmp_path / "test_data.parquet"

    # First create a parquet file
    import duckdb

    setup_conn = duckdb.connect(":memory:")
    setup_conn.execute("CREATE TABLE temp_data (id INT, name VARCHAR, value DOUBLE)")
    setup_conn.execute("INSERT INTO temp_data VALUES (1, 'Alpha', 10.5), (2, 'Beta', 20.7)")
    setup_conn.execute(f"COPY temp_data TO '{parquet_file}' (FORMAT PARQUET)")
    setup_conn.close()

    # Now test reading it with the extension
    conn = lineage_connection

    # Read Parquet into a table
    conn.execute(
        f"""
        CREATE TABLE from_parquet AS
        SELECT * FROM read_parquet('{parquet_file}')
    """
    )

    # Verify data was imported
    result = conn.execute("SELECT COUNT(*) FROM from_parquet").fetchone()
    assert result[0] == 2

    assert (
        marquez_client.get_dataset(FILE_NAMESPACE, str(parquet_file)) is not None
    ), f"Should have created dataset for Parquet file {parquet_file} in 'file' namespace"


@pytest.mark.integration
def test_parquet_write_lineage(lineage_connection, marquez_client, tmp_path):
    """Test lineage tracking for writing Parquet files."""
    parquet_file = tmp_path / "output_data.parquet"
    conn = lineage_connection

    # Create source data
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

    # Export to Parquet
    conn.execute(f"COPY analytics_results TO '{parquet_file}' (FORMAT PARQUET)")

    # Verify file was created
    assert parquet_file.exists()
    assert (
        marquez_client.get_dataset(FILE_NAMESPACE, str(parquet_file)) is not None
    ), f"Should have created dataset for Parquet file {parquet_file} in 'file' namespace"


@pytest.mark.integration
def test_multiple_file_operations(lineage_connection, marquez_client, tmp_path):
    """Test lineage tracking for ETL pipeline with file operations."""
    input_csv = tmp_path / "raw_data.csv"
    output_parquet = tmp_path / "processed_data.parquet"
    input_csv.write_text("id,category,amount\n1,A,100\n2,B,200\n3,A,150\n")

    conn = lineage_connection

    # ETL pipeline: read CSV -> transform -> write Parquet
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

    # Verify pipeline executed correctly
    assert output_parquet.exists()
    result = conn.execute("SELECT COUNT(*) FROM processed_data").fetchone()
    assert result[0] == 2  # Two categories: A, B

    assert (
        marquez_client.get_dataset(FILE_NAMESPACE, str(input_csv)) is not None
    ), f"Should have created dataset for input CSV file {input_csv} in 'file' namespace"

    assert (
        marquez_client.get_dataset(FILE_NAMESPACE, str(output_parquet)) is not None
    ), f"Should have created dataset for output Parquet file {output_parquet} in 'file' namespace"
