"""
Tests for DuckLake storage extension lineage.

DuckLake is a DuckDB storage extension that provides a local file-based storage layer.
These tests verify that lineage events are correctly captured for DuckLake operations.

Prerequisites:
    - DuckLake extension must be available to DuckDB (auto-installed by tests)
    - Marquez must be running (started automatically by make targets)

Run tests:
    make test-smoke       # Run smoke tests (includes test_ducklake_dataset_facets)
    make test-integration # Run all integration tests (includes all DuckLake tests)

Note: Tests will skip if DuckLake extension is not available.
"""

import pytest
from pathlib import Path


DUCKLAKE_NAMESPACE = "ducklake"


@pytest.fixture
def ducklake_db_file(tmp_path):
    """
    Create a temporary file path for DuckLake metadata.
    """
    return str(tmp_path / "test_ducklake.ducklake")


@pytest.fixture
def ducklake_data_path(tmp_path):
    """
    Create a temporary directory for DuckLake data storage.
    """
    ducklake_path = tmp_path / "ducklake_data"
    ducklake_path.mkdir()
    return str(ducklake_path)


@pytest.fixture
def duckdb_with_ducklake(lineage_connection, ducklake_db_file, ducklake_data_path):
    """
    Create a DuckDB connection with DuckLake attached.

    Skips the test if DuckLake extension is not available.
    Yields (conn, ducklake_namespace) where ducklake_namespace is the DATA_PATH
    used as the dataset namespace for DuckLake tables.
    """
    conn = lineage_connection

    # Try to load DuckLake extension
    try:
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")
    except Exception as e:
        pytest.skip(f"DuckLake extension not available: {e}")

    # Attach DuckLake storage: ATTACH 'ducklake:<path>.ducklake' AS name (DATA_PATH '<data-path>')
    try:
        conn.execute(f"ATTACH 'ducklake:{ducklake_db_file}' AS ducklake_db (DATA_PATH '{ducklake_data_path}')")
    except Exception as e:
        pytest.skip(f"Could not attach DuckLake: {e}")

    yield conn, ducklake_data_path

    # Detach and cleanup
    try:
        conn.execute("DETACH ducklake_db")
    except Exception:
        pass  # Already detached or error


@pytest.mark.integration
@pytest.mark.smoke
def test_ducklake_dataset_facets(duckdb_with_ducklake, marquez_client):
    """Test that DuckLake datasets have correct facets (schema, dataSource, catalog)."""
    conn, ducklake_ns = duckdb_with_ducklake
    namespace = ducklake_ns

    # Create a table in DuckLake
    conn.execute(
        """
        CREATE TABLE ducklake_db.customers (
            id INTEGER,
            name VARCHAR,
            email VARCHAR,
            created_date DATE
        )
    """
    )

    # Get the dataset from Marquez
    dataset_name = "ducklake_db.main.customers"
    dataset = marquez_client.wait_for_dataset_with_facets(
        namespace, dataset_name, ["dataSource", "catalog"], timeout_seconds=30
    )

    assert dataset is not None, f"Dataset {dataset_name} should exist in Marquez"

    # Check schema facet
    assert "fields" in dataset, "Dataset should have a fields array"
    fields = dataset["fields"]

    # Verify field names and types
    field_names = {f["name"] for f in fields}
    assert "id" in field_names, "Should have 'id' field"
    assert "name" in field_names, "Should have 'name' field"
    assert "email" in field_names, "Should have 'email' field"
    assert "created_date" in field_names or "createdDate" in field_names, "Should have date field"

    # Check field types
    id_field = next((f for f in fields if f["name"] == "id"), None)
    assert id_field is not None, "id field should exist"
    assert id_field["type"].lower() in [
        "integer",
        "int",
        "int32",
    ], f"id field should be INTEGER, got {id_field['type']}"

    name_field = next((f for f in fields if f["name"] == "name"), None)
    assert name_field is not None, "name field should exist"
    assert (
        "varchar" in name_field["type"].lower() or "string" in name_field["type"].lower()
    ), f"name field should be VARCHAR, got {name_field['type']}"

    # Check dataSource facet
    facets = dataset.get("facets", {})
    assert "dataSource" in facets, "Dataset should have dataSource facet"

    data_source = facets["dataSource"]
    assert "uri" in data_source, "dataSource should have a URI"
    # The URI should contain file://
    uri = data_source["uri"]
    assert "file://" in uri, f"URI should have file:// scheme, got {uri}"

    # Check catalog facet
    assert "catalog" in facets, "Dataset should have catalog facet"
    catalog = facets["catalog"]
    assert catalog["name"] == "ducklake_db", f"Catalog name should be 'ducklake_db', got {catalog['name']}"
    assert catalog["type"] == "ducklake", f"Catalog type should be 'ducklake', got {catalog['type']}"


@pytest.mark.integration
def test_ducklake_insert_job_lineage(duckdb_with_ducklake, marquez_client):
    """Test that INSERT operations create datasets with correct lineage information."""
    conn, ducklake_ns = duckdb_with_ducklake
    namespace = ducklake_ns

    # Create and populate a table
    conn.execute(
        """
        CREATE TABLE ducklake_db.products (
            id INTEGER,
            product_name VARCHAR,
            price DECIMAL(10,2)
        )
    """
    )

    # Execute INSERT
    conn.execute(
        """
        INSERT INTO ducklake_db.products VALUES
            (1, 'Laptop', 999.99),
            (2, 'Mouse', 29.99)
    """
    )

    # Verify data was inserted
    result = conn.execute("SELECT COUNT(*) FROM ducklake_db.products").fetchone()
    assert result[0] == 2

    # Verify the dataset exists in Marquez
    dataset = marquez_client.wait_for_dataset_with_facets(
        namespace, "ducklake_db.main.products", ["dataSource"], timeout_seconds=30
    )
    assert dataset is not None, "Products dataset should exist in Marquez"

    # Verify it has the correct schema
    assert "fields" in dataset, "Dataset should have fields"
    field_names = {f["name"] for f in dataset["fields"]}
    assert "id" in field_names, "Should have id field"
    assert "product_name" in field_names or "productName" in field_names, "Should have product_name field"
    assert "price" in field_names, "Should have price field"

    # Verify dataSource facet
    facets = dataset.get("facets", {})
    assert "dataSource" in facets, "Should have dataSource facet"
    assert "file://" in facets["dataSource"]["uri"], "URI should have file:// scheme"


@pytest.mark.integration
def test_ducklake_select_job_with_inputs(duckdb_with_ducklake, marquez_client):
    """Test that SELECT queries track source datasets correctly."""
    conn, ducklake_ns = duckdb_with_ducklake
    namespace = ducklake_ns

    # Create source data
    conn.execute(
        """
        CREATE TABLE ducklake_db.orders (
            id INTEGER,
            customer_id INTEGER,
            total_amount DECIMAL(10,2)
        )
    """
    )

    conn.execute(
        """
        INSERT INTO ducklake_db.orders VALUES
            (1, 100, 150.00),
            (2, 101, 250.50)
    """
    )

    # Run a SELECT query
    conn.execute(
        """
        SELECT customer_id, SUM(total_amount) as total
        FROM ducklake_db.orders
        GROUP BY customer_id
    """
    )

    # Verify the source dataset exists and is tracked in Marquez
    dataset = marquez_client.wait_for_dataset_with_facets(
        namespace, "ducklake_db.main.orders", ["catalog"], timeout_seconds=30
    )
    assert dataset is not None, "Orders dataset should exist in Marquez"

    # Verify schema is correct
    assert "fields" in dataset, "Dataset should have fields"
    field_names = {f["name"] for f in dataset["fields"]}
    assert "id" in field_names, "Should have id field"
    assert "customer_id" in field_names or "customerId" in field_names, "Should have customer_id field"
    assert "total_amount" in field_names or "totalAmount" in field_names, "Should have total_amount field"

    # Verify catalog type
    facets = dataset.get("facets", {})
    assert "catalog" in facets, "Should have catalog facet"
    assert facets["catalog"]["type"] == "ducklake", "Catalog type should be ducklake"


@pytest.mark.integration
def test_ducklake_ctas_lineage_with_inputs_outputs(duckdb_with_ducklake, marquez_client):
    """Test CREATE TABLE AS SELECT correctly tracks both input and output datasets."""
    conn, ducklake_ns = duckdb_with_ducklake
    namespace = ducklake_ns

    # Create source table
    conn.execute(
        """
        CREATE TABLE ducklake_db.raw_sales (
            sale_id INTEGER,
            product VARCHAR,
            amount DECIMAL(10,2),
            region VARCHAR
        )
    """
    )

    conn.execute(
        """
        INSERT INTO ducklake_db.raw_sales VALUES
            (1, 'Widget', 100.00, 'North'),
            (2, 'Gadget', 150.00, 'South')
    """
    )

    # Create a summary table using CTAS
    conn.execute(
        """
        CREATE TABLE ducklake_db.regional_sales AS
        SELECT
            region,
            COUNT(*) as sale_count,
            SUM(amount) as total_amount
        FROM ducklake_db.raw_sales
        GROUP BY region
    """
    )

    # Verify the new table was created
    result = conn.execute("SELECT COUNT(*) FROM ducklake_db.regional_sales").fetchone()
    assert result[0] == 2

    # Verify both datasets exist in Marquez with catalog facets
    input_dataset = marquez_client.wait_for_dataset_with_facets(
        namespace, "ducklake_db.main.raw_sales", ["catalog"], timeout_seconds=30
    )
    assert input_dataset is not None, "Input dataset (raw_sales) should exist in Marquez"

    # Use wait_for_dataset_with_facets for CTAS output which may take longer to register
    output_dataset = marquez_client.wait_for_dataset_with_facets(
        namespace, "ducklake_db.main.regional_sales", ["catalog"], timeout_seconds=30
    )
    assert output_dataset is not None, "Output dataset (regional_sales) should exist in Marquez"

    # Check output schema has the correct fields
    fields = output_dataset.get("fields", [])
    field_names = {f["name"] for f in fields}
    assert "region" in field_names, "Should have region field"
    assert "sale_count" in field_names or "saleCount" in field_names, "Should have sale_count field"
    assert "total_amount" in field_names or "totalAmount" in field_names, "Should have total_amount field"

    # Verify both datasets have DuckLake catalog type
    input_facets = input_dataset.get("facets", {})
    assert "catalog" in input_facets, "Input dataset should have catalog facet"
    assert input_facets["catalog"]["type"] == "ducklake", "Input catalog type should be ducklake"

    output_facets = output_dataset.get("facets", {})
    assert "catalog" in output_facets, "Output dataset should have catalog facet"
    assert output_facets["catalog"]["type"] == "ducklake", "Output catalog type should be ducklake"


@pytest.mark.integration
def test_ducklake_cross_db_query_lineage(duckdb_with_ducklake, marquez_client):
    """Test that cross-database queries track datasets from both sources with different catalog types."""
    conn, ducklake_ns = duckdb_with_ducklake
    user_namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    # Create table in DuckLake
    conn.execute(
        """
        CREATE TABLE ducklake_db.inventory (
            product_id INTEGER,
            quantity INTEGER
        )
    """
    )

    conn.execute("INSERT INTO ducklake_db.inventory VALUES (1, 100), (2, 50)")

    # Create table in memory
    conn.execute(
        """
        CREATE TABLE local_suppliers (
            supplier_id INTEGER,
            product_id INTEGER,
            supplier_name VARCHAR
        )
    """
    )

    conn.execute("INSERT INTO local_suppliers VALUES (1, 1, 'Acme Corp')")

    # Join across DuckLake and local tables
    conn.execute(
        """
        SELECT
            i.product_id,
            i.quantity,
            s.supplier_name
        FROM ducklake_db.inventory i
        JOIN local_suppliers s ON i.product_id = s.product_id
    """
    )

    # Verify both datasets exist in Marquez with catalog facets
    # DuckLake datasets use the data path as namespace
    inventory_dataset = marquez_client.wait_for_dataset_with_facets(
        ducklake_ns, "ducklake_db.main.inventory", ["catalog"], timeout_seconds=30
    )
    assert inventory_dataset is not None, "Inventory dataset should exist in Marquez"

    # Memory datasets use the user-configured namespace
    suppliers_dataset = marquez_client.wait_for_dataset_with_facets(
        user_namespace, "memory.main.local_suppliers", ["catalog"], timeout_seconds=30
    )
    assert suppliers_dataset is not None, "Suppliers dataset should exist in Marquez"

    # Verify DuckLake dataset has correct catalog type
    inventory_facets = inventory_dataset.get("facets", {})
    assert "catalog" in inventory_facets, "Inventory dataset should have catalog facet"
    assert (
        inventory_facets["catalog"]["type"] == "ducklake"
    ), f"DuckLake dataset should have catalog type 'ducklake', got {inventory_facets['catalog']['type']}"

    # Verify memory dataset has different catalog type
    suppliers_facets = suppliers_dataset.get("facets", {})
    assert "catalog" in suppliers_facets, "Suppliers dataset should have catalog facet"
    assert (
        suppliers_facets["catalog"]["type"] == "memory"
    ), f"Memory dataset should have catalog type 'memory', got {suppliers_facets['catalog']['type']}"

    # Verify both have different catalog names
    assert inventory_facets["catalog"]["name"] == "ducklake_db", "DuckLake catalog name should be ducklake_db"
    assert suppliers_facets["catalog"]["name"] == "memory", "Memory catalog name should be memory"


@pytest.mark.integration
def test_ducklake_field_lineage(duckdb_with_ducklake, marquez_client):
    """Test field-level lineage for transformations."""
    conn, ducklake_ns = duckdb_with_ducklake
    namespace = ducklake_ns

    # Create source table
    conn.execute(
        """
        CREATE TABLE ducklake_db.events (
            event_id INTEGER,
            event_type VARCHAR,
            event_timestamp TIMESTAMP,
            user_id INTEGER,
            metadata VARCHAR
        )
    """
    )

    conn.execute(
        """
        INSERT INTO ducklake_db.events VALUES
            (1, 'login', '2024-01-01 10:00:00', 100, 'ip=1.2.3.4'),
            (2, 'purchase', '2024-01-01 10:05:00', 101, 'ip=1.2.3.5')
    """
    )

    # Create derived table with transformed fields
    conn.execute(
        """
        CREATE TABLE ducklake_db.user_summaries AS
        SELECT
            user_id,
            COUNT(*) as event_count,
            MIN(event_timestamp) as first_seen,
            MAX(event_timestamp) as last_seen
        FROM ducklake_db.events
        GROUP BY user_id
    """
    )

    # Get the derived dataset with fields populated
    summary_dataset = marquez_client.wait_for_dataset_with_fields(
        namespace, "ducklake_db.main.user_summaries", timeout_seconds=30
    )
    assert summary_dataset is not None, "Summary dataset should exist"

    # Check schema has transformed fields
    fields = summary_dataset.get("fields", [])
    field_map = {f["name"]: f for f in fields}

    # Check for fields with both snake_case and camelCase variants
    has_user_id = "user_id" in field_map or "userId" in field_map
    has_event_count = "event_count" in field_map or "eventCount" in field_map
    has_first_seen = "first_seen" in field_map or "firstSeen" in field_map
    has_last_seen = "last_seen" in field_map or "lastSeen" in field_map

    assert has_user_id, "Should have user_id field"
    assert has_event_count, "Should have event_count field"
    assert has_first_seen, "Should have first_seen field"
    assert has_last_seen, "Should have last_seen field"

    # Check field types
    count_field_name = "event_count" if "event_count" in field_map else "eventCount"
    count_field = field_map[count_field_name]
    # Event count should be a numeric type (BIGINT from COUNT)
    count_type = count_field.get("type", "").lower()
    assert (
        "bigint" in count_type or "int" in count_type or "long" in count_type
    ), f"event_count should be numeric, got {count_type}"


@pytest.mark.integration
def test_ducklake_datasource_facet_content(duckdb_with_ducklake, marquez_client):
    """Test that dataSource facet contains correct URI information."""
    conn, ducklake_ns = duckdb_with_ducklake
    namespace = ducklake_ns

    # Create a table
    conn.execute(
        """
        CREATE TABLE ducklake_db.test_table (
            id INTEGER,
            data VARCHAR
        )
    """
    )

    # Get the dataset with dataSource facet
    dataset = marquez_client.wait_for_dataset_with_facets(
        namespace, "ducklake_db.main.test_table", ["dataSource"], timeout_seconds=30
    )
    assert dataset is not None, "Dataset should exist"

    # Check dataSource facet
    facets = dataset.get("facets", {})
    assert "dataSource" in facets, "Should have dataSource facet"

    data_source = facets["dataSource"]
    assert "uri" in data_source, "dataSource should have URI"
    assert "name" in data_source, "dataSource should have name"

    # URI and name should both reference the ducklake location
    uri = data_source["uri"]
    name = data_source["name"]

    # Should contain file:// reference to the ducklake data directory
    assert "file://" in uri, f"URI should have file:// scheme, got {uri}"
    # The path should reference the data path directory
    assert "ducklake_data" in uri or ducklake_ns in uri, f"URI should reference the data path, got {uri}"

    # Name should match URI (both are the formatted path)
    assert name == uri, f"Name should match URI, got name={name}, uri={uri}"


@pytest.mark.integration
def test_ducklake_catalog_facet_content(duckdb_with_ducklake, marquez_client):
    """Test that catalog facet contains correct metadata for DuckLake."""
    conn, ducklake_ns = duckdb_with_ducklake
    namespace = ducklake_ns

    # Create a table
    conn.execute("CREATE TABLE ducklake_db.catalog_test (id INT, value VARCHAR)")

    # Get the dataset with catalog facet
    dataset = marquez_client.wait_for_dataset_with_facets(
        namespace, "ducklake_db.main.catalog_test", ["catalog"], timeout_seconds=30
    )
    assert dataset is not None, "Dataset should exist"

    # Check catalog facet
    facets = dataset.get("facets", {})
    assert "catalog" in facets, "Should have catalog facet"

    catalog = facets["catalog"]

    # Verify catalog properties
    assert "name" in catalog, "Catalog should have name"
    assert "type" in catalog, "Catalog should have type"
    assert "framework" in catalog, "Catalog should have framework"

    assert catalog["name"] == "ducklake_db", f"Catalog name should be 'ducklake_db', got {catalog['name']}"
    assert catalog["type"] == "ducklake", f"Catalog type should be 'ducklake', got {catalog['type']}"
    assert catalog["framework"] == "duckdb", f"Framework should be 'duckdb', got {catalog['framework']}"

    # Check optional fields
    if "source" in catalog and catalog["source"]:
        assert catalog["source"] == "duckdb", f"Source should be 'duckdb', got {catalog['source']}"

    # metadata_uri should be set
    if "metadataUri" in catalog or "metadata_uri" in catalog:
        metadata_uri = catalog.get("metadataUri") or catalog.get("metadata_uri")
        assert metadata_uri, "metadata_uri should not be empty"
        # Should reference the ducklake location
        assert (
            "file://" in metadata_uri or "ducklake" in metadata_uri.lower()
        ), f"metadata_uri should reference ducklake, got {metadata_uri}"
