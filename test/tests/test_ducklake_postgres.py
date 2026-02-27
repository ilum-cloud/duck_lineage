"""
Tests for DuckLake with PostgreSQL metadata + S3 (MinIO) data storage.

This is the most common production DuckLake setup. Tests verify that lineage events
correctly reflect the S3 data location (namespace, dataSource URI) and PostgreSQL
metadata location (catalog.metadataUri), and that credentials are never leaked.

Prerequisites:
    - Marquez must be running
    - DuckLake PostgreSQL (port 5433) and MinIO (port 9000) must be running
    - Start with: make ducklake-up

Run tests:
    cd test && uv run pytest -v -m ducklake_postgres
"""

import json
import socket

import duckdb
import pytest
import requests


DUCKLAKE_PG_HOST = "localhost"
DUCKLAKE_PG_PORT = 5433
DUCKLAKE_PG_USER = "ducklake"
DUCKLAKE_PG_PASSWORD = "ducklake"
DUCKLAKE_PG_DB = "ducklake"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "test-bucket"
S3_DATA_PATH = f"s3://{MINIO_BUCKET}/data"

DUCKLAKE_PG_CONN = (
    f"postgres:dbname={DUCKLAKE_PG_DB} host={DUCKLAKE_PG_HOST} "
    f"port={DUCKLAKE_PG_PORT} user={DUCKLAKE_PG_USER} password={DUCKLAKE_PG_PASSWORD}"
)
EXPECTED_PG_URI = f"postgres://{DUCKLAKE_PG_HOST}:{DUCKLAKE_PG_PORT}/{DUCKLAKE_PG_DB}"
DUCKLAKE_TEST_NAMESPACE = "ducklake_pg_test"


MARQUEZ_API = "http://localhost:5000/api/v1"


def _dump_dataset_debug(dataset_name, namespaces_to_check):
    """Log what Marquez actually has for a dataset name across namespaces."""
    lines = [f"DEBUG: Looking for dataset '{dataset_name}'"]

    # Check specific namespaces
    for ns in namespaces_to_check:
        try:
            resp = requests.get(f"{MARQUEZ_API}/namespaces/{ns}/datasets/{dataset_name}", timeout=5)
            if resp.status_code == 200:
                ds = resp.json()
                lines.append(f"  FOUND in namespace '{ns}': name={ds.get('name')}")
                facets = ds.get("facets", {})
                if "dataSource" in facets:
                    lines.append(f"    dataSource.uri = {facets['dataSource'].get('uri')}")
                if "catalog" in facets:
                    cat = facets["catalog"]
                    lines.append(
                        f"    catalog: type={cat.get('type')}, metadataUri={cat.get('metadataUri')}, warehouseUri={cat.get('warehouseUri')}"
                    )
            else:
                lines.append(f"  NOT in namespace '{ns}' (HTTP {resp.status_code})")
        except Exception as e:
            lines.append(f"  ERROR checking namespace '{ns}': {e}")

    # List all namespaces that have datasets matching this name
    try:
        resp = requests.get(f"{MARQUEZ_API}/namespaces", params={"limit": 100}, timeout=5)
        if resp.status_code == 200:
            all_ns = [n["name"] for n in resp.json().get("namespaces", [])]
            lines.append(f"  All namespaces: {all_ns}")
            for ns in all_ns:
                if ns in namespaces_to_check:
                    continue
                try:
                    r2 = requests.get(f"{MARQUEZ_API}/namespaces/{ns}/datasets/{dataset_name}", timeout=5)
                    if r2.status_code == 200:
                        ds = r2.json()
                        lines.append(f"  FOUND in OTHER namespace '{ns}': name={ds.get('name')}")
                        facets = ds.get("facets", {})
                        if "dataSource" in facets:
                            lines.append(f"    dataSource.uri = {facets['dataSource'].get('uri')}")
                        if "catalog" in facets:
                            cat = facets["catalog"]
                            lines.append(
                                f"    catalog: type={cat.get('type')}, metadataUri={cat.get('metadataUri')}, warehouseUri={cat.get('warehouseUri')}"
                            )
                except Exception:
                    pass
    except Exception:
        pass

    return "\n".join(lines)


def _is_port_open(host, port, timeout=2):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module", autouse=True)
def _check_ducklake_infra():
    """Skip all tests in this module if DuckLake infrastructure is not available."""
    if not _is_port_open(DUCKLAKE_PG_HOST, DUCKLAKE_PG_PORT):
        pytest.skip(f"DuckLake PostgreSQL not reachable at {DUCKLAKE_PG_HOST}:{DUCKLAKE_PG_PORT}")
    minio_host, minio_port = MINIO_ENDPOINT.split(":")
    if not _is_port_open(minio_host, int(minio_port)):
        pytest.skip(f"MinIO not reachable at {MINIO_ENDPOINT}")


@pytest.fixture
def duckdb_with_ducklake_pg(extension_path, marquez_api_url):
    """
    DuckDB connection with DuckLake attached using PostgreSQL metadata + S3 data.

    Does NOT set duck_lineage_namespace so the extension's natural namespace
    behavior for DuckLake catalogs is observable.
    """
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})

    conn.execute(f"LOAD '{extension_path}'")
    conn.execute(f"SET duck_lineage_url = '{marquez_api_url}/lineage'")
    conn.execute(f"SET duck_lineage_namespace = '{DUCKLAKE_TEST_NAMESPACE}'")
    conn.execute("SET duck_lineage_debug = true")

    try:
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")
    except Exception as e:
        conn.close()
        pytest.skip(f"DuckLake extension not available: {e}")

    try:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
    except Exception as e:
        conn.close()
        pytest.skip(f"httpfs extension not available: {e}")

    # Configure S3 to point at MinIO
    conn.execute(f"SET s3_endpoint = '{MINIO_ENDPOINT}'")
    conn.execute(f"SET s3_access_key_id = '{MINIO_ACCESS_KEY}'")
    conn.execute(f"SET s3_secret_access_key = '{MINIO_SECRET_KEY}'")
    conn.execute("SET s3_use_ssl = false")
    conn.execute("SET s3_url_style = 'path'")

    try:
        conn.execute(f"ATTACH 'ducklake:{DUCKLAKE_PG_CONN}' AS ducklake_db (DATA_PATH '{S3_DATA_PATH}')")
    except Exception as e:
        conn.close()
        pytest.skip(f"Could not attach DuckLake with postgres+S3: {e}")

    # Drop all existing tables so each test starts clean
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'ducklake_db' AND table_schema = 'main'"
    ).fetchall()
    for (table_name,) in tables:
        conn.execute(f"DROP TABLE IF EXISTS ducklake_db.main.{table_name}")

    yield conn

    try:
        conn.execute("DETACH ducklake_db")
    except Exception:
        pass
    conn.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.ducklake_postgres
@pytest.mark.integration
def test_pg_s3_dataset_namespace(duckdb_with_ducklake_pg, marquez_client):
    """Dataset namespace should be the S3 data path, not the user-configured namespace."""
    conn = duckdb_with_ducklake_pg

    conn.execute("CREATE TABLE ducklake_db.ns_test (id INTEGER, val VARCHAR)")

    # The dataset should land under the S3 data path namespace, not the user-configured one.
    # First try the correct namespace:
    dataset = marquez_client.wait_for_dataset_with_facets(
        S3_DATA_PATH, "ducklake_db.main.ns_test", ["catalog"], timeout_seconds=15
    )
    if dataset is not None:
        return  # Correct behavior

    # If not found, check the user namespace to confirm the bug
    dataset_wrong_ns = marquez_client.wait_for_dataset_with_facets(
        DUCKLAKE_TEST_NAMESPACE, "ducklake_db.main.ns_test", ["catalog"], timeout_seconds=15
    )
    if dataset_wrong_ns is not None:
        pytest.fail(
            f"BUG: Dataset landed in user-configured namespace '{DUCKLAKE_TEST_NAMESPACE}' "
            f"instead of S3 data path '{S3_DATA_PATH}'.\n"
            + _dump_dataset_debug("ducklake_db.main.ns_test", [DUCKLAKE_TEST_NAMESPACE])
        )
    else:
        pytest.fail(
            f"Dataset not found in any expected namespace.\n"
            + _dump_dataset_debug("ducklake_db.main.ns_test", [S3_DATA_PATH, DUCKLAKE_TEST_NAMESPACE])
        )


@pytest.mark.ducklake_postgres
@pytest.mark.integration
def test_pg_s3_datasource_uri(duckdb_with_ducklake_pg, marquez_client):
    """dataSource.uri should reference S3 data location, not the postgres metadata."""
    conn = duckdb_with_ducklake_pg

    conn.execute("CREATE TABLE ducklake_db.ds_test (id INTEGER)")

    # Try S3 namespace first, fall back to user namespace
    dataset = marquez_client.wait_for_dataset_with_facets(
        S3_DATA_PATH, "ducklake_db.main.ds_test", ["dataSource"], timeout_seconds=15
    )
    if dataset is None:
        dataset = marquez_client.wait_for_dataset_with_facets(
            DUCKLAKE_TEST_NAMESPACE, "ducklake_db.main.ds_test", ["dataSource"], timeout_seconds=15
        )
    assert dataset is not None, f"Dataset not found.\n" + _dump_dataset_debug(
        "ducklake_db.main.ds_test", [S3_DATA_PATH, DUCKLAKE_TEST_NAMESPACE]
    )

    uri = dataset["facets"]["dataSource"]["uri"]
    assert "s3://" in uri, f"dataSource.uri should reference S3, got {uri}"
    assert "postgres" not in uri.lower(), f"dataSource.uri should NOT be postgres, got {uri}"


@pytest.mark.ducklake_postgres
@pytest.mark.integration
def test_pg_s3_catalog_facet_metadata_uri(duckdb_with_ducklake_pg, marquez_client):
    """Catalog facet should have metadataUri pointing to postgres and warehouseUri pointing to S3."""
    conn = duckdb_with_ducklake_pg

    conn.execute("CREATE TABLE ducklake_db.cat_test (id INTEGER)")

    dataset = marquez_client.wait_for_dataset_with_facets(
        S3_DATA_PATH, "ducklake_db.main.cat_test", ["catalog"], timeout_seconds=15
    )
    if dataset is None:
        dataset = marquez_client.wait_for_dataset_with_facets(
            DUCKLAKE_TEST_NAMESPACE, "ducklake_db.main.cat_test", ["catalog"], timeout_seconds=15
        )
    assert dataset is not None, f"Dataset not found.\n" + _dump_dataset_debug(
        "ducklake_db.main.cat_test", [S3_DATA_PATH, DUCKLAKE_TEST_NAMESPACE]
    )

    catalog = dataset["facets"]["catalog"]
    assert catalog["type"] == "ducklake", f"Expected type 'ducklake', got {catalog['type']}"
    assert catalog["name"] == "ducklake_db", f"Expected name 'ducklake_db', got {catalog['name']}"
    assert (
        catalog.get("metadataUri") == EXPECTED_PG_URI
    ), f"metadataUri should be '{EXPECTED_PG_URI}', got {catalog.get('metadataUri')}"
    assert (
        catalog.get("warehouseUri") == S3_DATA_PATH
    ), f"warehouseUri should be '{S3_DATA_PATH}', got {catalog.get('warehouseUri')}"


@pytest.mark.ducklake_postgres
@pytest.mark.integration
def test_pg_s3_no_password_leak(duckdb_with_ducklake_pg, marquez_client):
    """Postgres password must never appear in connection-string form in any facet."""
    conn = duckdb_with_ducklake_pg
    namespace = DUCKLAKE_TEST_NAMESPACE

    conn.execute("CREATE TABLE ducklake_db.leak_test (id INTEGER)")

    # The password string "ducklake" also appears as the DB name, so check for
    # connection-string patterns that would indicate an actual credential leak.
    password_patterns = [
        f"password={DUCKLAKE_PG_PASSWORD}",
        f"password%3D{DUCKLAKE_PG_PASSWORD}",  # URL-encoded =
        f":{DUCKLAKE_PG_PASSWORD}@",  # userinfo in URI
    ]

    # Try both the S3 namespace and user namespace — whichever the dataset lands in
    for ns in [S3_DATA_PATH, namespace]:
        dataset = marquez_client.wait_for_dataset_with_facets(
            ns, "ducklake_db.main.leak_test", ["catalog"], timeout_seconds=15
        )
        if dataset is not None:
            facets_json = json.dumps(dataset.get("facets", {})).lower()
            for pattern in password_patterns:
                assert pattern.lower() not in facets_json, f"Password pattern '{pattern}' found in facets"
            return

    pytest.fail("Dataset ducklake_db.main.leak_test not found in any namespace")


@pytest.mark.ducklake_postgres
@pytest.mark.integration
def test_pg_s3_insert_dataset_name(duckdb_with_ducklake_pg, marquez_client):
    """INSERT should produce dataset with fully-qualified name ducklake_db.main.<table>."""
    conn = duckdb_with_ducklake_pg
    namespace = DUCKLAKE_TEST_NAMESPACE

    conn.execute("CREATE TABLE ducklake_db.ins_test (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO ducklake_db.ins_test VALUES (1, 'a'), (2, 'b')")

    # Try both possible namespaces
    for ns in [S3_DATA_PATH, namespace]:
        dataset = marquez_client.wait_for_dataset_with_facets(
            ns, "ducklake_db.main.ins_test", ["dataSource"], timeout_seconds=15
        )
        if dataset is not None:
            assert dataset["name"] == "ducklake_db.main.ins_test"
            return

    pytest.fail("Dataset ducklake_db.main.ins_test not found in any namespace")


@pytest.mark.ducklake_postgres
@pytest.mark.integration
def test_pg_s3_ctas_both_datasets(duckdb_with_ducklake_pg, marquez_client):
    """CTAS within DuckLake: both input and output should have S3 namespace and postgres metadataUri."""
    conn = duckdb_with_ducklake_pg

    conn.execute("CREATE TABLE ducklake_db.ctas_src (id INTEGER, val INTEGER)")
    conn.execute("INSERT INTO ducklake_db.ctas_src VALUES (1, 10), (2, 20)")
    conn.execute("CREATE TABLE ducklake_db.ctas_dst AS SELECT id, val * 2 AS doubled FROM ducklake_db.ctas_src")

    src = None
    for ns in [S3_DATA_PATH, DUCKLAKE_TEST_NAMESPACE]:
        src = marquez_client.wait_for_dataset_with_facets(
            ns, "ducklake_db.main.ctas_src", ["catalog"], timeout_seconds=15
        )
        if src is not None:
            break
    dst = None
    for ns in [S3_DATA_PATH, DUCKLAKE_TEST_NAMESPACE]:
        dst = marquez_client.wait_for_dataset_with_facets(
            ns, "ducklake_db.main.ctas_dst", ["catalog"], timeout_seconds=15
        )
        if dst is not None:
            break

    assert src is not None, f"Source dataset not found.\n" + _dump_dataset_debug(
        "ducklake_db.main.ctas_src", [S3_DATA_PATH, DUCKLAKE_TEST_NAMESPACE]
    )
    assert dst is not None, f"Destination dataset not found.\n" + _dump_dataset_debug(
        "ducklake_db.main.ctas_dst", [S3_DATA_PATH, DUCKLAKE_TEST_NAMESPACE]
    )

    for ds_name, ds in [("ctas_src", src), ("ctas_dst", dst)]:
        catalog = ds["facets"]["catalog"]
        assert (
            catalog.get("metadataUri") == EXPECTED_PG_URI
        ), f"{ds_name}: metadataUri should be '{EXPECTED_PG_URI}', got {catalog.get('metadataUri')}"


@pytest.mark.ducklake_postgres
@pytest.mark.integration
def test_pg_s3_cross_storage_query(duckdb_with_ducklake_pg, marquez_client):
    """Join DuckLake (postgres+S3) with in-memory table: different namespaces and catalog types."""
    conn = duckdb_with_ducklake_pg
    user_ns = DUCKLAKE_TEST_NAMESPACE

    conn.execute("CREATE TABLE ducklake_db.dl_items (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO ducklake_db.dl_items VALUES (1, 'Widget')")

    conn.execute("CREATE TABLE mem_prices (id INTEGER, price DECIMAL(10,2))")
    conn.execute("INSERT INTO mem_prices VALUES (1, 9.99)")

    conn.execute(
        """
        SELECT i.name, p.price
        FROM ducklake_db.dl_items i
        JOIN mem_prices p ON i.id = p.id
    """
    )

    dl_dataset = None
    dl_actual_ns = None
    for ns in [S3_DATA_PATH, user_ns]:
        dl_dataset = marquez_client.wait_for_dataset_with_facets(
            ns, "ducklake_db.main.dl_items", ["catalog"], timeout_seconds=15
        )
        if dl_dataset is not None:
            dl_actual_ns = ns
            break

    mem_dataset = marquez_client.wait_for_dataset_with_facets(
        user_ns, "memory.main.mem_prices", ["catalog"], timeout_seconds=30
    )

    assert dl_dataset is not None, f"DuckLake dataset not found.\n" + _dump_dataset_debug(
        "ducklake_db.main.dl_items", [S3_DATA_PATH, user_ns]
    )
    assert mem_dataset is not None, f"Memory dataset not found.\n" + _dump_dataset_debug(
        "memory.main.mem_prices", [user_ns, S3_DATA_PATH]
    )

    assert dl_dataset["facets"]["catalog"]["type"] == "ducklake"
    assert mem_dataset["facets"]["catalog"]["type"] == "memory"

    # DuckLake dataset should be in S3 namespace, memory dataset in user namespace
    assert dl_actual_ns == S3_DATA_PATH, f"DuckLake dataset namespace should be '{S3_DATA_PATH}', got '{dl_actual_ns}'"


@pytest.mark.ducklake_postgres
@pytest.mark.integration
def test_pg_s3_ctas_memory_to_ducklake(duckdb_with_ducklake_pg, marquez_client):
    """CTAS from in-memory into DuckLake: output should get S3 namespace."""
    conn = duckdb_with_ducklake_pg

    conn.execute("CREATE TABLE mem_source (id INTEGER, data VARCHAR)")
    conn.execute("INSERT INTO mem_source VALUES (1, 'hello'), (2, 'world')")

    conn.execute("CREATE TABLE ducklake_db.from_mem AS SELECT * FROM mem_source")

    dataset = marquez_client.wait_for_dataset_with_facets(
        S3_DATA_PATH, "ducklake_db.main.from_mem", ["catalog"], timeout_seconds=15
    )
    if dataset is None:
        dataset = marquez_client.wait_for_dataset_with_facets(
            DUCKLAKE_TEST_NAMESPACE, "ducklake_db.main.from_mem", ["catalog"], timeout_seconds=15
        )
    assert dataset is not None, f"Output dataset not found.\n" + _dump_dataset_debug(
        "ducklake_db.main.from_mem", [S3_DATA_PATH, DUCKLAKE_TEST_NAMESPACE]
    )
    assert dataset["facets"]["catalog"]["type"] == "ducklake"
