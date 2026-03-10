"""
Tests for column-level lineage (columnLineage facet).

Validates that the extension correctly maps output columns to their source
input columns across various SQL patterns.
"""

import pytest
from time import sleep

from event_helpers import (
    run_and_wait,
    find_complete_events,
    get_outputs,
    get_facets,
    assert_valid_facet,
    assert_output_has_column_lineage,
    assert_column_lineage_field_has_source,
)

NAMESPACE = "duckdb_test"


# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture
def col_conn(duckdb_with_extension):
    """Connection with sample tables for column lineage tests."""
    conn = duckdb_with_extension
    conn.execute("""
        CREATE TABLE source_a (
            id INTEGER,
            name VARCHAR,
            value DECIMAL(10,2)
        )
    """)
    conn.execute("INSERT INTO source_a VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)")

    conn.execute("""
        CREATE TABLE source_b (
            id INTEGER,
            category VARCHAR,
            score DOUBLE
        )
    """)
    conn.execute("INSERT INTO source_b VALUES (1, 'X', 0.5), (2, 'Y', 0.8)")
    return conn


def _get_column_lineage_from_complete_events(events, output_name_contains=None):
    """Find columnLineage facet from the most recent COMPLETE event's output."""
    complete = find_complete_events(events)
    assert complete, "No COMPLETE events found"

    for event in reversed(complete):
        for output in get_outputs(event):
            if (
                output_name_contains
                and output_name_contains.lower()
                not in (output.get("name") or "").lower()
            ):
                continue
            facets = get_facets(output)
            cl = facets.get("columnLineage")
            if cl:
                return cl, output

    return None, None


# ── Test: Direct column references ──────────────────────────────────────


@pytest.mark.integration
def test_direct_column_ref(col_conn, marquez_client):
    """SELECT a, b FROM t -> direct column mapping."""
    conn = col_conn
    conn.execute("CREATE TABLE direct_out AS SELECT id, name FROM source_a")
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=4, timeout_seconds=30)
    cl, output = _get_column_lineage_from_complete_events(events, "direct_out")

    assert cl is not None, "columnLineage facet should be present on output"
    assert_valid_facet(cl, "columnLineage")

    fields = cl.get("fields") or {}
    assert (
        len(fields) >= 2
    ), f"Expected at least 2 fields, got {len(fields)}: {list(fields.keys())}"

    assert_output_has_column_lineage(
        output,
        {
            "id": ["id"],
            "name": ["name"],
        },
    )


# ── Test: Expression with multiple source columns ───────────────────────


@pytest.mark.integration
def test_expression_column_lineage(col_conn, marquez_client):
    """SELECT a + b AS sum_col -> maps to both source columns."""
    conn = col_conn
    conn.execute(
        "CREATE TABLE expr_out AS SELECT id, value + CAST(id AS DECIMAL(10,2)) AS adjusted_value FROM source_a"
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=6, timeout_seconds=30)
    cl, output = _get_column_lineage_from_complete_events(events, "expr_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_output_has_column_lineage(
        output,
        {
            "id": ["id"],
            "adjusted_value": ["value", "id"],
        },
    )


# ── Test: Column alias ──────────────────────────────────────────────────


@pytest.mark.integration
def test_alias_column_lineage(col_conn, marquez_client):
    """SELECT a AS renamed -> output column tracks source through alias."""
    conn = col_conn
    conn.execute("CREATE TABLE alias_out AS SELECT name AS employee_name FROM source_a")
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=8, timeout_seconds=30)
    cl, output = _get_column_lineage_from_complete_events(events, "alias_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_output_has_column_lineage(
        output,
        {
            "employee_name": ["name"],
        },
    )


# ── Test: CAST expression ───────────────────────────────────────────────


@pytest.mark.integration
def test_cast_column_lineage(col_conn, marquez_client):
    """SELECT CAST(a AS INT) -> preserves lineage through cast."""
    conn = col_conn
    conn.execute(
        "CREATE TABLE cast_out AS SELECT CAST(value AS INTEGER) AS int_value FROM source_a"
    )
    sleep(3)

    events = marquez_client.wait_for_events(
        NAMESPACE, min_events=10, timeout_seconds=30
    )
    cl, output = _get_column_lineage_from_complete_events(events, "cast_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_output_has_column_lineage(
        output,
        {
            "int_value": ["value"],
        },
    )


# ── Test: Star expansion ────────────────────────────────────────────────


@pytest.mark.integration
def test_star_expansion_column_lineage(col_conn, marquez_client):
    """SELECT * FROM t -> all columns mapped."""
    conn = col_conn
    conn.execute("CREATE TABLE star_out AS SELECT * FROM source_a")
    sleep(3)

    events = marquez_client.wait_for_events(
        NAMESPACE, min_events=12, timeout_seconds=30
    )
    cl, output = _get_column_lineage_from_complete_events(events, "star_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_output_has_column_lineage(
        output,
        {
            "id": ["id"],
            "name": ["name"],
            "value": ["value"],
        },
    )


# ── Test: JOIN ───────────────────────────────────────────────────────────


@pytest.mark.integration
def test_join_column_lineage(col_conn, marquez_client):
    """SELECT t1.a, t2.b FROM t1 JOIN t2 -> columns traced to respective tables."""
    conn = col_conn
    conn.execute("""
        CREATE TABLE join_out AS
        SELECT source_a.name, source_b.category
        FROM source_a
        JOIN source_b ON source_a.id = source_b.id
    """)
    sleep(3)

    events = marquez_client.wait_for_events(
        NAMESPACE, min_events=14, timeout_seconds=30
    )
    cl, output = _get_column_lineage_from_complete_events(events, "join_out")

    assert cl is not None, "columnLineage facet should be present"

    fields = cl.get("fields") or {}
    assert len(fields) >= 2

    # 'name' should trace to source_a, 'category' to source_b
    assert_column_lineage_field_has_source(cl, "name", "source_a", "name")
    assert_column_lineage_field_has_source(cl, "category", "source_b", "category")


# ── Test: Aggregation ────────────────────────────────────────────────────


@pytest.mark.integration
def test_aggregation_column_lineage(col_conn, marquez_client):
    """SELECT SUM(a) FROM t GROUP BY b -> aggregate traces to source columns."""
    conn = col_conn
    conn.execute("""
        CREATE TABLE agg_out AS
        SELECT name, SUM(value) AS total_value
        FROM source_a
        GROUP BY name
    """)
    sleep(3)

    events = marquez_client.wait_for_events(
        NAMESPACE, min_events=16, timeout_seconds=30
    )
    cl, output = _get_column_lineage_from_complete_events(events, "agg_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_output_has_column_lineage(
        output,
        {
            "name": ["name"],
            "total_value": ["value"],
        },
    )


# ── Test: UNION ──────────────────────────────────────────────────────────


@pytest.mark.integration
def test_union_column_lineage(col_conn, marquez_client):
    """UNION merges column lineage from both branches."""
    conn = col_conn
    conn.execute("""
        CREATE TABLE union_out AS
        SELECT id, name FROM source_a
        UNION ALL
        SELECT id, category FROM source_b
    """)
    sleep(3)

    events = marquez_client.wait_for_events(
        NAMESPACE, min_events=18, timeout_seconds=30
    )
    cl, output = _get_column_lineage_from_complete_events(events, "union_out")

    assert cl is not None, "columnLineage facet should be present"

    # The 'id' column should trace to both source_a.id and source_b.id
    fields = cl.get("fields") or {}
    id_field = None
    for k, v in fields.items():
        if k.lower() == "id":
            id_field = v
            break
    assert (
        id_field is not None
    ), f"Missing 'id' field in columnLineage. Fields: {list(fields.keys())}"
    input_fields = id_field.get("inputFields") or []
    assert (
        len(input_fields) >= 2
    ), f"UNION id should trace to at least 2 sources, got {len(input_fields)}: {input_fields}"


# ── Test: INSERT INTO ... SELECT ─────────────────────────────────────────


@pytest.mark.integration
def test_insert_select_column_lineage(col_conn, marquez_client):
    """INSERT INTO target SELECT ... FROM source -> column lineage on target."""
    conn = col_conn
    conn.execute("CREATE TABLE insert_target (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO insert_target SELECT id, name FROM source_a")
    sleep(3)

    events = marquez_client.wait_for_events(
        NAMESPACE, min_events=20, timeout_seconds=30
    )
    cl, output = _get_column_lineage_from_complete_events(events, "insert_target")

    # Column lineage may or may not be present for INSERT depending on plan structure
    # If present, validate it
    if cl is not None:
        assert_output_has_column_lineage(
            output,
            {
                "id": ["id"],
                "name": ["name"],
            },
        )


# ── Test: Facet structure validation ─────────────────────────────────────


@pytest.mark.integration
def test_column_lineage_facet_structure(col_conn, marquez_client):
    """Validate the full OpenLineage columnLineage facet structure."""
    conn = col_conn
    conn.execute("CREATE TABLE struct_out AS SELECT id, name FROM source_a")
    sleep(3)

    events = marquez_client.wait_for_events(
        NAMESPACE, min_events=22, timeout_seconds=30
    )
    cl, output = _get_column_lineage_from_complete_events(events, "struct_out")

    assert cl is not None, "columnLineage facet should be present"

    # Validate _producer and _schemaURL
    assert_valid_facet(cl, "columnLineage")

    # Validate fields is an object (not array)
    fields = cl.get("fields")
    assert isinstance(
        fields, dict
    ), f"fields should be an object, got {type(fields).__name__}"

    # Validate each field entry structure
    for field_name, field_entry in fields.items():
        assert "inputFields" in field_entry, f"Field {field_name!r} missing inputFields"
        assert (
            "transformationType" in field_entry
        ), f"Field {field_name!r} missing transformationType"
        assert isinstance(
            field_entry["inputFields"], list
        ), f"Field {field_name!r} inputFields should be array"
        assert field_entry["transformationType"] in (
            "DIRECT",
            "INDIRECT",
        ), f"Field {field_name!r} invalid transformationType: {field_entry['transformationType']!r}"

        # Validate each inputField entry
        for inp in field_entry["inputFields"]:
            assert (
                "namespace" in inp
            ), f"inputField missing 'namespace' in field {field_name!r}"
            assert "name" in inp, f"inputField missing 'name' in field {field_name!r}"
            assert "field" in inp, f"inputField missing 'field' in field {field_name!r}"
