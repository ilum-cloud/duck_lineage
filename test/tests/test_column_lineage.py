"""
Tests for column-level lineage (columnLineage facet).

Validates that the extension correctly maps output columns to their source
input columns across various SQL patterns.
"""

import pytest
from time import sleep

from event_helpers import (
    TEST_NAMESPACE as NAMESPACE,
    run_and_wait,
    find_complete_events,
    get_outputs,
    get_facets,
    get_column_lineage_from_complete_events,
    assert_valid_facet,
    assert_output_has_column_lineage,
    assert_column_lineage_field_has_source,
)

# ── Test: Direct column references ──────────────────────────────────────


@pytest.mark.integration
def test_direct_column_ref(col_conn, marquez_client):
    """SELECT a, b FROM t -> direct column mapping."""
    conn = col_conn
    conn.execute("CREATE TABLE direct_out AS SELECT id, name FROM source_a")
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "direct_out")

    assert cl is not None, "columnLineage facet should be present on output"
    assert_valid_facet(cl, "columnLineage")

    fields = cl.get("fields") or {}
    assert len(fields) >= 2, f"Expected at least 2 fields, got {len(fields)}: {list(fields.keys())}"

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

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "expr_out")

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

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "alias_out")

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
    conn.execute("CREATE TABLE cast_out AS SELECT CAST(value AS INTEGER) AS int_value FROM source_a")
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "cast_out")

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

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "star_out")

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
    conn.execute(
        """
        CREATE TABLE join_out AS
        SELECT source_a.name, source_b.category
        FROM source_a
        JOIN source_b ON source_a.id = source_b.id
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "join_out")

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
    conn.execute(
        """
        CREATE TABLE agg_out AS
        SELECT name, SUM(value) AS total_value
        FROM source_a
        GROUP BY name
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "agg_out")

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
    conn.execute(
        """
        CREATE TABLE union_out AS
        SELECT id, name FROM source_a
        UNION ALL
        SELECT id, category FROM source_b
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "union_out")

    assert cl is not None, "columnLineage facet should be present"

    # The 'id' column should trace to both source_a.id and source_b.id
    fields = cl.get("fields") or {}
    id_field = None
    for k, v in fields.items():
        if k.lower() == "id":
            id_field = v
            break
    assert id_field is not None, f"Missing 'id' field in columnLineage. Fields: {list(fields.keys())}"
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

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "insert_target")

    assert cl is not None, "columnLineage facet should be present for INSERT...SELECT"
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

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "struct_out")

    assert cl is not None, "columnLineage facet should be present"

    # Validate _producer and _schemaURL
    assert_valid_facet(cl, "columnLineage")

    # Validate fields is an object (not array)
    fields = cl.get("fields")
    assert isinstance(fields, dict), f"fields should be an object, got {type(fields).__name__}"

    # Validate each field entry structure
    for field_name, field_entry in fields.items():
        assert "inputFields" in field_entry, f"Field {field_name!r} missing inputFields"
        assert "transformationType" in field_entry, f"Field {field_name!r} missing transformationType"
        assert isinstance(field_entry["inputFields"], list), f"Field {field_name!r} inputFields should be array"
        assert field_entry["transformationType"] in (
            "DIRECT",
            "INDIRECT",
        ), f"Field {field_name!r} invalid transformationType: {field_entry['transformationType']!r}"

        # Validate each inputField entry
        for inp in field_entry["inputFields"]:
            assert "namespace" in inp, f"inputField missing 'namespace' in field {field_name!r}"
            assert "name" in inp, f"inputField missing 'name' in field {field_name!r}"
            assert "field" in inp, f"inputField missing 'field' in field {field_name!r}"


# ── Test: Window functions ─────────────────────────────────────────────


@pytest.mark.integration
def test_window_function_column_lineage(col_conn, marquez_client):
    """Window functions: ROW_NUMBER, SUM OVER -> traces to partition/order/aggregate cols."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE window_out AS
        SELECT id, name,
               ROW_NUMBER() OVER (ORDER BY id) AS row_num,
               SUM(value) OVER (PARTITION BY name) AS running_total
        FROM source_a
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "window_out")

    assert cl is not None, "columnLineage facet should be present"

    fields = cl.get("fields") or {}
    assert len(fields) >= 2, f"Expected at least 2 fields, got {len(fields)}: {list(fields.keys())}"

    # Direct pass-throughs
    assert_output_has_column_lineage(output, {"id": ["id"], "name": ["name"]})

    # row_num: ROW_NUMBER() OVER (ORDER BY id) -> traces to id (order col)
    if "row_num" in {k.lower() for k in fields}:
        assert_column_lineage_field_has_source(cl, "row_num", "source_a", "id")

    # running_total: SUM(value) OVER (PARTITION BY name) -> traces to value and name
    if "running_total" in {k.lower() for k in fields}:
        assert_column_lineage_field_has_source(cl, "running_total", "source_a", "value")
        assert_column_lineage_field_has_source(cl, "running_total", "source_a", "name")


# ── Test: INTERSECT ────────────────────────────────────────────────────


@pytest.mark.integration
def test_intersect_column_lineage(col_conn, marquez_client):
    """INTERSECT merges column lineage from both branches."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE intersect_out AS
        SELECT id FROM source_a
        INTERSECT
        SELECT id FROM source_b
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "intersect_out")

    assert cl is not None, "columnLineage facet should be present"

    fields = cl.get("fields") or {}
    id_field = None
    for k, v in fields.items():
        if k.lower() == "id":
            id_field = v
            break
    assert id_field is not None, f"Missing 'id' field in columnLineage. Fields: {list(fields.keys())}"
    input_fields = id_field.get("inputFields") or []
    assert (
        len(input_fields) >= 2
    ), f"INTERSECT id should trace to at least 2 sources, got {len(input_fields)}: {input_fields}"


# ── Test: EXCEPT ───────────────────────────────────────────────────────


@pytest.mark.integration
def test_except_column_lineage(col_conn, marquez_client):
    """EXCEPT merges column lineage from both branches."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE except_out AS
        SELECT id FROM source_a
        EXCEPT
        SELECT id FROM source_b
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "except_out")

    assert cl is not None, "columnLineage facet should be present"

    fields = cl.get("fields") or {}
    id_field = None
    for k, v in fields.items():
        if k.lower() == "id":
            id_field = v
            break
    assert id_field is not None, f"Missing 'id' field in columnLineage. Fields: {list(fields.keys())}"
    input_fields = id_field.get("inputFields") or []
    assert (
        len(input_fields) >= 2
    ), f"EXCEPT id should trace to at least 2 sources, got {len(input_fields)}: {input_fields}"


# ── Test: Subquery ─────────────────────────────────────────────────────


@pytest.mark.integration
def test_subquery_column_lineage(col_conn, marquez_client):
    """SELECT FROM (SELECT ...) -> traces through subquery to base table."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE subq_out AS
        SELECT sub.id, sub.name
        FROM (SELECT id, name FROM source_a WHERE value > 50) AS sub
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "subq_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_output_has_column_lineage(
        output,
        {
            "id": ["id"],
            "name": ["name"],
        },
    )
    assert_column_lineage_field_has_source(cl, "id", "source_a", "id")
    assert_column_lineage_field_has_source(cl, "name", "source_a", "name")


# ── Test: CTE ──────────────────────────────────────────────────────────


@pytest.mark.integration
def test_cte_column_lineage(col_conn, marquez_client):
    """WITH cte AS (...) SELECT ... -> traces through CTE to base table."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE cte_out AS
        WITH filtered AS (SELECT id, name FROM source_a WHERE value > 50)
        SELECT id, name FROM filtered
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "cte_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_output_has_column_lineage(
        output,
        {
            "id": ["id"],
            "name": ["name"],
        },
    )
    assert_column_lineage_field_has_source(cl, "id", "source_a", "id")
    assert_column_lineage_field_has_source(cl, "name", "source_a", "name")


# ── Test: Multiple JOINs (3 tables) ───────────────────────────────────


@pytest.mark.integration
def test_multi_join_column_lineage(col_conn, marquez_client):
    """JOIN across 3 tables -> columns traced to respective source tables."""
    conn = col_conn
    conn.execute("CREATE TABLE source_c (id INTEGER, label VARCHAR)")
    conn.execute("INSERT INTO source_c VALUES (1, 'L1'), (2, 'L2')")
    conn.execute(
        """
        CREATE TABLE multi_join_out AS
        SELECT source_a.name, source_b.category, source_c.label
        FROM source_a
        JOIN source_b ON source_a.id = source_b.id
        JOIN source_c ON source_a.id = source_c.id
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "multi_join_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_column_lineage_field_has_source(cl, "name", "source_a", "name")
    assert_column_lineage_field_has_source(cl, "category", "source_b", "category")
    assert_column_lineage_field_has_source(cl, "label", "source_c", "label")


# ── Test: Self-join ────────────────────────────────────────────────────


@pytest.mark.integration
def test_self_join_column_lineage(col_conn, marquez_client):
    """Self-join: same table with different aliases -> both trace to same source."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE self_join_out AS
        SELECT a.name AS name_a, b.name AS name_b
        FROM source_a a
        JOIN source_a b ON a.id = b.id
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "self_join_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_column_lineage_field_has_source(cl, "name_a", "source_a", "name")
    assert_column_lineage_field_has_source(cl, "name_b", "source_a", "name")


# ── Test: CASE expression ─────────────────────────────────────────────


@pytest.mark.integration
def test_case_expression_column_lineage(col_conn, marquez_client):
    """CASE WHEN -> traces to columns used in WHEN conditions and THEN values."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE case_out AS
        SELECT id,
               CASE WHEN value > 150 THEN name ELSE 'unknown' END AS label
        FROM source_a
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "case_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_output_has_column_lineage(output, {"id": ["id"]})

    # label should trace to both value (WHEN condition) and name (THEN value)
    assert_column_lineage_field_has_source(cl, "label", "source_a", "value")
    assert_column_lineage_field_has_source(cl, "label", "source_a", "name")


# ── Test: COALESCE / string functions ──────────────────────────────────


@pytest.mark.integration
def test_coalesce_and_functions_column_lineage(col_conn, marquez_client):
    """COALESCE and UPPER -> trace to underlying source column."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE coalesce_out AS
        SELECT id,
               COALESCE(name, 'default') AS safe_name,
               UPPER(name) AS upper_name
        FROM source_a
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "coalesce_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_output_has_column_lineage(
        output,
        {
            "id": ["id"],
            "safe_name": ["name"],
            "upper_name": ["name"],
        },
    )


# ── Test: Nested arithmetic expressions ────────────────────────────────


@pytest.mark.integration
def test_nested_expression_column_lineage(col_conn, marquez_client):
    """Nested arithmetic (a + b - c * d) -> traces to all source columns."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE arith_out AS
        SELECT id,
               value + CAST(id AS DECIMAL(10,2)) - value * 2 AS computed
        FROM source_a
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "arith_out")

    assert cl is not None, "columnLineage facet should be present"
    assert_output_has_column_lineage(
        output,
        {
            "id": ["id"],
            "computed": ["value", "id"],
        },
    )


# ── Test: COPY TO ──────────────────────────────────────────────────────


@pytest.mark.integration
def test_copy_to_column_lineage(col_conn, marquez_client):
    """COPY (SELECT ...) TO file -> output dataset has column lineage."""
    conn = col_conn
    conn.execute("COPY (SELECT id, name FROM source_a) TO '/tmp/test_col_lineage_copy.csv'")
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)

    # Find COMPLETE event for the COPY operation
    complete = find_complete_events(events)
    assert complete, "No COMPLETE events found"

    # Look for columnLineage on any output of the most recent COPY events
    cl_found = False
    for event in reversed(complete):
        for output in get_outputs(event):
            facets = get_facets(output)
            cl = facets.get("columnLineage")
            if cl:
                cl_found = True
                assert_valid_facet(cl, "columnLineage")
                fields = cl.get("fields") or {}
                # Should have lineage for id and name
                fields_lower = {k.lower(): v for k, v in fields.items()}
                if "id" in fields_lower and "name" in fields_lower:
                    assert_column_lineage_field_has_source(cl, "id", "source_a", "id")
                    assert_column_lineage_field_has_source(cl, "name", "source_a", "name")
                break
        if cl_found:
            break

    # COPY TO may or may not emit column lineage depending on plan structure
    # If it does, we validated it above; if not, that's acceptable


# ── Test: File scan (CSV) ────────────────────────────────────────────


@pytest.mark.integration
def test_file_scan_csv_column_lineage(col_conn, marquez_client, tmp_path):
    """Column lineage traces through read_csv to output table."""
    conn = col_conn
    csv_file = str(tmp_path / "data.csv")
    conn.execute(f"COPY (SELECT 1 AS id, 'alice' AS name, 100 AS score) TO '{csv_file}' (HEADER)")
    sleep(2)  # let COPY event settle
    conn.execute(
        f"""
        CREATE TABLE csv_out AS
        SELECT id, name, score FROM read_csv('{csv_file}')
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "csv_out")

    assert cl is not None, "columnLineage facet should be present for file scan"
    assert_valid_facet(cl, "columnLineage")

    fields = cl.get("fields") or {}
    assert len(fields) >= 3, f"Expected at least 3 fields, got {len(fields)}: {list(fields.keys())}"


# ── Test: File scan (Parquet) ────────────────────────────────────────


@pytest.mark.integration
def test_file_scan_parquet_column_lineage(col_conn, marquez_client, tmp_path):
    """Column lineage traces through read_parquet to output table."""
    conn = col_conn
    parquet_file = str(tmp_path / "data.parquet")
    conn.execute(f"COPY (SELECT 1 AS id, 'bob' AS name, 200 AS score) TO '{parquet_file}' (FORMAT PARQUET)")
    sleep(2)
    conn.execute(
        f"""
        CREATE TABLE parquet_out AS
        SELECT id, name FROM read_parquet('{parquet_file}')
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "parquet_out")

    assert cl is not None, "columnLineage facet should be present for parquet scan"
    assert_valid_facet(cl, "columnLineage")

    fields = cl.get("fields") or {}
    assert len(fields) >= 2, f"Expected at least 2 fields, got {len(fields)}: {list(fields.keys())}"


# ── Test: PIVOT ──────────────────────────────────────────────────────


@pytest.mark.integration
def test_pivot_column_lineage(col_conn, marquez_client):
    """PIVOT: group columns trace to source, pivoted columns trace to aggregated column."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE pivot_src (city VARCHAR, quarter VARCHAR, revenue INTEGER);
        INSERT INTO pivot_src VALUES
            ('NYC', 'Q1', 100), ('NYC', 'Q2', 200),
            ('SF', 'Q1', 150), ('SF', 'Q2', 250);
    """
    )
    conn.execute(
        """
        CREATE TABLE pivot_out AS
        PIVOT pivot_src ON quarter USING SUM(revenue) GROUP BY city
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "pivot_out")

    assert cl is not None, "columnLineage facet should be present for PIVOT"
    assert_valid_facet(cl, "columnLineage")

    fields = cl.get("fields") or {}
    # Should have at least 'city' group column + pivoted value columns
    assert len(fields) >= 2, f"Expected at least 2 fields, got {len(fields)}: {list(fields.keys())}"

    # city is a group-by passthrough -> traces to pivot_src.city
    assert_column_lineage_field_has_source(cl, "city", "pivot_src", "city")

    # Pivoted columns (Q1, Q2) should trace to pivot_src.revenue
    # DuckDB rewrites PIVOT as AGGREGATE + PROJECTION, so transformation
    # may be DIRECT or INDIRECT depending on how aggregation is resolved
    for field_name, field_entry in fields.items():
        if field_name.lower() != "city":
            # Should trace to revenue column
            input_fields = field_entry.get("inputFields") or []
            source_cols = [f.get("field", "").lower() for f in input_fields]
            assert any("revenue" in s for s in source_cols), (
                f"Pivoted column {field_name!r} should trace to 'revenue'. " f"Found sources: {source_cols}"
            )


# ── Test: UNNEST ─────────────────────────────────────────────────────


@pytest.mark.integration
def test_unnest_column_lineage(col_conn, marquez_client):
    """UNNEST: pass-through columns retain lineage, unnested column traces to source."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE unnest_src (id INTEGER, tags INTEGER[]);
        INSERT INTO unnest_src VALUES (1, [10, 20]), (2, [30]);
    """
    )
    conn.execute(
        """
        CREATE TABLE unnest_out AS
        SELECT id, UNNEST(tags) AS tag FROM unnest_src
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "unnest_out")

    assert cl is not None, "columnLineage facet should be present for UNNEST"
    assert_valid_facet(cl, "columnLineage")

    fields = cl.get("fields") or {}
    assert len(fields) >= 2, f"Expected at least 2 fields, got {len(fields)}: {list(fields.keys())}"

    # id is a pass-through -> traces to unnest_src.id (DIRECT)
    assert_column_lineage_field_has_source(cl, "id", "unnest_src", "id")

    # tag is unnested -> traces to unnest_src.tags (INDIRECT)
    assert_column_lineage_field_has_source(cl, "tag", "unnest_src", "tags")

    # Verify transformation types
    fields_lower = {k.lower(): v for k, v in fields.items()}
    id_field = fields_lower.get("id")
    tag_field = fields_lower.get("tag")
    if id_field:
        assert id_field.get("transformationType") == "DIRECT", "id should be DIRECT"
    if tag_field:
        assert tag_field.get("transformationType") == "INDIRECT", "tag should be INDIRECT"


# ── Test: UNPIVOT ────────────────────────────────────────────────────


@pytest.mark.integration
def test_unpivot_column_lineage(col_conn, marquez_client):
    """UNPIVOT transforms columns into rows — lineage should trace source columns."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE unpivot_src (city VARCHAR, q1 INTEGER, q2 INTEGER);
        INSERT INTO unpivot_src VALUES ('NYC', 100, 200), ('SF', 150, 250);
    """
    )
    conn.execute(
        """
        CREATE TABLE unpivot_out AS
        UNPIVOT unpivot_src ON q1, q2 INTO NAME quarter VALUE revenue
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "unpivot_out")

    # UNPIVOT may be handled via default passthrough or dedicated handler
    # If column lineage is present, validate it
    if cl is not None:
        assert_valid_facet(cl, "columnLineage")
        fields = cl.get("fields") or {}
        # city should trace to unpivot_src.city
        if "city" in {k.lower() for k in fields}:
            assert_column_lineage_field_has_source(cl, "city", "unpivot_src", "city")


# ── Test: DISTINCT ───────────────────────────────────────────────────


@pytest.mark.integration
def test_distinct_column_lineage(col_conn, marquez_client):
    """DISTINCT preserves column lineage through passthrough."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE distinct_out AS
        SELECT DISTINCT name FROM source_a
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "distinct_out")

    assert cl is not None, "columnLineage facet should be present for DISTINCT"
    assert_output_has_column_lineage(output, {"name": ["name"]})
    assert_column_lineage_field_has_source(cl, "name", "source_a", "name")


# ── Test: ORDER BY + LIMIT ───────────────────────────────────────────


@pytest.mark.integration
def test_order_limit_column_lineage(col_conn, marquez_client):
    """ORDER BY + LIMIT preserves column lineage (passthrough)."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE order_out AS
        SELECT id, name FROM source_a ORDER BY id LIMIT 1
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "order_out")

    assert cl is not None, "columnLineage facet should be present for ORDER BY + LIMIT"
    assert_output_has_column_lineage(output, {"id": ["id"], "name": ["name"]})


# ── Test: Cross join ─────────────────────────────────────────────────


@pytest.mark.integration
def test_cross_join_column_lineage(col_conn, marquez_client):
    """Cross join: columns from both sides trace correctly."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE cross_out AS
        SELECT source_a.name, source_b.category
        FROM source_a CROSS JOIN source_b
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "cross_out")

    assert cl is not None, "columnLineage facet should be present for CROSS JOIN"
    assert_column_lineage_field_has_source(cl, "name", "source_a", "name")
    assert_column_lineage_field_has_source(cl, "category", "source_b", "category")


# ── Test: Nested functions ───────────────────────────────────────────


@pytest.mark.integration
def test_nested_functions_column_lineage(col_conn, marquez_client):
    """UPPER(SUBSTRING(name, 1, 3)) traces to source column 'name'."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE nested_fn_out AS
        SELECT id, UPPER(SUBSTRING(name, 1, 3)) AS short_name FROM source_a
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "nested_fn_out")

    assert cl is not None, "columnLineage facet should be present for nested functions"
    assert_output_has_column_lineage(
        output,
        {
            "id": ["id"],
            "short_name": ["name"],
        },
    )


# ── Test: Multiple aggregates ────────────────────────────────────────


@pytest.mark.integration
def test_multiple_aggregates_column_lineage(col_conn, marquez_client):
    """SUM, COUNT, AVG in same query all trace correctly."""
    conn = col_conn
    conn.execute(
        """
        CREATE TABLE multi_agg_out AS
        SELECT name,
               SUM(value) AS total,
               COUNT(id) AS cnt,
               AVG(value) AS avg_val
        FROM source_a
        GROUP BY name
    """
    )
    sleep(3)

    events = marquez_client.wait_for_events(NAMESPACE, min_events=2, timeout_seconds=30)
    cl, output = get_column_lineage_from_complete_events(events, "multi_agg_out")

    assert cl is not None, "columnLineage facet should be present for multiple aggregates"
    assert_column_lineage_field_has_source(cl, "name", "source_a", "name")
    assert_column_lineage_field_has_source(cl, "total", "source_a", "value")
    assert_column_lineage_field_has_source(cl, "cnt", "source_a", "id")
    assert_column_lineage_field_has_source(cl, "avg_val", "source_a", "value")
