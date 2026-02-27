"""
Tests for complex query patterns: CTEs, JOINs, UNIONs, subqueries, complex types.

These verify multi-input lineage extraction and schema handling for non-trivial
query patterns that the optimizer's plan visitor traverses.
"""

import pytest

from event_helpers import (
    run_and_wait,
    get_outputs,
    get_inputs,
    get_facets,
    assert_valid_event,
    assert_valid_output,
    assert_valid_input,
    assert_output_has_schema,
)


def _find_event_with_io(events, input_substr, output_substr):
    for e in events:
        input_names = [i.get("name", "").lower() for i in get_inputs(e)]
        output_names = [o.get("name", "").lower() for o in get_outputs(e)]
        has_input = any(input_substr in n for n in input_names)
        has_output = any(output_substr in n for n in output_names)
        if has_input and has_output:
            return e
    return None


def _io_summary(events):
    return [
        {"inputs": [i.get("name") for i in get_inputs(e)], "outputs": [o.get("name") for o in get_outputs(e)]}
        for e in events
    ]


def _assert_event_io_deep(event, namespace, input_substrs, output_substr):
    """Validate event structure and all its inputs/outputs deeply."""
    assert_valid_event(event, namespace)
    for inp in get_inputs(event):
        for substr in input_substrs:
            if substr in (inp.get("name") or "").lower():
                assert_valid_input(inp, substr)
    for out in get_outputs(event):
        if output_substr in (out.get("name") or "").lower():
            assert_valid_output(out, namespace, output_substr)


# ── CTEs ───────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_cte_tracks_source_tables(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE cte_src (id INT, val VARCHAR)",
            "INSERT INTO cte_src VALUES (1, 'x'), (2, 'y')",
            "CREATE TABLE cte_dst AS WITH w AS (SELECT * FROM cte_src) SELECT * FROM w",
        ],
        min_events=6,
    )

    e = _find_event_with_io(events, "cte_src", "cte_dst")
    assert e is not None, f"No CTAS event with cte_src→cte_dst. Events: {_io_summary(events)}"
    _assert_event_io_deep(e, clean_marquez_namespace, ["cte_src"], "cte_dst")


# ── JOINs ──────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_join_tracks_both_inputs(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE join_a (id INT, name VARCHAR)",
            "CREATE TABLE join_b (id INT, score INT)",
            "INSERT INTO join_a VALUES (1, 'Alice')",
            "INSERT INTO join_b VALUES (1, 95)",
            "CREATE TABLE join_out AS SELECT a.name, b.score FROM join_a a JOIN join_b b ON a.id = b.id",
        ],
        min_events=10,
    )

    for e in events:
        input_names = [i.get("name", "").lower() for i in get_inputs(e)]
        output_names = [o.get("name", "").lower() for o in get_outputs(e)]
        has_a = any("join_a" in n for n in input_names)
        has_b = any("join_b" in n for n in input_names)
        has_out = any("join_out" in n for n in output_names)
        if has_a and has_b and has_out:
            _assert_event_io_deep(e, clean_marquez_namespace, ["join_a", "join_b"], "join_out")
            return

    pytest.fail(f"No event with join_a+join_b as inputs → join_out. Events: {_io_summary(events)}")


@pytest.mark.integration
def test_insert_select_from_join(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE isj_a (id INT, name VARCHAR)",
            "CREATE TABLE isj_b (id INT, dept VARCHAR)",
            "CREATE TABLE isj_out (name VARCHAR, dept VARCHAR)",
            "INSERT INTO isj_a VALUES (1, 'Alice')",
            "INSERT INTO isj_b VALUES (1, 'Eng')",
            "INSERT INTO isj_out SELECT a.name, b.dept FROM isj_a a JOIN isj_b b ON a.id = b.id",
        ],
        min_events=12,
    )

    for e in events:
        input_names = [i.get("name", "").lower() for i in get_inputs(e)]
        output_names = [o.get("name", "").lower() for o in get_outputs(e)]
        has_a = any("isj_a" in n for n in input_names)
        has_b = any("isj_b" in n for n in input_names)
        has_out = any("isj_out" in n for n in output_names)
        if has_a and has_b and has_out:
            _assert_event_io_deep(e, clean_marquez_namespace, ["isj_a", "isj_b"], "isj_out")
            return

    pytest.fail(f"No INSERT event with isj_a+isj_b→isj_out. Events: {_io_summary(events)}")


# ── Subqueries ─────────────────────────────────────────────────────────


@pytest.mark.integration
def test_subquery_in_where(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE sq_main (id INT, name VARCHAR)",
            "CREATE TABLE sq_filter (id INT)",
            "INSERT INTO sq_main VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "INSERT INTO sq_filter VALUES (1), (3)",
            "CREATE TABLE sq_out AS SELECT * FROM sq_main WHERE id IN (SELECT id FROM sq_filter)",
        ],
        min_events=10,
    )

    for e in events:
        input_names = [i.get("name", "").lower() for i in get_inputs(e)]
        output_names = [o.get("name", "").lower() for o in get_outputs(e)]
        has_main = any("sq_main" in n for n in input_names)
        has_filter = any("sq_filter" in n for n in input_names)
        has_out = any("sq_out" in n for n in output_names)
        if has_main and has_filter and has_out:
            _assert_event_io_deep(e, clean_marquez_namespace, ["sq_main", "sq_filter"], "sq_out")
            return

    pytest.fail(f"No event with sq_main+sq_filter as inputs. Events: {_io_summary(events)}")


# ── UNION ──────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_union_tracks_all_sources(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE union_a (id INT, name VARCHAR)",
            "CREATE TABLE union_b (id INT, name VARCHAR)",
            "INSERT INTO union_a VALUES (1, 'a')",
            "INSERT INTO union_b VALUES (2, 'b')",
            "CREATE TABLE union_out AS SELECT * FROM union_a UNION ALL SELECT * FROM union_b",
        ],
        min_events=10,
    )

    for e in events:
        input_names = [i.get("name", "").lower() for i in get_inputs(e)]
        output_names = [o.get("name", "").lower() for o in get_outputs(e)]
        has_a = any("union_a" in n for n in input_names)
        has_b = any("union_b" in n for n in input_names)
        has_out = any("union_out" in n for n in output_names)
        if has_a and has_b and has_out:
            _assert_event_io_deep(e, clean_marquez_namespace, ["union_a", "union_b"], "union_out")
            return

    pytest.fail(f"No event with union_a+union_b as inputs. Events: {_io_summary(events)}")


# ── Lineage chain (non-transitive) ────────────────────────────────────


@pytest.mark.integration
def test_nested_ctas_chain(lineage_connection, marquez_client, clean_marquez_namespace):
    """Each CTAS should track its direct source, not transitive ancestors."""
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE chain_t1 (id INT)",
            "INSERT INTO chain_t1 VALUES (1)",
            "CREATE TABLE chain_t2 AS SELECT * FROM chain_t1",
            "CREATE TABLE chain_t3 AS SELECT * FROM chain_t2",
        ],
        min_events=8,
    )

    e = _find_event_with_io(events, "chain_t2", "chain_t3")
    assert e is not None, f"No event with chain_t2→chain_t3. Events: {_io_summary(events)}"
    _assert_event_io_deep(e, clean_marquez_namespace, ["chain_t2"], "chain_t3")


# ── Complex types in schema ────────────────────────────────────────────


@pytest.mark.integration
def test_complex_types_in_schema(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE complex_t (id INT, tags VARCHAR[], metadata STRUCT(key VARCHAR, val INT))"],
        min_events=2,
    )

    for e in events:
        for output in get_outputs(e):
            schema = get_facets(output).get("schema") or {}
            fields = schema.get("fields") or []
            if len(fields) >= 3:
                assert_output_has_schema(output, {"id": "INTEGER", "tags": "VARCHAR", "metadata": "STRUCT"})
                assert_valid_output(output, clean_marquez_namespace, "complex_t")
                return

    fields_found = []
    for e in events:
        for output in get_outputs(e):
            fields_found.extend((get_facets(output).get("schema") or {}).get("fields") or [])
    pytest.fail(f"No schema with 3+ fields for complex_t. Fields found: {fields_found}")


@pytest.mark.integration
def test_aggregate_ctas_schema(lineage_connection, marquez_client, clean_marquez_namespace):
    conn = lineage_connection
    conn.execute("CREATE TABLE agg_src (department VARCHAR, salary DECIMAL(10,2))")
    conn.execute("INSERT INTO agg_src VALUES ('Eng', 100.00), ('Eng', 120.00), ('Sales', 90.00)")

    events = run_and_wait(
        conn,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE agg_out AS SELECT department, COUNT(*) as cnt, AVG(salary) as avg_sal FROM agg_src GROUP BY department",
        ],
        min_events=2,
    )

    for e in events:
        for output in get_outputs(e):
            if "agg_out" not in (output.get("name") or "").lower():
                continue
            schema = get_facets(output).get("schema") or {}
            fields = schema.get("fields") or []
            if len(fields) >= 3:
                assert_output_has_schema(output, {"department": "VARCHAR", "cnt": "BIGINT", "avg_sal": "DOUBLE"})
                assert_valid_output(output, clean_marquez_namespace, "agg_out")
                return

    fields_found = []
    for e in events:
        for output in get_outputs(e):
            if "agg_out" in (output.get("name") or "").lower():
                fields_found.extend((get_facets(output).get("schema") or {}).get("fields") or [])
    pytest.fail(f"No schema with 3+ fields for agg_out. Fields found: {fields_found}")
