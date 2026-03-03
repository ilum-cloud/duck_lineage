"""
Tests for OpenLineage event correctness.

These tests verify that the extension emits correct OpenLineage data —
facets, lifecycle states, row counts, SQL text, job I/O, and event sequences.
"""

import pytest

from event_helpers import (
    run_and_wait,
    get_outputs,
    get_inputs,
    get_facets,
    get_run_facets,
    find_complete_events,
    collect_lifecycle_states,
    assert_valid_event,
    assert_valid_facet,
    assert_valid_output,
    assert_valid_input,
    assert_output_has_schema,
    assert_output_has_lifecycle,
    assert_output_has_dataset_type,
    assert_output_has_statistics,
)


@pytest.mark.integration
def test_run_lifecycle_start_then_complete(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE t_lifecycle (id INT)", "INSERT INTO t_lifecycle VALUES (1)"],
        min_events=4,
    )

    # Validate every event has correct top-level structure
    for e in events:
        assert_valid_event(e, clean_marquez_namespace)

    # Group events by runId and verify START+COMPLETE pairing
    runs: dict[str, set[str]] = {}
    for e in events:
        run_id = (e.get("run") or {}).get("runId")
        event_type = e.get("eventType")
        if run_id and event_type:
            runs.setdefault(run_id, set()).add(event_type)

    paired = [rid for rid, types in runs.items() if "START" in types and "COMPLETE" in types]
    assert paired, f"No run has both START and COMPLETE. Runs: {runs}"


@pytest.mark.integration
def test_sql_facet_contains_query_text(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE sql_test (id INT, name VARCHAR)"],
        min_events=2,
    )

    for e in events:
        sql_facet = get_facets(e.get("job") or {}).get("sql")
        if not sql_facet:
            continue
        assert_valid_facet(sql_facet, "sql")
        query = sql_facet.get("query", "")
        if "sql_test" in query.lower():
            return

    sql_texts = []
    for e in events:
        q = (get_facets(e.get("job") or {}).get("sql") or {}).get("query", "")
        if q:
            sql_texts.append(q)
    pytest.fail(f"No SQL facet contains 'sql_test'. Found: {sql_texts}")


@pytest.mark.integration
def test_output_statistics_row_count(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE t_rowcount (id INT)", "INSERT INTO t_rowcount VALUES (1), (2), (3)"],
        min_events=4,
    )

    insert_complete = find_complete_events(events, "INSERT")
    assert insert_complete, "No COMPLETE event found for INSERT"

    for e in insert_complete:
        assert_valid_event(e, clean_marquez_namespace)
        for output in get_outputs(e):
            output_facets = output.get("outputFacets") or {}
            stats = output_facets.get("outputStatistics")
            if stats:
                assert_output_has_statistics(output)
                return

    pytest.fail("No outputStatistics.rowCount found on INSERT COMPLETE event")


@pytest.mark.integration
def test_lifecycle_state_create(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE t_lc_create (id INT)"],
        min_events=2,
    )

    for e in events:
        for output in get_outputs(e):
            lsc = get_facets(output).get("lifecycleStateChange")
            if lsc and lsc.get("lifecycleStateChange") == "CREATE":
                assert_output_has_lifecycle(output, "CREATE")
                assert_valid_output(output, clean_marquez_namespace, "t_lc_create")
                return

    states = collect_lifecycle_states(events)
    pytest.fail(f"Expected CREATE lifecycle state. Found: {states}")


@pytest.mark.integration
def test_lifecycle_state_overwrite_on_insert(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE t_lc_overwrite (id INT)", "INSERT INTO t_lc_overwrite VALUES (1)"],
        min_events=4,
    )

    for e in events:
        for output in get_outputs(e):
            lsc = get_facets(output).get("lifecycleStateChange")
            if lsc and lsc.get("lifecycleStateChange") == "OVERWRITE":
                assert_output_has_lifecycle(output, "OVERWRITE")
                assert_valid_output(output, clean_marquez_namespace, "t_lc_overwrite")
                return

    states = collect_lifecycle_states(events)
    pytest.fail(f"Expected OVERWRITE lifecycle state. Found: {states}")


@pytest.mark.integration
def test_lifecycle_state_drop(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE t_lc_drop (id INT)", "DROP TABLE t_lc_drop"],
        min_events=4,
    )

    for e in events:
        for output in get_outputs(e):
            lsc = get_facets(output).get("lifecycleStateChange")
            if lsc and lsc.get("lifecycleStateChange") == "DROP":
                assert_output_has_lifecycle(output, "DROP")
                return

    states = collect_lifecycle_states(events)
    pytest.fail(f"Expected DROP lifecycle state. Found: {states}")


@pytest.mark.integration
def test_ctas_job_has_input_and_output(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE ctas_src (id INT)",
            "INSERT INTO ctas_src VALUES (1), (2)",
            "CREATE TABLE ctas_dst AS SELECT * FROM ctas_src",
        ],
        min_events=6,
    )

    for e in events:
        input_names = [i.get("name", "").lower() for i in get_inputs(e)]
        output_names = [o.get("name", "").lower() for o in get_outputs(e)]
        has_src = any("ctas_src" in n for n in input_names)
        has_dst = any("ctas_dst" in n for n in output_names)
        if has_src and has_dst:
            assert_valid_event(e, clean_marquez_namespace)
            for inp in get_inputs(e):
                if "ctas_src" in (inp.get("name") or "").lower():
                    assert_valid_input(inp, "ctas_src")
            for out in get_outputs(e):
                if "ctas_dst" in (out.get("name") or "").lower():
                    assert_valid_output(out, clean_marquez_namespace, "ctas_dst")
            return

    io_summary = [
        {"inputs": [i.get("name") for i in get_inputs(e)], "outputs": [o.get("name") for o in get_outputs(e)]}
        for e in events
    ]
    pytest.fail(f"No CTAS event with ctas_src as input and ctas_dst as output. Events I/O: {io_summary}")


@pytest.mark.integration
def test_dataset_type_facet_is_table(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE t_dtype (id INT)", "INSERT INTO t_dtype VALUES (1)"],
        min_events=4,
    )

    for e in events:
        for output in get_outputs(e):
            dt_facet = get_facets(output).get("datasetType")
            if dt_facet and dt_facet.get("datasetType") == "TABLE":
                assert_output_has_dataset_type(output, "TABLE")
                assert_valid_output(output, clean_marquez_namespace, "t_dtype")
                return

    dataset_types = []
    for e in events:
        for output in get_outputs(e):
            dt = (get_facets(output).get("datasetType") or {}).get("datasetType")
            if dt:
                dataset_types.append(dt)
    pytest.fail(f"Expected datasetType=TABLE. Found: {dataset_types}")


@pytest.mark.integration
def test_schema_facet_has_correct_fields(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE t_schema (id INT, name VARCHAR, amount DECIMAL(10,2))"],
        min_events=2,
    )

    for e in events:
        for output in get_outputs(e):
            schema = get_facets(output).get("schema") or {}
            fields = schema.get("fields") or []
            if len(fields) >= 3:
                assert_output_has_schema(output, {"id": "INTEGER", "name": "VARCHAR", "amount": "DECIMAL"})
                assert_valid_output(output, clean_marquez_namespace, "t_schema")
                return

    fields_found = []
    for e in events:
        for output in get_outputs(e):
            fields_found.extend((get_facets(output).get("schema") or {}).get("fields") or [])
    pytest.fail(f"No event with schema fields for t_schema. Fields found: {fields_found}")


@pytest.mark.integration
def test_processing_engine_facet(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE t_engine (id INT)"],
        min_events=2,
    )

    start_events = [e for e in events if e.get("eventType") == "START"]
    for e in start_events:
        engine = get_run_facets(e).get("processing_engine")
        if engine and engine.get("name") == "DuckDB" and engine.get("version"):
            assert_valid_facet(engine, "processing_engine")
            assert_valid_event(e, clean_marquez_namespace)
            return

    engines = [get_run_facets(e).get("processing_engine") for e in start_events]
    pytest.fail(f"No START event with processing_engine name=DuckDB. Found: {engines}")
