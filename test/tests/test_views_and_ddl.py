"""
Tests for views, ALTER, DELETE, and UPDATE lineage.

These test C++ code paths in duck_lineage_optimizer.cpp that have zero coverage:
- LogicalCreateView handler (line 947)
- DROP TABLE/VIEW with VIEW_ENTRY (line 991)
- ALTER handler: RENAME vs other ALTER (line 1033)
- UPDATE handler with OVERWRITE lifecycle (line 1119)
- DELETE handler (line 1150)
- VIEW_ENTRY detection in LogicalGet (line 611)
"""

import pytest

from event_helpers import (
    run_and_wait,
    get_outputs,
    get_inputs,
    get_facets,
    find_events_by_job,
    find_complete_events,
    collect_lifecycle_states,
    collect_dataset_types,
    assert_valid_event,
    assert_valid_output,
    assert_valid_input,
    assert_output_has_lifecycle,
    assert_output_has_dataset_type,
    assert_output_has_statistics,
)


# ── Views ──────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_create_view_lifecycle(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE v_base (id INT, name VARCHAR)",
            "CREATE VIEW v_test AS SELECT * FROM v_base",
        ],
        min_events=4,
    )

    found_view_create = False
    for e in events:
        assert_valid_event(e, clean_marquez_namespace)
        for output in get_outputs(e):
            dt_facet = get_facets(output).get("datasetType") or {}
            if dt_facet.get("datasetType") == "VIEW":
                assert_output_has_dataset_type(output, "VIEW")
                assert_output_has_lifecycle(output, "CREATE")
                assert_valid_output(output, clean_marquez_namespace, "v_test")
                found_view_create = True

    assert found_view_create, (
        f"No output with datasetType=VIEW found. "
        f"Types: {collect_dataset_types(events)}, States: {collect_lifecycle_states(events)}"
    )


@pytest.mark.integration
def test_select_from_view_tracks_view_as_input(lineage_connection, marquez_client, clean_marquez_namespace):
    conn = lineage_connection
    conn.execute("CREATE TABLE v_src (id INT, val VARCHAR)")
    conn.execute("INSERT INTO v_src VALUES (1, 'a'), (2, 'b')")
    conn.execute("CREATE VIEW v_read AS SELECT * FROM v_src")

    events = run_and_wait(
        conn,
        marquez_client,
        clean_marquez_namespace,
        ["CREATE TABLE v_dst AS SELECT * FROM v_read"],
        min_events=2,
    )

    for e in events:
        input_names = [i.get("name", "").lower() for i in get_inputs(e)]
        output_names = [o.get("name", "").lower() for o in get_outputs(e)]
        has_view_or_src = any("v_read" in n or "v_src" in n for n in input_names)
        has_dst = any("v_dst" in n for n in output_names)
        if has_view_or_src and has_dst:
            assert_valid_event(e, clean_marquez_namespace)
            for inp in get_inputs(e):
                if "v_read" in (inp.get("name") or "").lower() or "v_src" in (inp.get("name") or "").lower():
                    assert_valid_input(inp)
            for out in get_outputs(e):
                if "v_dst" in (out.get("name") or "").lower():
                    assert_valid_output(out, clean_marquez_namespace, "v_dst")
            return

    io_summary = [
        {"inputs": [i.get("name") for i in get_inputs(e)], "outputs": [o.get("name") for o in get_outputs(e)]}
        for e in events
    ]
    pytest.fail(f"No event with view/source as input and v_dst as output. Events I/O: {io_summary}")


@pytest.mark.integration
def test_drop_view_lifecycle(lineage_connection, marquez_client, clean_marquez_namespace):
    conn = lineage_connection
    conn.execute("CREATE TABLE dv_base (id INT)")
    conn.execute("CREATE VIEW dv_view AS SELECT * FROM dv_base")

    events = run_and_wait(
        conn,
        marquez_client,
        clean_marquez_namespace,
        ["DROP VIEW dv_view"],
        min_events=2,
    )

    for e in events:
        for output in get_outputs(e):
            lsc = get_facets(output).get("lifecycleStateChange") or {}
            if lsc.get("lifecycleStateChange") == "DROP":
                assert_output_has_lifecycle(output, "DROP")
                assert_output_has_dataset_type(output, "VIEW")
                assert_valid_output(output, clean_marquez_namespace, "dv_view")
                return

    pytest.fail(
        f"No DROP lifecycle for view. States: {collect_lifecycle_states(events)}, Types: {collect_dataset_types(events)}"
    )


# ── ALTER TABLE ────────────────────────────────────────────────────────


@pytest.mark.integration
def test_alter_table_rename(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE rename_old (id INT, val VARCHAR)",
            "ALTER TABLE rename_old RENAME TO rename_new",
        ],
        min_events=4,
    )

    for e in events:
        for output in get_outputs(e):
            lsc = get_facets(output).get("lifecycleStateChange") or {}
            if lsc.get("lifecycleStateChange") == "RENAME":
                assert_output_has_lifecycle(output, "RENAME", previous_name="rename_old")
                assert_valid_output(output, clean_marquez_namespace, "rename_new")
                return

    pytest.fail(f"No output with RENAME lifecycle. States: {collect_lifecycle_states(events)}")


@pytest.mark.integration
def test_alter_table_add_column(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE alter_col (id INT)",
            "ALTER TABLE alter_col ADD COLUMN name VARCHAR",
        ],
        min_events=4,
    )

    for e in events:
        for output in get_outputs(e):
            lsc = get_facets(output).get("lifecycleStateChange") or {}
            if lsc.get("lifecycleStateChange") == "ALTER":
                assert_output_has_lifecycle(output, "ALTER")
                assert_valid_output(output, clean_marquez_namespace, "alter_col")
                return

    states = collect_lifecycle_states(events)
    pytest.fail(f"Expected ALTER lifecycle state. Found: {states}")


# ── DELETE ─────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_delete_lineage(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE del_t (id INT, name VARCHAR)",
            "INSERT INTO del_t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "DELETE FROM del_t WHERE id = 1",
        ],
        min_events=6,
    )

    delete_events = find_events_by_job(events, "DELETE")
    assert delete_events, f"No DELETE event found. Job names: {[(e.get('job') or {}).get('name') for e in events]}"

    for e in delete_events:
        assert_valid_event(e, clean_marquez_namespace)
        for output in get_outputs(e):
            if "del_t" in (output.get("name") or "").lower():
                assert_valid_output(output, clean_marquez_namespace, "del_t")
                return

    pytest.fail(f"DELETE event should have del_t as output. Outputs: {[get_outputs(e) for e in delete_events]}")


# ── UPDATE ─────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_update_lineage(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE upd_t (id INT, name VARCHAR)",
            "INSERT INTO upd_t VALUES (1, 'old'), (2, 'old')",
            "UPDATE upd_t SET name = 'new' WHERE id = 1",
        ],
        min_events=6,
    )

    update_events = find_events_by_job(events, "UPDATE")
    assert update_events, f"No UPDATE event found. Job names: {[(e.get('job') or {}).get('name') for e in events]}"

    for e in update_events:
        assert_valid_event(e, clean_marquez_namespace)
        for output in get_outputs(e):
            if "upd_t" in (output.get("name") or "").lower():
                assert_output_has_lifecycle(output, "OVERWRITE")
                assert_valid_output(output, clean_marquez_namespace, "upd_t")
                return

    pytest.fail("UPDATE event should have upd_t as output with OVERWRITE lifecycle")


@pytest.mark.integration
def test_update_has_output_statistics(lineage_connection, marquez_client, clean_marquez_namespace):
    events = run_and_wait(
        lineage_connection,
        marquez_client,
        clean_marquez_namespace,
        [
            "CREATE TABLE upd_stats (id INT, val INT)",
            "INSERT INTO upd_stats VALUES (1, 10), (2, 20), (3, 30)",
            "UPDATE upd_stats SET val = 0",
        ],
        min_events=6,
    )

    update_complete = find_complete_events(events, "UPDATE")
    assert update_complete, "No COMPLETE event found for UPDATE"

    for e in update_complete:
        assert_valid_event(e, clean_marquez_namespace)
        for output in get_outputs(e):
            output_facets = output.get("outputFacets") or {}
            if output_facets.get("outputStatistics"):
                assert_output_has_statistics(output)
                return

    pytest.fail("No outputStatistics.rowCount found on UPDATE COMPLETE event")
