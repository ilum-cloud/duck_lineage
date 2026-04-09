"""
Tests for configurable dataset prefix filtering.

Verifies that the duck_lineage_exclude_dataset_prefixes config option correctly
filters datasets from lineage events based on fully qualified name prefixes.
"""

import pytest
from time import sleep

from event_helpers import (
    run_and_wait,
    get_inputs,
    get_outputs,
    assert_valid_event,
    assert_valid_output,
    assert_valid_input,
)


# ── Helpers ────────────────────────────────────────────────────────────


def assert_no_dataset_in_events(events, dataset_name_substring):
    """Assert that no event contains a dataset matching the given substring."""
    for event in events:
        for inp in get_inputs(event):
            name = inp.get("name") or ""
            assert dataset_name_substring not in name, (
                f"Found excluded dataset '{dataset_name_substring}' in inputs: {name}"
            )
        for out in get_outputs(event):
            name = out.get("name") or ""
            assert dataset_name_substring not in name, (
                f"Found excluded dataset '{dataset_name_substring}' in outputs: {name}"
            )


def any_dataset_in_events(events, dataset_name_substring):
    """Check if any event contains a dataset matching the given substring."""
    for event in events:
        for inp in get_inputs(event):
            if dataset_name_substring in (inp.get("name") or ""):
                return True
        for out in get_outputs(event):
            if dataset_name_substring in (out.get("name") or ""):
                return True
    return False


# ── Configuration Tests ────────────────────────────────────────────────


@pytest.mark.integration
def test_exclude_prefixes_config_default(duckdb_with_extension):
    """Default value should be '__ducklake_metadata_'."""
    conn = duckdb_with_extension
    value = conn.execute(
        "SELECT current_setting('duck_lineage_exclude_dataset_prefixes')"
    ).fetchone()[0]
    assert value == "__ducklake_metadata_"


@pytest.mark.integration
def test_exclude_prefixes_config_set_and_read(duckdb_with_extension):
    """Setting and reading back custom and empty prefix values."""
    conn = duckdb_with_extension

    # Set custom value
    conn.execute("SET duck_lineage_exclude_dataset_prefixes = 'custom_prefix_'")
    value = conn.execute(
        "SELECT current_setting('duck_lineage_exclude_dataset_prefixes')"
    ).fetchone()[0]
    assert value == "custom_prefix_"

    # Set to empty string (disables filtering)
    conn.execute("SET duck_lineage_exclude_dataset_prefixes = ''")
    value = conn.execute(
        "SELECT current_setting('duck_lineage_exclude_dataset_prefixes')"
    ).fetchone()[0]
    assert value == ""


@pytest.mark.integration
def test_exclude_prefixes_config_multiple(duckdb_with_extension):
    """Multiple comma-separated prefixes with varied whitespace."""
    conn = duckdb_with_extension
    conn.execute(
        "SET duck_lineage_exclude_dataset_prefixes = 'prefix_a_, prefix_b_,prefix_c_'"
    )
    value = conn.execute(
        "SELECT current_setting('duck_lineage_exclude_dataset_prefixes')"
    ).fetchone()[0]
    assert value == "prefix_a_, prefix_b_,prefix_c_"


# ── Core Filtering Tests ──────────────────────────────────────────────


@pytest.mark.integration
def test_excluded_prefix_table_not_in_inputs(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """Tables matching exclude prefix should not appear as inputs in events."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    conn.execute(
        "SET duck_lineage_exclude_dataset_prefixes = 'memory.main.__excluded_'"
    )

    # Create normal source (generates events) and excluded source (filtered)
    events = run_and_wait(
        conn,
        marquez_client,
        ns,
        [
            "CREATE TABLE __excluded_source (id INT)",
            "INSERT INTO __excluded_source VALUES (1), (2)",
            "CREATE TABLE normal_source (id INT)",
            "INSERT INTO normal_source VALUES (1), (2)",
            "CREATE TABLE result AS SELECT * FROM __excluded_source e JOIN normal_source n ON e.id = n.id",
        ],
        min_events=6,
    )

    # normal_source should appear in events
    assert any_dataset_in_events(events, "normal_source"), (
        "normal_source should appear in events"
    )

    # __excluded_source should NOT appear anywhere
    assert_no_dataset_in_events(events, "__excluded_source")

    # result should appear as output
    assert any_dataset_in_events(events, "result"), "result table should appear in events"


@pytest.mark.integration
def test_non_excluded_table_appears_normally(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """Normal tables (not matching any prefix) should appear in events as usual."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    # Default prefix is __ducklake_metadata_ — normal tables should be unaffected
    events = run_and_wait(
        conn,
        marquez_client,
        ns,
        [
            "CREATE TABLE normal_table (id INT, name VARCHAR)",
            "INSERT INTO normal_table VALUES (1, 'test')",
        ],
        min_events=4,
    )

    assert any_dataset_in_events(events, "normal_table"), (
        "normal_table should appear in events with default filtering"
    )

    # Validate event structure
    for e in events:
        assert_valid_event(e, ns)


@pytest.mark.integration
def test_custom_prefix_filters_matching_tables(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """Custom prefix should filter matching tables and keep non-matching ones."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    conn.execute(
        "SET duck_lineage_exclude_dataset_prefixes = 'memory.main.temp_staging_'"
    )

    events = run_and_wait(
        conn,
        marquez_client,
        ns,
        [
            "CREATE TABLE temp_staging_data (id INT)",
            "INSERT INTO temp_staging_data VALUES (1)",
            "CREATE TABLE production_data (id INT)",
            "INSERT INTO production_data VALUES (1)",
            "CREATE TABLE combined AS SELECT * FROM temp_staging_data UNION ALL SELECT * FROM production_data",
        ],
        min_events=6,
    )

    assert any_dataset_in_events(events, "production_data"), (
        "production_data should appear"
    )
    assert_no_dataset_in_events(events, "temp_staging_data")


@pytest.mark.integration
def test_multiple_prefixes_all_filter(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """Multiple prefixes should each independently filter matching tables."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    conn.execute(
        "SET duck_lineage_exclude_dataset_prefixes = "
        "'memory.main.__internal_,memory.main._temp_,memory.main.staging_'"
    )

    events = run_and_wait(
        conn,
        marquez_client,
        ns,
        [
            "CREATE TABLE __internal_config (id INT)",
            "CREATE TABLE _temp_cache (id INT)",
            "CREATE TABLE staging_raw (id INT)",
            "CREATE TABLE user_data (id INT, name VARCHAR)",
            "INSERT INTO user_data VALUES (1, 'test')",
        ],
        min_events=4,
    )

    assert any_dataset_in_events(events, "user_data"), "user_data should appear"
    assert_no_dataset_in_events(events, "__internal_config")
    assert_no_dataset_in_events(events, "_temp_cache")
    assert_no_dataset_in_events(events, "staging_raw")


# ── Edge Cases ─────────────────────────────────────────────────────────


@pytest.mark.integration
def test_empty_prefix_disables_filtering(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """Empty prefix string should disable all filtering."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    conn.execute("SET duck_lineage_exclude_dataset_prefixes = ''")

    events = run_and_wait(
        conn,
        marquez_client,
        ns,
        [
            "CREATE TABLE __ducklake_metadata_test (id INT)",
            "INSERT INTO __ducklake_metadata_test VALUES (1)",
        ],
        min_events=4,
    )

    # With filtering disabled, even __ducklake_metadata_ prefixed tables should appear
    assert any_dataset_in_events(events, "__ducklake_metadata_test"), (
        "With empty prefix, __ducklake_metadata_test should appear in events"
    )


@pytest.mark.integration
def test_prefix_is_case_sensitive(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """Prefix matching should be case-sensitive."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    # Default prefix is __ducklake_metadata_ (lowercase)
    # Table with uppercase should NOT be filtered
    conn.execute(
        "SET duck_lineage_exclude_dataset_prefixes = 'memory.main.__ducklake_metadata_'"
    )

    events = run_and_wait(
        conn,
        marquez_client,
        ns,
        [
            "CREATE TABLE __DUCKLAKE_METADATA_test (id INT)",
            "INSERT INTO __DUCKLAKE_METADATA_test VALUES (1)",
        ],
        min_events=4,
    )

    assert any_dataset_in_events(events, "__DUCKLAKE_METADATA_test"), (
        "Uppercase table should not be filtered by lowercase prefix"
    )


@pytest.mark.integration
def test_prefix_match_is_starts_with_not_contains(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """Prefix should only match at the start of the FQN, not in the middle."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    # Set prefix to __excluded_ — this won't match memory.main.data__excluded_test
    # because the FQN starts with "memory", not "__excluded_"
    conn.execute("SET duck_lineage_exclude_dataset_prefixes = '__excluded_'")

    events = run_and_wait(
        conn,
        marquez_client,
        ns,
        [
            "CREATE TABLE data__excluded_test (id INT)",
            "INSERT INTO data__excluded_test VALUES (1)",
        ],
        min_events=4,
    )

    assert any_dataset_in_events(events, "data__excluded_test"), (
        "Table with prefix in middle of name should not be filtered"
    )


@pytest.mark.integration
def test_event_skipped_when_all_datasets_filtered(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """Events with no remaining datasets after filtering should not be emitted."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    conn.execute(
        "SET duck_lineage_exclude_dataset_prefixes = 'memory.main.__filtered_'"
    )

    # These operations only touch filtered tables — no events should be emitted
    conn.execute("CREATE TABLE __filtered_only (id INT)")
    conn.execute("INSERT INTO __filtered_only VALUES (1), (2)")

    sleep(3)

    # No jobs should exist in this clean namespace
    jobs = marquez_client.list_jobs(ns)
    filtered_jobs = [
        j
        for j in jobs
        if "__filtered_only" in (j.get("name") or "")
    ]
    assert not filtered_jobs, (
        f"Fully filtered operations should not create jobs, but found: "
        f"{[j.get('name') for j in filtered_jobs]}"
    )


@pytest.mark.integration
def test_mixed_filtered_and_unfiltered_inputs(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """When some inputs are filtered and others aren't, only unfiltered appear."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    conn.execute(
        "SET duck_lineage_exclude_dataset_prefixes = 'memory.main.__hidden_'"
    )

    events = run_and_wait(
        conn,
        marquez_client,
        ns,
        [
            "CREATE TABLE __hidden_src (id INT, val INT)",
            "INSERT INTO __hidden_src VALUES (1, 100), (2, 200)",
            "CREATE TABLE visible_src (id INT, label VARCHAR)",
            "INSERT INTO visible_src VALUES (1, 'a'), (2, 'b')",
            "CREATE TABLE joined AS SELECT v.id, v.label, h.val FROM visible_src v JOIN __hidden_src h ON v.id = h.id",
        ],
        min_events=6,
    )

    # The CTAS event should have visible_src as input but NOT __hidden_src
    # Find START events with 'joined' as output (COMPLETE events may not have inputs)
    ctas_start_events = [
        e for e in events
        if e.get("eventType") == "START"
        and any("joined" in (o.get("name") or "") for o in get_outputs(e))
    ]
    assert ctas_start_events, "Should find CTAS START event with 'joined' as output"

    for e in ctas_start_events:
        # visible_src should be in inputs
        input_names = [inp.get("name", "") for inp in get_inputs(e)]
        assert any("visible_src" in n for n in input_names), (
            f"visible_src should be in inputs. Found: {input_names}"
        )
        # __hidden_src should NOT be in inputs
        assert all("__hidden_src" not in n for n in input_names), (
            f"__hidden_src should NOT be in inputs. Found: {input_names}"
        )

    # joined should appear as output
    assert any_dataset_in_events(events, "joined"), "joined table should appear"


# ── Operation-Specific Filtering ──────────────────────────────────────


@pytest.mark.integration
def test_insert_into_excluded_table_filtered(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """INSERT into an excluded table should not generate lineage events."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    conn.execute(
        "SET duck_lineage_exclude_dataset_prefixes = 'memory.main.__skip_'"
    )

    conn.execute("CREATE TABLE __skip_sink (id INT)")
    conn.execute("INSERT INTO __skip_sink VALUES (1), (2), (3)")

    sleep(3)

    jobs = marquez_client.list_jobs(ns)
    skip_jobs = [j for j in jobs if "__skip_sink" in (j.get("name") or "")]
    assert not skip_jobs, (
        f"INSERT into excluded table should not create jobs, found: "
        f"{[j.get('name') for j in skip_jobs]}"
    )


@pytest.mark.integration
def test_drop_excluded_table_filtered(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """DROP of an excluded table should not generate lineage events."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    conn.execute(
        "SET duck_lineage_exclude_dataset_prefixes = 'memory.main.__drop_'"
    )

    conn.execute("CREATE TABLE __drop_target (id INT)")
    conn.execute("DROP TABLE __drop_target")

    sleep(3)

    jobs = marquez_client.list_jobs(ns)
    drop_jobs = [j for j in jobs if "__drop_target" in (j.get("name") or "")]
    assert not drop_jobs, (
        f"DROP of excluded table should not create jobs, found: "
        f"{[j.get('name') for j in drop_jobs]}"
    )


@pytest.mark.integration
def test_update_excluded_table_filtered(
    lineage_connection, marquez_client, clean_marquez_namespace
):
    """UPDATE of an excluded table should not generate lineage events for the update."""
    conn = lineage_connection
    ns = clean_marquez_namespace

    conn.execute(
        "SET duck_lineage_exclude_dataset_prefixes = 'memory.main.__upd_'"
    )

    conn.execute("CREATE TABLE __upd_target (id INT, val INT)")
    conn.execute("INSERT INTO __upd_target VALUES (1, 10), (2, 20)")
    conn.execute("UPDATE __upd_target SET val = 99 WHERE id = 1")

    sleep(3)

    jobs = marquez_client.list_jobs(ns)
    upd_jobs = [j for j in jobs if "__upd_target" in (j.get("name") or "")]
    assert not upd_jobs, (
        f"UPDATE of excluded table should not create jobs, found: "
        f"{[j.get('name') for j in upd_jobs]}"
    )
