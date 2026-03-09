"""
E2E tests for the demo quickstart pipeline.

Runs the full demo (Marquez + DuckDB ETL seed) non-interactively and verifies
that lineage events land correctly in Marquez.
"""

import os
import subprocess
import pytest

import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from marquez import TestMarquezClient
from event_helpers import (
    assert_valid_event,
    get_inputs,
)

pytestmark = pytest.mark.demo


@pytest.fixture(scope="session", autouse=True)
def _check_marquez():
    """Override conftest's _check_marquez — demo_seed starts Marquez itself."""
    pass


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEMO_SCRIPT = os.path.join(REPO_ROOT, "test", "demo.sh")
DEMO_NAMESPACE = "demo"

# Expected tables from the ETL pipeline
SOURCE_TABLES = {"customers", "products", "orders", "order_items"}
STAGING_TABLES = {"stg_order_details", "stg_customer_orders"}
ANALYTICS_TABLES = {
    "analytics_revenue_by_product",
    "analytics_customer_lifetime_value",
    "analytics_daily_sales",
}
SUMMARY_TABLES = {"executive_summary"}
ALL_TABLES = SOURCE_TABLES | STAGING_TABLES | ANALYTICS_TABLES | SUMMARY_TABLES


def _short_name(full_name):
    """Extract just the table name from a fully-qualified dataset name like 'memory.main.orders'."""
    return full_name.rsplit(".", 1)[-1]


def _input_names(event):
    """Extract lowercase short input dataset names from an event."""
    return {_short_name(inp.get("name", "")).lower() for inp in get_inputs(event)}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def demo_seed():
    """Run the demo seed pipeline, then tear down after tests."""
    # Add locally built DuckDB to PATH so demo.sh uses it
    env = os.environ.copy()
    build_dir = os.path.join(REPO_ROOT, "build", "release")
    if os.path.isfile(os.path.join(build_dir, "duckdb")):
        env["PATH"] = build_dir + os.pathsep + env.get("PATH", "")

    result = subprocess.run(
        ["bash", DEMO_SCRIPT, "--seed-only"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    assert result.returncode == 0, (
        f"demo.sh --seed-only failed (rc={result.returncode}):\n"
        f"stdout: {result.stdout[-2000:]}\n"
        f"stderr: {result.stderr[-2000:]}"
    )
    yield
    # Teardown
    subprocess.run(
        ["bash", DEMO_SCRIPT, "--down"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.fixture(scope="module")
def marquez():
    """Marquez client for querying the demo namespace."""
    return TestMarquezClient("http://localhost:5000")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_demo_datasets_exist(marquez):
    """All 10 expected tables appear as datasets in Marquez."""
    dataset_names = {n.lower() for n in marquez.list_dataset_names(DEMO_NAMESPACE)}
    for table in ALL_TABLES:
        assert any(
            table.lower() in n for n in dataset_names
        ), f"Expected dataset '{table}' not found. Got: {sorted(dataset_names)}"


def _find_start_events_by_output(events, output_table):
    """Find START events that have the given table as an output dataset."""
    result = []
    for e in events:
        if e.get("eventType") != "START":
            continue
        for out in e.get("outputs") or []:
            if _short_name(out.get("name", "")).lower() == output_table.lower():
                result.append(e)
                break
    return result


def test_demo_staging_lineage(marquez):
    """Staging tables have correct upstream inputs."""
    events = marquez.wait_for_events(DEMO_NAMESPACE, min_events=20, timeout_seconds=30)
    assert len(events) >= 20, f"Expected >=20 events, got {len(events)}"

    # stg_order_details <- orders, order_items, products (inputs are on START events)
    sod_events = _find_start_events_by_output(events, "stg_order_details")
    assert sod_events, "No START events for stg_order_details"
    sod_inputs = _input_names(sod_events[0])
    for expected in ("orders", "order_items", "products"):
        assert expected in sod_inputs, f"stg_order_details missing input '{expected}'. Got: {sod_inputs}"

    # stg_customer_orders <- customers, orders
    sco_events = _find_start_events_by_output(events, "stg_customer_orders")
    assert sco_events, "No START events for stg_customer_orders"
    sco_inputs = _input_names(sco_events[0])
    for expected in ("customers", "orders"):
        assert expected in sco_inputs, f"stg_customer_orders missing input '{expected}'. Got: {sco_inputs}"


def test_demo_analytics_lineage(marquez):
    """Analytics tables have correct upstream inputs."""
    events = marquez.wait_for_events(DEMO_NAMESPACE, min_events=20, timeout_seconds=30)

    # analytics_revenue_by_product <- stg_order_details (inputs are on START events)
    rev_events = _find_start_events_by_output(events, "analytics_revenue_by_product")
    assert rev_events, "No START events for analytics_revenue_by_product"
    assert "stg_order_details" in _input_names(rev_events[0])

    # analytics_daily_sales <- stg_order_details
    ds_events = _find_start_events_by_output(events, "analytics_daily_sales")
    assert ds_events, "No START events for analytics_daily_sales"
    assert "stg_order_details" in _input_names(ds_events[0])

    # analytics_customer_lifetime_value <- stg_customer_orders, order_items, products
    clv_events = _find_start_events_by_output(events, "analytics_customer_lifetime_value")
    assert clv_events, "No START events for analytics_customer_lifetime_value"
    clv_inputs = _input_names(clv_events[0])
    for expected in ("stg_customer_orders", "order_items", "products"):
        assert (
            expected in clv_inputs
        ), f"analytics_customer_lifetime_value missing input '{expected}'. Got: {clv_inputs}"


def test_demo_executive_summary_lineage(marquez):
    """Executive summary has all 3 analytics tables as inputs."""
    events = marquez.wait_for_events(DEMO_NAMESPACE, min_events=20, timeout_seconds=30)

    es_events = _find_start_events_by_output(events, "executive_summary")
    assert es_events, "No START events for executive_summary"
    es_inputs = _input_names(es_events[0])
    for expected in (
        "analytics_revenue_by_product",
        "analytics_customer_lifetime_value",
        "analytics_daily_sales",
    ):
        assert expected in es_inputs, f"executive_summary missing input '{expected}'. Got: {es_inputs}"


def test_demo_events_have_valid_structure(marquez):
    """Events have valid OpenLineage structure."""
    events = marquez.wait_for_events(DEMO_NAMESPACE, min_events=20, timeout_seconds=30)

    # Validate a sample of events
    for event in events[:5]:
        assert_valid_event(event, DEMO_NAMESPACE)

    # Both START and COMPLETE types should be present
    event_types = {e.get("eventType") for e in events}
    assert "START" in event_types, f"No START events found. Types: {event_types}"
    assert "COMPLETE" in event_types, f"No COMPLETE events found. Types: {event_types}"


# ---------------------------------------------------------------------------
# Comprehensive e2e validation tests
# ---------------------------------------------------------------------------

# The 10 ETL output tables (CTAS queries) that should have jobs
ETL_OUTPUT_TABLES = sorted(ALL_TABLES)


def test_demo_expected_event_count(marquez):
    """At least 20 events (START+COMPLETE for 10 CTAS). Catches queue drain bug."""
    events = marquez.wait_for_events(DEMO_NAMESPACE, min_events=20, timeout_seconds=30)
    assert len(events) >= 20, (
        f"Expected >=20 events (10 CTAS × 2 event types), got {len(events)}. "
        "This likely means events were lost during shutdown (queue drain bug)."
    )


def test_demo_etl_jobs_exist(marquez):
    """All 10 ETL jobs (by output dataset name) exist via the jobs API."""
    jobs = marquez.list_jobs(DEMO_NAMESPACE)
    job_names = {j.get("name", "").lower() for j in jobs}

    for table in ETL_OUTPUT_TABLES:
        assert any(
            table.lower() in name for name in job_names
        ), f"No job found for table '{table}'. Jobs: {sorted(job_names)}"


def test_demo_jobs_have_completed_runs(marquez):
    """Every ETL job has latestRun.state == COMPLETED and non-empty latestRuns."""
    jobs = marquez.list_jobs(DEMO_NAMESPACE)

    for table in ETL_OUTPUT_TABLES:
        matching = [j for j in jobs if table.lower() in j.get("name", "").lower()]
        assert matching, f"No job found for table '{table}'"

        job = matching[0]
        latest_run = job.get("latestRun") or {}
        assert latest_run, f"Job for '{table}' ({job.get('name')}) has no latestRun"
        assert latest_run.get("state") == "COMPLETED", (
            f"Job for '{table}' ({job.get('name')}) latestRun.state is "
            f"'{latest_run.get('state')}', expected 'COMPLETED'"
        )


def test_demo_no_orphaned_running_runs(marquez):
    """No ETL job has latestRun.state == RUNNING (catches lost COMPLETE events)."""
    jobs = marquez.list_jobs(DEMO_NAMESPACE)
    # Only check ETL jobs (those matching our known output tables)
    etl_jobs = [j for j in jobs if any(table.lower() in j.get("name", "").lower() for table in ETL_OUTPUT_TABLES)]
    running_jobs = [j.get("name") for j in etl_jobs if (j.get("latestRun") or {}).get("state") == "RUNNING"]
    assert not running_jobs, f"Jobs stuck in RUNNING state (missing COMPLETE events): {running_jobs}"


def test_demo_datasets_have_schema(marquez):
    """Each CTAS dataset has a non-empty fields array (schema facet)."""
    # Dataset names in Marquez are fully qualified (e.g. demo.main.orders)
    all_names = marquez.list_dataset_names(DEMO_NAMESPACE)
    for table in ETL_OUTPUT_TABLES:
        # Find the fully qualified name that ends with our table name
        full_name = next((n for n in all_names if n.lower().endswith(table.lower())), None)
        assert full_name is not None, f"Dataset '{table}' not found in Marquez. Got: {sorted(all_names)}"

        dataset = marquez.get_dataset(DEMO_NAMESPACE, full_name)
        assert dataset is not None, f"Dataset '{full_name}' not found in Marquez"

        fields = dataset.get("fields") or []
        assert len(fields) > 0, f"Dataset '{table}' has no schema fields. " "Schema extraction may not be working."


def test_demo_start_complete_pairs(marquez):
    """Every job that has a START event also has a COMPLETE event."""
    events = marquez.wait_for_events(DEMO_NAMESPACE, min_events=20, timeout_seconds=30)

    # Collect job names by event type
    start_jobs = set()
    complete_jobs = set()
    for event in events:
        job_name = (event.get("job") or {}).get("name", "").lower()
        event_type = event.get("eventType")
        if event_type == "START":
            start_jobs.add(job_name)
        elif event_type == "COMPLETE":
            complete_jobs.add(job_name)

    missing_complete = start_jobs - complete_jobs
    assert not missing_complete, f"Jobs with START but no COMPLETE event (event loss): {sorted(missing_complete)}"


def test_demo_teardown():
    """demo.sh --down exits cleanly."""
    result = subprocess.run(
        ["bash", DEMO_SCRIPT, "--down"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"demo.sh --down failed (rc={result.returncode}):\n" f"stderr: {result.stderr[-1000:]}"
    )

    # Marquez should no longer be reachable
    ping = subprocess.run(
        ["curl", "-sf", "http://localhost:5001/ping"],
        capture_output=True,
        timeout=5,
    )
    assert ping.returncode != 0, "Marquez should not be reachable after --down"


def test_demo_clean():
    """demo.sh --clean removes downloaded DuckDB binary."""
    duckdb_dir = os.path.join(REPO_ROOT, "test", ".duckdb")

    if not os.path.isdir(duckdb_dir):
        pytest.skip("No .duckdb/ directory (system DuckDB was used)")

    result = subprocess.run(
        ["bash", DEMO_SCRIPT, "--clean"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"demo.sh --clean failed (rc={result.returncode}):\n" f"stderr: {result.stderr[-1000:]}"
    )
    assert not os.path.isdir(duckdb_dir), f"{duckdb_dir} should be removed after --clean"
