"""
Shared helpers and assertion functions for OpenLineage event validation.

Provides deep validation of event structure, facet metadata (_producer, _schemaURL),
dataset namespaces/names, and facet-specific content.
"""

import re
from time import sleep

# ── Constants matching C++ lineage_event_builder.hpp:331-354 ───────────

PRODUCER = "https://github.com/Ilum/duckdb-openlineage"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"

FACET_SCHEMAS = {
    "sql": "https://openlineage.io/spec/job/sql/1-0-0.json",
    "schema": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
    "dataSource": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
    "catalog": "https://openlineage.io/spec/facets/1-0-0/CatalogDatasetFacet.json",
    "lifecycleStateChange": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json",
    "datasetType": "https://openlineage.io/spec/facets/1-0-0/DatasetTypeDatasetFacet.json",
    "outputStatistics": "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json",
    "processing_engine": "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json",
    "symlinks": "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json",
    "errorMessage": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
    "parent": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
}

VALID_EVENT_TYPES = {"START", "COMPLETE", "FAIL"}
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
ISO_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


# ── Null-safe accessors ────────────────────────────────────────────────


def get_outputs(event):
    return event.get("outputs") or []


def get_inputs(event):
    return event.get("inputs") or []


def get_facets(obj):
    return obj.get("facets") or {}


def get_run_facets(event):
    run = event.get("run") or {}
    return run.get("facets") or {}


# ── Query + poll ───────────────────────────────────────────────────────


def run_and_wait(conn, marquez_client, namespace, queries, min_events, timeout=30):
    for q in queries:
        conn.execute(q)
    sleep(2)
    return marquez_client.wait_for_events(namespace, min_events, timeout_seconds=timeout)


# ── Event filtering ────────────────────────────────────────────────────


def find_events_by_job(events, job_name_contains):
    return [e for e in events if job_name_contains.upper() in ((e.get("job") or {}).get("name") or "").upper()]


def find_complete_events(events, job_name_contains=None):
    result = [e for e in events if e.get("eventType") == "COMPLETE"]
    if job_name_contains:
        result = [e for e in result if job_name_contains.upper() in ((e.get("job") or {}).get("name") or "").upper()]
    return result


# ── Deep validation: events ────────────────────────────────────────────


def assert_valid_event(event, expected_namespace):
    assert event.get("producer") == PRODUCER, f"Event producer mismatch: {event.get('producer')!r} != {PRODUCER!r}"
    assert (
        event.get("schemaURL") == SCHEMA_URL
    ), f"Event schemaURL mismatch: {event.get('schemaURL')!r} != {SCHEMA_URL!r}"
    assert event.get("eventType") in VALID_EVENT_TYPES, f"Invalid eventType: {event.get('eventType')!r}"

    event_time = event.get("eventTime") or ""
    assert ISO_DATETIME_RE.match(event_time), f"eventTime not ISO 8601: {event_time!r}"

    run = event.get("run") or {}
    run_id = run.get("runId") or ""
    assert UUID_RE.match(run_id), f"run.runId not UUID format: {run_id!r}"

    job = event.get("job") or {}
    assert (
        job.get("namespace") == expected_namespace
    ), f"job.namespace mismatch: {job.get('namespace')!r} != {expected_namespace!r}"
    assert job.get("name"), "job.name should not be empty"


# ── Deep validation: facets ────────────────────────────────────────────


def assert_valid_facet(facet, facet_name):
    assert (
        facet.get("_producer") == PRODUCER
    ), f"Facet '{facet_name}' _producer mismatch: {facet.get('_producer')!r} != {PRODUCER!r}"
    expected_schema = FACET_SCHEMAS.get(facet_name)
    if expected_schema:
        assert (
            facet.get("_schemaURL") == expected_schema
        ), f"Facet '{facet_name}' _schemaURL mismatch: {facet.get('_schemaURL')!r} != {expected_schema!r}"


# ── Deep validation: datasets ─────────────────────────────────────────


def assert_valid_output(output, expected_namespace, expected_name_contains=None):
    assert (
        output.get("namespace") == expected_namespace
    ), f"Output namespace mismatch: {output.get('namespace')!r} != {expected_namespace!r}"
    name = output.get("name") or ""
    assert name, "Output name should not be empty"
    if expected_name_contains:
        assert (
            expected_name_contains.lower() in name.lower()
        ), f"Output name {name!r} should contain {expected_name_contains!r}"

    # Validate all facets on this output
    for facet_name, facet in get_facets(output).items():
        if isinstance(facet, dict) and "_producer" in facet:
            assert_valid_facet(facet, facet_name)


def assert_valid_input(input_ds, expected_name_contains=None):
    assert input_ds.get("namespace"), "Input namespace should not be empty"
    name = input_ds.get("name") or ""
    assert name, "Input name should not be empty"
    if expected_name_contains:
        assert (
            expected_name_contains.lower() in name.lower()
        ), f"Input name {name!r} should contain {expected_name_contains!r}"


# ── Deep validation: specific facets ──────────────────────────────────


def assert_output_has_schema(output, expected_fields):
    """Validate schema facet exists with correct metadata and field names/types.

    expected_fields: dict mapping field name (lowercase) to type substring (uppercase).
    Example: {"id": "INTEGER", "name": "VARCHAR"}
    """
    schema = get_facets(output).get("schema")
    assert schema, f"Output {output.get('name')!r} missing schema facet"
    assert_valid_facet(schema, "schema")

    fields = schema.get("fields") or []
    field_map = {f["name"].lower(): f["type"].upper() for f in fields}

    for name, type_substr in expected_fields.items():
        assert name.lower() in field_map, f"Missing field {name!r} in schema. Fields: {field_map}"
        assert (
            type_substr.upper() in field_map[name.lower()]
        ), f"Field {name!r} type mismatch: {field_map[name.lower()]!r} doesn't contain {type_substr!r}"


def assert_output_has_lifecycle(output, expected_state, previous_name=None):
    """Validate lifecycleStateChange facet with correct metadata and state."""
    lsc = get_facets(output).get("lifecycleStateChange")
    assert lsc, f"Output {output.get('name')!r} missing lifecycleStateChange facet"
    assert_valid_facet(lsc, "lifecycleStateChange")
    assert (
        lsc.get("lifecycleStateChange") == expected_state
    ), f"Lifecycle state mismatch: {lsc.get('lifecycleStateChange')!r} != {expected_state!r}"

    if previous_name is not None:
        prev = lsc.get("previousIdentifier") or {}
        prev_id_name = prev.get("name") or ""
        assert (
            previous_name.lower() in prev_id_name.lower()
        ), f"previousIdentifier.name {prev_id_name!r} should contain {previous_name!r}"


def assert_output_has_dataset_type(output, expected_type):
    """Validate datasetType facet with correct metadata and type."""
    dt = get_facets(output).get("datasetType")
    assert dt, f"Output {output.get('name')!r} missing datasetType facet"
    assert_valid_facet(dt, "datasetType")
    assert (
        dt.get("datasetType") == expected_type
    ), f"datasetType mismatch: {dt.get('datasetType')!r} != {expected_type!r}"


def assert_output_has_statistics(output):
    """Validate outputStatistics facet in outputFacets with correct metadata."""
    output_facets = output.get("outputFacets") or {}
    stats = output_facets.get("outputStatistics")
    assert stats, f"Output {output.get('name')!r} missing outputStatistics facet"
    assert_valid_facet(stats, "outputStatistics")
    assert "rowCount" in stats, f"outputStatistics missing rowCount"
    assert stats["rowCount"] >= 0, f"Unexpected negative rowCount: {stats['rowCount']}"


# ── Collector helpers ──────────────────────────────────────────────────


def collect_lifecycle_states(events):
    states = []
    for e in events:
        for output in get_outputs(e):
            lsc_facet = get_facets(output).get("lifecycleStateChange") or {}
            lsc = lsc_facet.get("lifecycleStateChange")
            if lsc:
                states.append(lsc)
    return states


def collect_dataset_types(events):
    types = []
    for e in events:
        for output in get_outputs(e):
            dt_facet = get_facets(output).get("datasetType") or {}
            dt = dt_facet.get("datasetType")
            if dt:
                types.append(dt)
    return types


# ── Deep validation: Marquez dataset objects ───────────────────────────
# These validate the objects returned by Marquez API (GET /api/v1/namespaces/{ns}/datasets/{name}),
# NOT the raw OpenLineage events. Marquez aggregates event data into rich dataset/job objects.


def assert_valid_dataset(dataset, expected_namespace, expected_name_contains=None):
    """Validate a Marquez dataset object has correct structure and metadata."""
    # Identity
    ds_id = dataset.get("id") or {}
    assert (
        ds_id.get("namespace") == expected_namespace
    ), f"Dataset id.namespace mismatch: {ds_id.get('namespace')!r} != {expected_namespace!r}"
    assert (
        dataset.get("namespace") == expected_namespace
    ), f"Dataset namespace mismatch: {dataset.get('namespace')!r} != {expected_namespace!r}"

    name = dataset.get("name") or ""
    assert name, "Dataset name should not be empty"
    assert dataset.get("physicalName"), "Dataset physicalName should not be empty"
    if expected_name_contains:
        assert (
            expected_name_contains.lower() in name.lower()
        ), f"Dataset name {name!r} should contain {expected_name_contains!r}"

    # Timestamps
    assert dataset.get("createdAt"), "Dataset createdAt should not be empty"
    assert dataset.get("updatedAt"), "Dataset updatedAt should not be empty"

    # Version
    assert dataset.get("currentVersion"), "Dataset currentVersion should not be empty"

    # Validate all facets have correct _producer/_schemaURL
    for facet_name, facet in get_facets(dataset).items():
        if isinstance(facet, dict) and "_producer" in facet:
            assert_valid_facet(facet, facet_name)


def assert_dataset_has_fields(dataset, expected_fields):
    """Validate dataset fields array has correct names and types.

    expected_fields: dict mapping field name (lowercase) to type substring (uppercase).
    """
    fields = dataset.get("fields") or []
    assert fields, f"Dataset {dataset.get('name')!r} has no fields"

    field_map = {f["name"].lower(): f["type"].upper() for f in fields}
    for name, type_substr in expected_fields.items():
        assert name.lower() in field_map, f"Missing field {name!r} in dataset. Fields: {field_map}"
        assert (
            type_substr.upper() in field_map[name.lower()]
        ), f"Field {name!r} type mismatch: {field_map[name.lower()]!r} doesn't contain {type_substr!r}"


def assert_dataset_has_facet(dataset, facet_name, expected_values=None):
    """Validate a specific facet exists on a dataset with correct metadata.

    expected_values: optional dict of {key: value} to check within the facet.
    """
    facets = get_facets(dataset)
    facet = facets.get(facet_name)
    assert facet, f"Dataset {dataset.get('name')!r} missing facet {facet_name!r}. Has: {list(facets.keys())}"
    assert_valid_facet(facet, facet_name)

    if expected_values:
        for key, value in expected_values.items():
            assert (
                facet.get(key) == value
            ), f"Facet '{facet_name}' field '{key}' mismatch: {facet.get(key)!r} != {value!r}"


def assert_dataset_lifecycle(dataset, expected_state):
    """Validate dataset lastLifecycleState and the lifecycleStateChange facet."""
    assert (
        dataset.get("lastLifecycleState") == expected_state
    ), f"Dataset lastLifecycleState mismatch: {dataset.get('lastLifecycleState')!r} != {expected_state!r}"
    assert_dataset_has_facet(dataset, "lifecycleStateChange", {"lifecycleStateChange": expected_state})


# ── Deep validation: Marquez job objects ───────────────────────────────
# These validate the objects returned by Marquez API (GET /api/v1/namespaces/{ns}/jobs/{name}).


def assert_valid_job(job, expected_namespace):
    """Validate a Marquez job object has correct structure and metadata."""
    # Identity
    job_id = job.get("id") or {}
    assert (
        job_id.get("namespace") == expected_namespace
    ), f"Job id.namespace mismatch: {job_id.get('namespace')!r} != {expected_namespace!r}"
    assert (
        job.get("namespace") == expected_namespace
    ), f"Job namespace mismatch: {job.get('namespace')!r} != {expected_namespace!r}"

    name = job.get("name") or ""
    assert name, "Job name should not be empty"
    assert job.get("type") == "BATCH", f"Job type should be BATCH, got: {job.get('type')!r}"

    # Timestamps
    assert job.get("createdAt"), "Job createdAt should not be empty"
    assert job.get("updatedAt"), "Job updatedAt should not be empty"

    # Validate job facets
    for facet_name, facet in get_facets(job).items():
        if isinstance(facet, dict) and "_producer" in facet:
            assert_valid_facet(facet, facet_name)


def assert_job_has_io(job, expected_input_contains=None, expected_output_contains=None):
    """Validate job has expected inputs and/or outputs."""
    if expected_input_contains:
        inputs = job.get("inputs") or []
        input_names = [i.get("name", "").lower() for i in inputs]
        assert any(
            expected_input_contains.lower() in n for n in input_names
        ), f"Job {job.get('name')!r} missing input containing {expected_input_contains!r}. Inputs: {input_names}"

    if expected_output_contains:
        outputs = job.get("outputs") or []
        output_names = [o.get("name", "").lower() for o in outputs]
        assert any(
            expected_output_contains.lower() in n for n in output_names
        ), f"Job {job.get('name')!r} missing output containing {expected_output_contains!r}. Outputs: {output_names}"


def assert_job_run_completed(job):
    """Validate the job's latest run completed successfully."""
    latest_run = job.get("latestRun") or {}
    assert latest_run, f"Job {job.get('name')!r} has no latestRun"
    assert (
        latest_run.get("state") == "COMPLETED"
    ), f"Job {job.get('name')!r} latestRun.state mismatch: {latest_run.get('state')!r} != 'COMPLETED'"
    assert latest_run.get("id"), "latestRun.id should not be empty"
    assert latest_run.get("startedAt"), "latestRun.startedAt should not be empty"
    assert latest_run.get("endedAt"), "latestRun.endedAt should not be empty"

    # Run facets should include processing_engine
    run_facets = latest_run.get("facets") or {}
    engine = run_facets.get("processing_engine")
    if engine:
        assert_valid_facet(engine, "processing_engine")
        assert engine.get("name") == "DuckDB", f"processing_engine.name should be DuckDB, got: {engine.get('name')!r}"
        assert engine.get("version"), "processing_engine.version should not be empty"


def assert_job_has_sql_facet(job, query_contains):
    """Validate job has a SQL facet containing expected query text."""
    facets = get_facets(job)
    sql = facets.get("sql")
    assert sql, f"Job {job.get('name')!r} missing sql facet"
    assert_valid_facet(sql, "sql")
    query = sql.get("query") or ""
    assert query_contains.lower() in query.lower(), f"Job sql.query {query!r} should contain {query_contains!r}"
