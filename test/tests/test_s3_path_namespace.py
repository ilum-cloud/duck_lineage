"""
Regression test for OpenLineage S3 path namespace handling.

Per the OpenLineage naming spec (https://openlineage.io/docs/spec/naming/),
S3 datasets must be emitted as:

    namespace = "s3://<bucket>"
    name      = "<key>"

The producer must split URI-style paths at top-level inputs[], outputs[],
AND inside outputs[].facets.columnLineage.fields[*].inputFields[] so that
Marquez (and other consumers) can join field-level lineage to the dataset.

Prerequisites:
    - Marquez at http://localhost:5000
    - MinIO at localhost:9000 (bucket "test-bucket" auto-created by minio-init)
    - Start with: make ducklake-up
"""

import socket
import time
import uuid

import pytest

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "test-bucket"


def _is_port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module", autouse=True)
def _check_minio():
    host, port = MINIO_ENDPOINT.split(":")
    if not _is_port_open(host, int(port)):
        pytest.skip(f"MinIO not reachable at {MINIO_ENDPOINT}")


@pytest.fixture
def s3_connection(lineage_connection):
    """A lineage_connection with httpfs loaded and S3 pointing at MinIO."""
    conn = lineage_connection
    try:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
    except Exception as e:
        pytest.skip(f"httpfs extension not available: {e}")

    conn.execute(f"SET s3_endpoint = '{MINIO_ENDPOINT}'")
    conn.execute(f"SET s3_access_key_id = '{MINIO_ACCESS_KEY}'")
    conn.execute(f"SET s3_secret_access_key = '{MINIO_SECRET_KEY}'")
    conn.execute("SET s3_use_ssl = false")
    conn.execute("SET s3_url_style = 'path'")
    return conn


def _find_copy_event(events):
    """Return the START event whose SQL is the COPY ... TO ... bug repro, or None."""
    for ev in events:
        if ev.get("eventType") != "START":
            continue
        sql = ((ev.get("job") or {}).get("facets") or {}).get("sql", {}).get("query", "")
        if "COPY" in sql and "read_parquet" in sql and "s3://" in sql:
            return ev
    return None


def _wait_for_copy_event(marquez_client, namespace, timeout_seconds=30):
    """Poll until the read_parquet+COPY-TO START event lands in Marquez."""
    deadline = time.time() + timeout_seconds
    last_events = []
    while time.time() < deadline:
        try:
            events = marquez_client._fetch_events(namespace)
        except Exception:
            events = []
        last_events = events
        ev = _find_copy_event(events)
        if ev is not None:
            return ev, events
        time.sleep(1)
    return None, last_events


@pytest.mark.integration
def test_s3_read_and_copy_to_namespace(s3_connection, marquez_client):
    """
    read_parquet('s3://...') + COPY ... TO 's3://...' must produce
    OpenLineage datasets split per the naming spec at every level:
    top-level inputs[], top-level outputs[], and
    outputs[].facets.columnLineage.fields[*].inputFields[].
    """
    conn = s3_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    # Unique key prefix per run so the test is repeatable against a long-lived bucket.
    run_id = uuid.uuid4().hex[:8]
    src_dir = f"src-{run_id}"
    dst_dir = f"dst-{run_id}"
    src_path = f"s3://{MINIO_BUCKET}/{src_dir}/seed.parquet"
    src_glob = f"s3://{MINIO_BUCKET}/{src_dir}/*.parquet"
    dst_path = f"s3://{MINIO_BUCKET}/{dst_dir}/"

    expected_ns = f"s3://{MINIO_BUCKET}"
    expected_input_name = f"{src_dir}/seed.parquet"
    expected_output_name = f"{dst_dir}/"

    # Seed: write a parquet file into the source prefix on MinIO.
    conn.execute(f"COPY (SELECT 1 AS a, 'x' AS b) TO '{src_path}' (FORMAT PARQUET)")

    # Bug repro — matches the SQL from the issue.
    conn.execute(f"""
        COPY (
            SELECT a, b FROM read_parquet('{src_glob}')
        ) TO '{dst_path}' (FORMAT PARQUET)
        """)

    # Poll until our specific read_parquet+COPY-TO START event lands. Using
    # wait_for_events alone would race: the seed write emits a START+COMPLETE
    # pair that satisfies min_events before the repro's events get ingested.
    copy_event, events = _wait_for_copy_event(marquez_client, namespace, timeout_seconds=30)
    assert copy_event is not None, (
        f"Could not find a START event with read_parquet+COPY TO s3:// SQL. "
        f"Events seen: {[(e.get('eventType'), ((e.get('job') or {}).get('facets') or {}).get('sql', {}).get('query', '')[:80]) for e in events]}"
    )

    inputs = copy_event.get("inputs") or []
    outputs = copy_event.get("outputs") or []

    # Diagnostic dump (visible with `pytest -v -s`).
    print("\n--- duck_lineage S3 namespace — captured START event ---")
    print(f"inputs:  {inputs}")
    print(f"outputs: {outputs}")

    # ─── inputs[] split per spec ──────────────────────────────────────────
    matching_inputs = [d for d in inputs if d.get("namespace") == expected_ns and d.get("name") == expected_input_name]
    assert matching_inputs, (
        f"Expected input with namespace={expected_ns!r}, name={expected_input_name!r}; " f"got inputs={inputs!r}"
    )
    # Nothing should still carry the old broken shape.
    for d in inputs:
        assert d.get("namespace") != "file" or not (d.get("name") or "").startswith("s3://"), (
            f"Regression: input dataset still emitted with namespace='file' and " f"s3:// URL in name: {d!r}"
        )

    # ─── outputs[] split per spec ─────────────────────────────────────────
    matching_outputs = [
        d for d in outputs if d.get("namespace") == expected_ns and d.get("name") == expected_output_name
    ]
    assert matching_outputs, (
        f"Expected output with namespace={expected_ns!r}, name={expected_output_name!r}; " f"got outputs={outputs!r}"
    )
    for d in outputs:
        assert d.get("namespace") != "file" or not (d.get("name") or "").startswith("s3://"), (
            f"Regression: output dataset still emitted with namespace='file' and " f"s3:// URL in name: {d!r}"
        )

    # ─── lifecycle facet attached to the split key ────────────────────────
    out = matching_outputs[0]
    facets = out.get("facets") or {}
    assert "lifecycleStateChange" in facets, (
        f"lifecycleStateChange facet missing on output {out!r} — likely indicates "
        f"the facet was attached to a different (namespace,name) key than the dataset "
        f"(builder matches by exact key)."
    )

    # ─── columnLineage.inputFields[] split per spec ───────────────────────
    column_lineage = (facets.get("columnLineage") or {}).get("fields") or {}
    assert column_lineage, f"columnLineage facet should be present on output {out!r}; got facets={list(facets)!r}"
    for col_name, col_def in column_lineage.items():
        for src in col_def.get("inputFields") or []:
            assert (
                src.get("namespace") == expected_ns
            ), f"columnLineage source for column {col_name!r} should use namespace={expected_ns!r}, got {src!r}"
            assert not (src.get("name") or "").startswith(
                "s3://"
            ), f"columnLineage source name should be the object key, not the full s3:// URL — got {src!r}"
            assert (
                src.get("name") == expected_input_name
            ), f"columnLineage source name for column {col_name!r} should be {expected_input_name!r}, got {src!r}"


def _wait_for_event_matching(marquez_client, namespace, predicate, timeout_seconds=30):
    """Poll until any event for the namespace matches predicate(event) -> bool."""
    deadline = time.time() + timeout_seconds
    last_events = []
    while time.time() < deadline:
        try:
            events = marquez_client._fetch_events(namespace)
        except Exception:
            events = []
        last_events = events
        for ev in events:
            if predicate(ev):
                return ev, events
        time.sleep(1)
    return None, last_events


@pytest.mark.integration
def test_s3_multi_file_glob_inputs(s3_connection, marquez_client):
    """
    A glob over multiple s3:// objects must produce one inputs[] entry per
    expanded file, each with the same namespace and a different key. Catches
    regressions in the per-file loop in the optimizer's read_parquet branch.
    """
    conn = s3_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    run_id = uuid.uuid4().hex[:8]
    src_dir = f"multisrc-{run_id}"
    expected_ns = f"s3://{MINIO_BUCKET}"
    expected_keys = {f"{src_dir}/a.parquet", f"{src_dir}/b.parquet"}

    # Seed two parquet files under the same prefix.
    for key in sorted(expected_keys):
        conn.execute(f"COPY (SELECT 1 AS x) TO 's3://{MINIO_BUCKET}/{key}' (FORMAT PARQUET)")

    # Read both via glob — must result in two split inputs.
    glob = f"s3://{MINIO_BUCKET}/{src_dir}/*.parquet"
    conn.execute(f"CREATE TABLE multi_glob AS SELECT * FROM read_parquet('{glob}')")

    def _has_two_split_inputs(ev):
        if ev.get("eventType") != "START":
            return False
        sql = ((ev.get("job") or {}).get("facets") or {}).get("sql", {}).get("query", "")
        if "multi_glob" not in sql:
            return False
        s3_inputs = [
            d
            for d in (ev.get("inputs") or [])
            if d.get("namespace") == expected_ns and (d.get("name") or "") in expected_keys
        ]
        return len(s3_inputs) == 2

    ev, events = _wait_for_event_matching(marquez_client, namespace, _has_two_split_inputs, timeout_seconds=30)
    assert ev is not None, (
        f"Did not see a CREATE TABLE multi_glob START event with two split s3 inputs. "
        f"Last events: {[(e.get('eventType'), [(d.get('namespace'), d.get('name')) for d in (e.get('inputs') or [])]) for e in events]}"
    )

    inputs = ev["inputs"]
    seen_keys = {d["name"] for d in inputs if d.get("namespace") == expected_ns}
    assert seen_keys == expected_keys, f"Expected exactly {expected_keys}, got {seen_keys}. Full inputs: {inputs!r}"
    # All inputs must share the same split namespace.
    for d in inputs:
        assert d.get("namespace") == expected_ns, f"Multi-file glob input has unexpected namespace: {d!r}"


@pytest.mark.integration
def test_s3a_scheme_normalised_to_s3(s3_connection, marquez_client):
    """
    s3a:// is a Hadoop-flavoured alias. The helper normalises it to s3://
    (matching existing Hive Metastore behaviour). Skips cleanly if DuckDB's
    httpfs cannot resolve s3a:// directly in this env.
    """
    conn = s3_connection
    namespace = conn.execute("SELECT current_setting('duck_lineage_namespace')").fetchone()[0]

    run_id = uuid.uuid4().hex[:8]
    src_path = f"s3a://{MINIO_BUCKET}/s3a-{run_id}/seed.parquet"
    expected_ns = f"s3://{MINIO_BUCKET}"
    expected_name = f"s3a-{run_id}/seed.parquet"

    try:
        # Seed via s3:// (always works), then read via s3a:// to exercise the
        # normalisation in the optimizer's input path.
        s3_seed = f"s3://{MINIO_BUCKET}/s3a-{run_id}/seed.parquet"
        conn.execute(f"COPY (SELECT 1 AS x) TO '{s3_seed}' (FORMAT PARQUET)")
        conn.execute(f"CREATE TABLE s3a_in AS SELECT * FROM read_parquet('{src_path}')")
    except Exception as e:
        pytest.skip(f"DuckDB httpfs does not accept s3a:// in this env: {e}")

    def _has_normalised_input(ev):
        if ev.get("eventType") != "START":
            return False
        sql = ((ev.get("job") or {}).get("facets") or {}).get("sql", {}).get("query", "")
        if "s3a_in" not in sql:
            return False
        for d in ev.get("inputs") or []:
            if d.get("namespace") == expected_ns and d.get("name") == expected_name:
                return True
        return False

    ev, events = _wait_for_event_matching(marquez_client, namespace, _has_normalised_input, timeout_seconds=30)
    assert ev is not None, (
        f"Expected an input with namespace={expected_ns!r}, name={expected_name!r} (s3a normalised to s3). "
        f"Last events: {[(e.get('eventType'), [(d.get('namespace'), d.get('name')) for d in (e.get('inputs') or [])]) for e in events]}"
    )

    # Belt-and-braces: nothing should leak the literal s3a scheme into the dataset key.
    for d in ev.get("inputs") or []:
        ns = d.get("namespace") or ""
        nm = d.get("name") or ""
        assert "s3a://" not in ns and "s3a://" not in nm, f"s3a:// leaked into dataset identifier: {d!r}"
