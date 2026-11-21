"""
Tests for outputStatistics.rowCount on COMPLETE events.

Without RETURNING, DML statements return a single row holding the number of rows they wrote, so rowCount must
come from that value rather than from the number of result rows (which used to make it 1 for every statement).

The events are captured with the file transport, so these tests do not need Marquez.
"""

import json
import time
import uuid

import duckdb
import pytest


@pytest.fixture(autouse=True)
def _restore_transport_defaults(extension_path):
    """The LineageClient is a process-wide singleton: switch back to the HTTP transport after each test."""
    yield
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    try:
        conn.execute(f"LOAD '{extension_path}'")
        conn.execute("RESET duck_lineage_transport")
        conn.execute("RESET duck_lineage_file_path")
        conn.execute("RESET duck_lineage_namespace")
    finally:
        conn.close()


@pytest.fixture
def capture(extension_path, tmp_path):
    """Connection writing its events to a file; yields a function returning the COMPLETE event of a statement."""
    path = tmp_path / "events.jsonl"
    namespace = f"row_count_{uuid.uuid4().hex[:8]}"
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{extension_path}'")
    conn.execute("SET duck_lineage_transport = 'file'")
    conn.execute(f"SET duck_lineage_file_path = '{path}'")
    conn.execute(f"SET duck_lineage_namespace = '{namespace}'")

    def complete_event(sql, timeout=10.0):
        deadline = time.time() + timeout
        while True:
            for line in path.read_text().splitlines():
                event = json.loads(line)
                if (
                    event["eventType"] == "COMPLETE"
                    and event["job"]["namespace"] == namespace
                    and event["job"].get("facets", {}).get("sql", {}).get("query") == sql
                ):
                    return event
            if time.time() > deadline:
                pytest.fail(f"No COMPLETE event for {sql!r}")
            time.sleep(0.1)

    try:
        yield conn, complete_event, tmp_path
    finally:
        conn.close()


def _row_counts(event):
    return [o["outputFacets"]["outputStatistics"]["rowCount"] for o in event.get("outputs", [])]


# (setup statements, statement under test, expected rowCount). {dir} is replaced by a temporary directory.
_CASES = {
    "ctas": ([], "CREATE TABLE rc AS SELECT range AS id FROM range(100)", 100),
    "ctas_empty": (
        [],
        "CREATE TABLE rc AS SELECT range AS id FROM range(100) LIMIT 0",
        0,
    ),
    "insert": (
        ["CREATE TABLE rc (id BIGINT)"],
        "INSERT INTO rc SELECT * FROM range(7)",
        7,
    ),
    "insert_returning": (
        ["CREATE TABLE rc (id BIGINT)"],
        "INSERT INTO rc SELECT * FROM range(3) RETURNING id",
        3,
    ),
    "update": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "UPDATE rc SET id = id + 1000 WHERE id < 20",
        20,
    ),
    "update_returning": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "UPDATE rc SET id = id + 1000 WHERE id < 4 RETURNING id",
        4,
    ),
    "delete": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "DELETE FROM rc WHERE id >= 90",
        10,
    ),
    "delete_nothing": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "DELETE FROM rc WHERE id < 0",
        0,
    ),
    "copy_to": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "COPY rc TO '{dir}/rc.parquet' (FORMAT parquet)",
        100,
    ),
    "copy_to_return_files": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "COPY rc TO '{dir}/rc.parquet' (FORMAT parquet, RETURN_FILES true)",
        100,
    ),
    "copy_to_partitioned_return_stats": (
        ["CREATE TABLE rc AS SELECT range AS id, range % 4 AS p FROM range(100)"],
        "COPY rc TO '{dir}/rc_part' (FORMAT parquet, PARTITION_BY (p), RETURN_STATS true)",
        100,
    ),
    "copy_from": (
        [
            "CREATE TABLE rc_src AS SELECT range AS id FROM range(100)",
            "COPY rc_src TO '{dir}/rc.csv'",
            "CREATE TABLE rc (id BIGINT)",
        ],
        "COPY rc FROM '{dir}/rc.csv'",
        100,
    ),
}


@pytest.mark.integration
@pytest.mark.parametrize("case", list(_CASES))
def test_row_count_is_rows_written(capture, case):
    conn, complete_event, tmp_path = capture
    setup, statement, expected = _CASES[case]
    for sql in setup:
        conn.execute(sql.format(dir=tmp_path))
    statement = statement.format(dir=tmp_path)
    conn.execute(statement).fetchall()

    row_counts = _row_counts(complete_event(statement))
    assert row_counts, "COMPLETE event has no output datasets"
    assert row_counts == [expected] * len(row_counts)


@pytest.mark.integration
def test_select_has_no_output_statistics(capture):
    """A plain SELECT writes nothing, so it has no outputs (and no rowCount) at all."""
    conn, complete_event, _ = capture
    conn.execute("CREATE TABLE rc AS SELECT range AS id FROM range(100)")
    conn.execute("SELECT * FROM rc").fetchall()
    assert complete_event("SELECT * FROM rc").get("outputs", []) == []
