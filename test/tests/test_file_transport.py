"""
Tests for the file transport (duck_lineage_transport = 'file').

Events are appended to duck_lineage_file_path as JSON Lines, so lineage can be captured without a running
OpenLineage backend. None of these tests need Marquez.
"""

import json
import subprocess
import sys
import textwrap
import time
import uuid

import duckdb
import pytest

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _new_connection(extension_path, **config):
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true", **config})
    conn.execute(f"LOAD '{extension_path}'")
    return conn


def _setting(conn, name):
    return conn.execute(f"SELECT current_setting('{name}')").fetchone()[0]


def _read_events(path, namespace, count, timeout=60.0):
    """
    Poll the file until it holds ``count`` events of ``namespace``.

    Events are written by a background thread. Filtering by namespace ignores events that other test modules
    left in the process-wide queue.
    """
    deadline = time.time() + timeout
    while True:
        lines = path.read_text().splitlines() if path.exists() else []
        events = [e for e in map(json.loads, lines) if e["job"]["namespace"] == namespace]
        if len(events) >= count or time.time() > deadline:
            return events
        time.sleep(0.1)


@pytest.fixture(autouse=True)
def _restore_transport_defaults(extension_path):
    """
    Switch back to the HTTP transport after each test.

    The LineageClient is a process-wide singleton, so configuration set here would otherwise leak into test
    files that run later in the same pytest session.
    """
    conn = _new_connection(extension_path)
    # Events left in the process-wide queue by earlier tests are delivered before ours. Without a URL, the ones
    # still bound for an HTTP backend are dropped right away instead of holding ours up.
    conn.execute("SET duck_lineage_url = ''")
    conn.close()
    yield
    conn = _new_connection(extension_path)
    try:
        conn.execute("RESET duck_lineage_transport")
        conn.execute("RESET duck_lineage_file_path")
        conn.execute("RESET duck_lineage_namespace")
    finally:
        conn.close()


@pytest.fixture
def file_conn(extension_path, tmp_path):
    """Connection writing events to a fresh file, under a namespace unique to the test."""
    conn = _new_connection(extension_path)
    conn.execute("SET duck_lineage_transport = 'file'")
    conn.execute(f"SET duck_lineage_file_path = '{tmp_path / 'events.jsonl'}'")
    namespace = f"file_transport_{uuid.uuid4().hex[:8]}"
    conn.execute(f"SET duck_lineage_namespace = '{namespace}'")
    try:
        yield conn, tmp_path / "events.jsonl", namespace
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# SET / GET surface
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_file_transport_defaults(extension_path):
    """HTTP stays the default transport, and no file is configured."""
    conn = _new_connection(extension_path)
    try:
        assert _setting(conn, "duck_lineage_transport") == "http"
        assert _setting(conn, "duck_lineage_file_path") == ""
    finally:
        conn.close()


@pytest.mark.integration
def test_transport_is_validated_and_normalized(extension_path):
    conn = _new_connection(extension_path)
    try:
        conn.execute("SET duck_lineage_transport = 'FILE'")
        assert _setting(conn, "duck_lineage_transport") == "file"

        with pytest.raises(duckdb.InvalidInputException, match="Expected 'http' or 'file'"):
            conn.execute("SET duck_lineage_transport = 'kafka'")
        # A rejected SET leaves the previous value in place
        assert _setting(conn, "duck_lineage_transport") == "file"
    finally:
        conn.close()


@pytest.mark.integration
def test_unwritable_file_path_fails_the_set(extension_path, tmp_path):
    """A path that cannot be written is reported right away instead of every event being dropped later."""
    conn = _new_connection(extension_path)
    try:
        with pytest.raises(duckdb.IOException):
            conn.execute(f"SET duck_lineage_file_path = '{tmp_path / 'missing-dir' / 'events.jsonl'}'")
        assert _setting(conn, "duck_lineage_file_path") == ""
    finally:
        conn.close()


@pytest.mark.integration
def test_null_file_path_disables_file_output(extension_path, tmp_path, monkeypatch):
    """SET ... = NULL must behave like '' and not create a file literally named NULL."""
    monkeypatch.chdir(tmp_path)
    conn = _new_connection(extension_path)
    try:
        conn.execute("SET duck_lineage_file_path = NULL")
        assert list(tmp_path.iterdir()) == []
    finally:
        conn.close()


@pytest.mark.integration
def test_file_path_respects_enable_external_access(extension_path, tmp_path):
    """With external access disabled, SQL must not be able to make the extension write files."""
    conn = _new_connection(extension_path)
    try:
        conn.execute("SET enable_external_access = false")
        with pytest.raises(duckdb.PermissionException):
            conn.execute(f"SET duck_lineage_file_path = '{tmp_path / 'events.jsonl'}'")
        assert not (tmp_path / "events.jsonl").exists()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Event delivery
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_events_are_written_as_json_lines(file_conn, tmp_path):
    conn, path, namespace = file_conn
    assert _setting(conn, "duck_lineage_url") == ""

    conn.execute("CREATE TABLE ft_src AS SELECT range AS id FROM range(10)")
    conn.execute("CREATE TABLE ft_out AS SELECT id * 2 AS doubled FROM ft_src")
    conn.execute(f"COPY ft_out TO '{tmp_path / 'ft_out.parquet'}' (FORMAT parquet)")

    events = _read_events(path, namespace, count=6)
    assert [e["eventType"] for e in events] == ["START", "COMPLETE"] * 3

    runs = {}
    for event in events:
        runs.setdefault(event["run"]["runId"], []).append(event["eventType"])
    assert len(runs) == 3 and all(types == ["START", "COMPLETE"] for types in runs.values())

    outputs = {output["name"] for event in events for output in event.get("outputs", [])}
    assert "memory.main.ft_out" in outputs
    assert str(tmp_path / "ft_out.parquet") in outputs
    inputs = {i["name"] for event in events for i in event.get("inputs", [])}
    assert "memory.main.ft_src" in inputs

    # One compact JSON document per line, newline-terminated
    content = path.read_text()
    assert content.endswith("\n")
    assert all(line.startswith("{") and line.endswith("}") for line in content.splitlines())


@pytest.mark.integration
def test_relative_file_path_is_pinned_to_working_directory(extension_path, tmp_path, monkeypatch):
    """Events are written by a background thread, so a relative path must not follow later chdir() calls."""
    conn = _new_connection(extension_path)
    try:
        monkeypatch.chdir(tmp_path)
        namespace = f"file_transport_{uuid.uuid4().hex[:8]}"
        conn.execute("SET duck_lineage_transport = 'file'")
        conn.execute("SET duck_lineage_file_path = 'relative.jsonl'")
        conn.execute(f"SET duck_lineage_namespace = '{namespace}'")
        monkeypatch.chdir("/")

        conn.execute("CREATE TABLE ft_relative AS SELECT 1 AS x")
        assert len(_read_events(tmp_path / "relative.jsonl", namespace, count=2)) == 2
    finally:
        conn.close()


@pytest.mark.integration
def test_rotated_file_is_recreated(file_conn):
    """A file moved away by log rotation is recreated for the next events."""
    conn, path, namespace = file_conn
    conn.execute("CREATE TABLE ft_before AS SELECT 1 AS x")
    assert len(_read_events(path, namespace, count=2)) == 2

    path.rename(path.with_name("events.jsonl.1"))
    conn.execute("CREATE TABLE ft_after AS SELECT 1 AS x")

    events = _read_events(path, namespace, count=2)
    assert len(events) == 2
    assert all("ft_after" in e["job"]["name"] for e in events)


@pytest.mark.integration
def test_http_transport_does_not_write_to_file(file_conn):
    conn, path, namespace = file_conn
    conn.execute("SET duck_lineage_transport = 'http'")
    conn.execute("CREATE TABLE ft_http AS SELECT 1 AS x")
    time.sleep(1)
    assert _read_events(path, namespace, count=1, timeout=0) == []


# ---------------------------------------------------------------------------
# Process exit and concurrent writers
# ---------------------------------------------------------------------------
# These run in subprocesses: they check what is in the file once the interpreter has exited.

_WRITER_SCRIPT = textwrap.dedent("""
    import sys
    import duckdb

    extension_path, path, statements = sys.argv[1], sys.argv[2], int(sys.argv[3])
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{extension_path}'")
    conn.execute("SET duck_lineage_transport = 'file'")
    conn.execute(f"SET duck_lineage_file_path = '{path}'")
    conn.execute("CREATE TABLE src AS SELECT range AS a FROM range(10)")
    for i in range(statements - 1):
        conn.execute(f"CREATE TABLE out_{i} AS SELECT a * {i} AS b FROM src")
    # Exit right away, while the worker thread still has events queued.
    """)


def _start_writer(extension_path, path, statements):
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            _WRITER_SCRIPT,
            extension_path,
            str(path),
            str(statements),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


@pytest.mark.integration
def test_exit_flushes_queued_events_to_file(extension_path, tmp_path):
    path = tmp_path / "exit.jsonl"
    writer = _start_writer(extension_path, path, statements=100)
    _, stderr = writer.communicate(timeout=120)

    assert writer.returncode == 0, f"process exited with {writer.returncode}: {stderr[-2000:]}"
    # One START and one COMPLETE event per statement
    assert len(path.read_text().splitlines()) == 2 * 100


@pytest.mark.integration
def test_concurrent_processes_do_not_interleave_lines(extension_path, tmp_path):
    """Several processes can append to the same file: every line stays a complete JSON document."""
    if sys.platform == "win32":
        pytest.skip("Concurrent appends from several processes are only atomic on POSIX")
    path = tmp_path / "shared.jsonl"
    writers = [_start_writer(extension_path, path, statements=50) for _ in range(4)]
    for writer in writers:
        _, stderr = writer.communicate(timeout=120)
        assert writer.returncode == 0, f"process exited with {writer.returncode}: {stderr[-2000:]}"

    lines = path.read_text().splitlines()
    assert len(lines) == 4 * 2 * 50
    for line in lines:
        json.loads(line)
