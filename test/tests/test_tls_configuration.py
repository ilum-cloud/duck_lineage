"""
Tests for TLS / SSL configuration of the OpenLineage HTTP client.

These cover the SET/GET surface of the new options plus an end-to-end check against
a local self-signed HTTPS server that records received events. The end-to-end tests
reproduce the original report (HTTPS backend behind an untrusted/internal CA) and
verify the fix: events are rejected by default but succeed once a CA bundle is
supplied via ``duck_lineage_ca_cert_file`` or when verification is disabled.
"""

import shutil
import ssl
import subprocess
import sys
import textwrap
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import duckdb
import pytest

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _new_connection(extension_path):
    """Fresh in-memory DuckDB connection with the extension loaded (no backend yet)."""
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{extension_path}'")
    conn.execute("SET duck_lineage_debug = true")
    return conn


def _run_lineage_query(conn):
    """Run queries that produce OpenLineage events (input + output datasets)."""
    conn.execute("CREATE TABLE tls_probe (id INTEGER)")
    conn.execute("INSERT INTO tls_probe VALUES (1), (2)")
    conn.execute("SELECT count(*) FROM tls_probe")


def _wait_for(received, count=1, timeout=10.0):
    """Poll the recorded-request list until it reaches ``count`` or the timeout elapses."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(received) >= count:
            return True
        time.sleep(0.1)
    return len(received) >= count


@pytest.fixture(autouse=True, scope="module")
def _restore_tls_defaults(extension_path):
    """
    Reset TLS-related settings after this module.

    The LineageClient is a process-wide singleton, so configuration set here would
    otherwise leak into test files that run later in the same pytest session.
    """
    yield
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    try:
        conn.execute(f"LOAD '{extension_path}'")
        conn.execute("SET duck_lineage_ca_cert_file = ''")
        conn.execute("SET duck_lineage_ca_cert_dir = ''")
        conn.execute("SET duck_lineage_proxy = ''")
        conn.execute("SET duck_lineage_ssl_verify = true")
        conn.execute("SET duck_lineage_max_retries = 3")
        conn.execute("SET duck_lineage_timeout = 10")
    finally:
        conn.close()


@pytest.fixture
def tls_backend(tmp_path):
    """
    Start a local HTTPS server with a self-signed certificate that records POSTed bodies.

    Yields an object with ``.url`` (the lineage endpoint), ``.cert`` (path to the PEM CA
    bundle that trusts this server), and ``.received`` (list of recorded requests).
    """
    openssl = shutil.which("openssl")
    if not openssl:
        pytest.skip("openssl CLI not available to generate a self-signed certificate")

    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    result = subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1,DNS:localhost",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"openssl could not generate a self-signed certificate: {result.stderr}")

    received = []

    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else b""
            received.append({"path": self.path, "body": body})
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):  # silence noisy stderr logging
            pass

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certfile=str(cert), keyfile=str(key))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    port = server.server_address[1]

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        yield SimpleNamespace(
            url=f"https://127.0.0.1:{port}/api/v1/lineage",
            cert=str(cert),
            received=received,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


# ---------------------------------------------------------------------------
# SET / GET surface
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_tls_options_set_and_get(extension_path):
    """The new TLS options can be set and read back via current_setting()."""
    conn = _new_connection(extension_path)
    try:
        conn.execute("SET duck_lineage_ca_cert_file = '/etc/ssl/certs/ca-bundle.pem'")
        conn.execute("SET duck_lineage_ca_cert_dir = '/etc/ssl/certs'")
        conn.execute("SET duck_lineage_ssl_verify = false")
        conn.execute("SET duck_lineage_proxy = 'http://proxy.company.com:8080'")

        ca_file = conn.execute("SELECT current_setting('duck_lineage_ca_cert_file')").fetchone()[0]
        assert ca_file == "/etc/ssl/certs/ca-bundle.pem"

        ca_dir = conn.execute("SELECT current_setting('duck_lineage_ca_cert_dir')").fetchone()[0]
        assert ca_dir == "/etc/ssl/certs"

        verify = conn.execute("SELECT current_setting('duck_lineage_ssl_verify')").fetchone()[0]
        assert verify in (False, "false")

        proxy = conn.execute("SELECT current_setting('duck_lineage_proxy')").fetchone()[0]
        assert proxy == "http://proxy.company.com:8080"
    finally:
        conn.close()


@pytest.mark.integration
def test_tls_ssl_verify_defaults_to_true(extension_path):
    """TLS verification is enabled by default."""
    conn = _new_connection(extension_path)
    try:
        verify = conn.execute("SELECT current_setting('duck_lineage_ssl_verify')").fetchone()[0]
        assert verify in (True, "true")
    finally:
        conn.close()


@pytest.mark.integration
def test_tls_invalid_cert_and_proxy_do_not_crash(extension_path):
    """A bogus CA path and proxy must not crash the extension."""
    conn = _new_connection(extension_path)
    try:
        conn.execute("SET duck_lineage_url = 'http://192.0.2.1:1/api/v1/lineage'")
        conn.execute("SET duck_lineage_ca_cert_file = '/nonexistent/ca.pem'")
        conn.execute("SET duck_lineage_proxy = 'http://invalid-proxy.invalid:9'")
        conn.execute("SET duck_lineage_max_retries = 0")
        conn.execute("SET duck_lineage_timeout = 2")

        _run_lineage_query(conn)

        # Extension still functional after attempting (and failing) to deliver events.
        assert conn.execute("SELECT 1").fetchone()[0] == 1
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# End-to-end TLS behavior against a self-signed HTTPS server
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_https_self_signed_rejected_by_default(extension_path, tls_backend):
    """Without a trusted CA, the self-signed backend cert is rejected (the reported bug)."""
    conn = _new_connection(extension_path)
    try:
        conn.execute(f"SET duck_lineage_url = '{tls_backend.url}'")
        conn.execute("SET duck_lineage_ca_cert_file = ''")
        conn.execute("SET duck_lineage_ca_cert_dir = ''")
        conn.execute("SET duck_lineage_proxy = ''")
        conn.execute("SET duck_lineage_ssl_verify = true")
        conn.execute("SET duck_lineage_max_retries = 0")
        conn.execute("SET duck_lineage_timeout = 5")

        _run_lineage_query(conn)

        # Verification must fail, so the server should receive nothing.
        assert not _wait_for(
            tls_backend.received, count=1, timeout=5
        ), f"expected no events to be delivered, got {len(tls_backend.received)}"
    finally:
        conn.close()


@pytest.mark.integration
def test_https_self_signed_accepted_with_ca_cert_file(extension_path, tls_backend):
    """Supplying the self-signed cert as the CA bundle lets verification succeed."""
    conn = _new_connection(extension_path)
    try:
        conn.execute(f"SET duck_lineage_url = '{tls_backend.url}'")
        conn.execute(f"SET duck_lineage_ca_cert_file = '{tls_backend.cert}'")
        conn.execute("SET duck_lineage_ca_cert_dir = ''")
        conn.execute("SET duck_lineage_proxy = ''")
        conn.execute("SET duck_lineage_ssl_verify = true")
        conn.execute("SET duck_lineage_max_retries = 1")
        conn.execute("SET duck_lineage_timeout = 5")

        _run_lineage_query(conn)

        assert _wait_for(tls_backend.received, count=1, timeout=10), "expected at least one delivered event"
    finally:
        conn.close()


@pytest.mark.integration
def test_https_self_signed_accepted_with_verify_disabled(extension_path, tls_backend):
    """Disabling verification (escape hatch) lets events reach the self-signed backend."""
    conn = _new_connection(extension_path)
    try:
        conn.execute(f"SET duck_lineage_url = '{tls_backend.url}'")
        conn.execute("SET duck_lineage_ca_cert_file = ''")
        conn.execute("SET duck_lineage_ca_cert_dir = ''")
        conn.execute("SET duck_lineage_proxy = ''")
        conn.execute("SET duck_lineage_ssl_verify = false")
        conn.execute("SET duck_lineage_max_retries = 1")
        conn.execute("SET duck_lineage_timeout = 5")

        _run_lineage_query(conn)

        assert _wait_for(tls_backend.received, count=1, timeout=10), "expected at least one delivered event"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Process exit
# ---------------------------------------------------------------------------
# These run in a subprocess because they assert on how the interpreter exits.

_EXIT_STATEMENTS = 20

_EXIT_SCRIPT = textwrap.dedent("""
    import sys
    import duckdb

    extension_path, url, ca_cert_file, statements = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{extension_path}'")
    conn.execute(f"SET duck_lineage_url = '{url}'")
    conn.execute(f"SET duck_lineage_ca_cert_file = '{ca_cert_file}'")
    conn.execute("SET duck_lineage_timeout = 5")
    conn.execute("CREATE TABLE exit_src AS SELECT range AS a FROM range(100)")
    for i in range(statements - 1):
        conn.execute(f"CREATE TABLE exit_out_{i} AS SELECT a * {i} AS b FROM exit_src")
    # Exit right away, while the worker thread still has events queued.
    """)


def _run_and_exit(extension_path, url, ca_cert_file):
    started = time.time()
    result = subprocess.run(
        [sys.executable, "-c", _EXIT_SCRIPT, extension_path, url, ca_cert_file, str(_EXIT_STATEMENTS)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result, time.time() - started


@pytest.mark.integration
def test_exit_with_queued_https_events_delivers_them_without_crashing(extension_path, tls_backend):
    """
    A process that exits while HTTPS events are still queued must not crash, and the events must arrive.

    Regression test: OpenSSL's atexit cleanup used to run before the worker thread drained the queue,
    so the next request segfaulted inside OpenSSL (exit code 139 / -11).
    """
    result, _ = _run_and_exit(extension_path, tls_backend.url, tls_backend.cert)

    assert result.returncode == 0, f"process exited with {result.returncode}: {result.stderr[-2000:]}"
    # One START and one COMPLETE event per statement
    assert len(tls_backend.received) == 2 * _EXIT_STATEMENTS


@pytest.mark.integration
def test_exit_with_failing_https_backend_is_clean_and_fast(extension_path, tls_backend):
    """
    With a CA bundle that cannot be loaded every delivery fails, so the queue is always backed up at exit.
    The process must still exit cleanly, and without retrying every queued event first.
    """
    result, elapsed = _run_and_exit(extension_path, tls_backend.url, "/nonexistent/ca-bundle.pem")

    assert result.returncode == 0, f"process exited with {result.returncode}: {result.stderr[-2000:]}"
    assert tls_backend.received == []
    assert elapsed < 30, f"process took {elapsed:.1f}s to exit"
