"""
Tests for how the process exits while lineage events are still queued.

Regression tests for a segfault at process exit: OpenSSL's atexit cleanup used to run before
the worker thread had drained the event queue, so the next HTTPS request crashed inside OpenSSL
(exit code 139 / -11). They run DuckDB in a subprocess because they assert on how it exits.
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

import pytest

_STATEMENTS = 20

_EXIT_SCRIPT = textwrap.dedent(
    """
    import sys
    import duckdb

    extension_path, url, statements = sys.argv[1], sys.argv[2], int(sys.argv[3])
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{extension_path}'")
    conn.execute(f"SET duck_lineage_url = '{url}'")
    conn.execute("SET duck_lineage_timeout = 5")
    conn.execute("CREATE TABLE exit_src AS SELECT range AS a FROM range(100)")
    for i in range(statements - 1):
        conn.execute(f"CREATE TABLE exit_out_{i} AS SELECT a * {i} AS b FROM exit_src")
    # Exit right away, while the worker thread still has events queued.
    """
)


class _RecordingHandler(BaseHTTPRequestHandler):
    received = None

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        self.received.append(self.path)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):  # silence noisy stderr logging
        pass


def _serve(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


def _stop(server, thread):
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


@pytest.fixture
def http_backend():
    """Local HTTP server that records POSTed events."""
    received = []
    handler = type("_Handler", (_RecordingHandler,), {"received": received})
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = _serve(server)
    try:
        yield SimpleNamespace(url=f"http://127.0.0.1:{server.server_address[1]}/api/v1/lineage", received=received)
    finally:
        _stop(server, thread)


@pytest.fixture
def untrusted_https_backend(tmp_path):
    """
    Local HTTPS server with a self-signed certificate that the extension does not trust.

    Every delivery fails certificate verification and is retried, so the queue is always
    backed up when the process exits.
    """
    openssl = shutil.which("openssl")
    if not openssl:
        pytest.skip("openssl CLI not available to generate a self-signed certificate")

    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    result = subprocess.run(
        [openssl, "req", "-x509", "-newkey", "rsa:2048", "-keyout", str(key), "-out", str(cert), "-days", "1"]
        + ["-nodes", "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1,DNS:localhost"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"openssl could not generate a self-signed certificate: {result.stderr}")

    received = []
    handler = type("_Handler", (_RecordingHandler,), {"received": received})
    server = HTTPServer(("127.0.0.1", 0), handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certfile=str(cert), keyfile=str(key))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = _serve(server)
    try:
        yield SimpleNamespace(url=f"https://127.0.0.1:{server.server_address[1]}/api/v1/lineage", received=received)
    finally:
        _stop(server, thread)


def _run_and_exit(extension_path, url):
    started = time.time()
    result = subprocess.run(
        [sys.executable, "-c", _EXIT_SCRIPT, extension_path, url, str(_STATEMENTS)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result, time.time() - started


@pytest.mark.integration
def test_exit_with_failing_https_backend_is_clean_and_fast(extension_path, untrusted_https_backend):
    """Exiting with HTTPS events still queued must not crash, nor retry every queued event first."""
    result, elapsed = _run_and_exit(extension_path, untrusted_https_backend.url)

    assert result.returncode == 0, f"process exited with {result.returncode}: {result.stderr[-2000:]}"
    assert untrusted_https_backend.received == []
    assert elapsed < 30, f"process took {elapsed:.1f}s to exit"


@pytest.mark.integration
def test_exit_delivers_queued_events(extension_path, http_backend):
    """Events still queued when the process exits are delivered before it terminates."""
    result, _ = _run_and_exit(extension_path, http_backend.url)

    assert result.returncode == 0, f"process exited with {result.returncode}: {result.stderr[-2000:]}"
    # One START and one COMPLETE event per statement
    assert len(http_backend.received) == 2 * _STATEMENTS
