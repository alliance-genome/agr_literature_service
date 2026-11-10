"""download_url_to_file / download_file against a local HTTP server that can
drop the connection mid-body, as MGI's server does now and then (SCRUM-6846)."""
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError

import pytest

from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import (
    IncompleteDownloadError,
    download_file,
    download_url_to_file,
)

BODY = b"x" * 100_000


class _Handler(BaseHTTPRequestHandler):
    # requests seen per path, so a test can make the first N attempts fail
    hits: dict = {}
    short_until = 0

    def log_message(self, *args):
        pass

    def do_GET(self):
        hits = _Handler.hits[self.path] = _Handler.hits.get(self.path, 0) + 1
        if self.path == "/missing":
            self.send_error(404)
            return
        if self.path == "/server-error":
            self.send_error(503)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(BODY)))
        self.end_headers()
        if self.path == "/flaky" and hits <= _Handler.short_until:
            # promise the whole body, send part of it, hang up
            self.wfile.write(BODY[:40_000])
            self.wfile.flush()
            self.close_connection = True
            return
        self.wfile.write(BODY)


@pytest.fixture
def server():
    _Handler.hits = {}
    _Handler.short_until = 0
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()


def test_full_download(server, tmp_path):
    target = tmp_path / "f.json.gz"
    download_url_to_file(server + "/full", str(target))
    assert target.read_bytes() == BODY
    assert not (tmp_path / "f.json.gz.part").exists()


def test_dropped_connection_is_an_error_not_a_short_file(server, tmp_path):
    _Handler.short_until = 99
    target = tmp_path / "f.json.gz"
    with pytest.raises(IncompleteDownloadError, match="got 40000 of 100000 bytes"):
        download_url_to_file(server + "/flaky", str(target), attempts=2, retry_wait=0)
    assert _Handler.hits["/flaky"] == 2
    assert not target.exists()
    assert not (tmp_path / "f.json.gz.part").exists()


def test_retry_gets_the_whole_file(server, tmp_path):
    _Handler.short_until = 2
    target = tmp_path / "f.json.gz"
    download_url_to_file(server + "/flaky", str(target), attempts=3, retry_wait=0)
    assert target.read_bytes() == BODY
    assert _Handler.hits["/flaky"] == 3


def test_not_found_is_not_retried(server, tmp_path):
    with pytest.raises(HTTPError):
        download_url_to_file(server + "/missing", str(tmp_path / "f"), attempts=3, retry_wait=0)
    assert _Handler.hits["/missing"] == 1


def test_server_error_is_retried(server, tmp_path):
    with pytest.raises(HTTPError):
        download_url_to_file(server + "/server-error", str(tmp_path / "f"), attempts=3, retry_wait=0)
    assert _Handler.hits["/server-error"] == 3


def test_failed_attempt_keeps_an_existing_file(server, tmp_path):
    # the file only appears, or is replaced, once a download is complete
    _Handler.short_until = 99
    target = tmp_path / "f"
    target.write_bytes(b"old")
    with pytest.raises(IncompleteDownloadError):
        download_url_to_file(server + "/flaky", str(target), retry_wait=0)
    assert target.read_bytes() == b"old"


def test_download_file_returns_false_on_a_dropped_connection(server, tmp_path):
    _Handler.short_until = 99
    assert download_file(server + "/flaky", str(tmp_path / "f")) is False
    assert _Handler.hits["/flaky"] == 1
    assert download_file(server + "/full", str(tmp_path / "g")) is True
