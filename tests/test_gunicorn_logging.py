"""Tests for gunicorn_logging.SplitStreamLogger (SCRUM-6248).

The Docker GELF driver labels whatever a container writes to stderr as ERROR,
so gunicorn's routine error-log lines must reach stdout and only ERROR and
CRITICAL may stay on stderr.
"""

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
from gunicorn.config import Config

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from gunicorn_logging import SplitStreamLogger  # noqa: E402

APP_SOURCE = '''
async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})
'''


def _make_logger(errorlog="-"):
    """Build the logger inside the test, so it binds capsys's streams."""
    cfg = Config()
    cfg.set("errorlog", errorlog)
    cfg.set("loglevel", "debug")
    return SplitStreamLogger(cfg), cfg


def test_levels_are_split_between_the_streams(capsys):
    log, _ = _make_logger()
    log.debug("debug line")
    log.info("info line")
    log.warning("warning line")
    log.error("error line")
    log.critical("critical line")
    out, err = capsys.readouterr()

    for line in ("debug line", "info line", "warning line"):
        assert line in out and line not in err
    for line in ("error line", "critical line"):
        assert line in err and line not in out


def test_gunicorn_format_is_kept(capsys):
    """The alerter and viewer key off the `[LEVEL]` preamble; it must survive."""
    log, _ = _make_logger()
    log.info("Booting worker with pid: 42")
    log.error("Exception in worker process")
    out, err = capsys.readouterr()
    assert "[INFO] Booting worker with pid: 42" in out
    assert "[ERROR] Exception in worker process" in err


def test_reload_does_not_duplicate_lines(capsys):
    """setup() runs again on SIGHUP; handlers must be replaced, not stacked."""
    log, cfg = _make_logger()
    log.setup(cfg)
    log.setup(cfg)
    log.info("once")
    log.error("once too")
    out, err = capsys.readouterr()
    assert out.count("once") == 1
    assert err.count("once too") == 1
    # gunicorn.error is process-wide and pytest may attach its own capture
    # handlers to it, so count only the ones this class owns.
    ours = [h for h in log.error_log.handlers if getattr(h, "_split_stream", False)]
    assert len(ours) == 2
    assert sorted(h.stream is sys.stdout for h in ours) == [False, True]


def test_log_file_is_left_alone(capsys, tmp_path):
    """Stream severity only matters for '-'; a file keeps gunicorn's handler."""
    logfile = tmp_path / "error.log"
    log, _ = _make_logger(errorlog=str(logfile))
    log.info("to the file")
    for handler in log.error_log.handlers:
        handler.flush()
    out, err = capsys.readouterr()
    assert "to the file" in logfile.read_text()
    assert "to the file" not in out and "to the file" not in err
    assert not any(getattr(h, "_split_stream", False) for h in log.error_log.handlers)


def test_gunicorn_conf_uses_the_split_logger():
    """Guard against the setting being dropped from the deployed config."""
    settings: dict = {}
    exec(compile((REPO_ROOT / "gunicorn.conf.py").read_text(), "gunicorn.conf.py", "exec"), settings)
    assert settings["errorlog"] == "-"
    assert settings["logger_class"] == "gunicorn_logging.SplitStreamLogger"


def test_api_dockerfile_ships_the_module():
    dockerfile = (REPO_ROOT / "docker" / "api.dockerfile").read_text()
    assert "COPY ./gunicorn_logging.py ." in dockerfile


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.skipif(sys.platform == "win32", reason="gunicorn does not run on Windows")
def test_real_gunicorn_worker_lifecycle_goes_to_stdout(tmp_path):
    """End to end: gunicorn + the project's uvicorn worker, as the API runs.

    Boots, serves enough requests to trip max_requests so a worker recycles,
    then shuts down. Every lifecycle line must be on stdout and none on stderr.
    """
    (tmp_path / "tiny_app.py").write_text(APP_SOURCE)
    port = _free_port()
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(tmp_path), str(REPO_ROOT)]))
    stdout_file = tmp_path / "stdout"
    stderr_file = tmp_path / "stderr"
    with open(stdout_file, "w") as out, open(stderr_file, "w") as err:
        proc = subprocess.Popen(
            [sys.executable, "-m", "gunicorn", "tiny_app:app",
             "-c", "/dev/null",
             "--bind", f"127.0.0.1:{port}",
             "--workers", "1",
             "--worker-class", "uvicorn_worker.UvicornWorker",
             "--max-requests", "2",
             "--error-logfile", "-",
             "--logger-class", "gunicorn_logging.SplitStreamLogger"],
            cwd=tmp_path, env=env, stdout=out, stderr=err,
        )
        try:
            deadline = time.monotonic() + 30
            served = 0
            while served < 6 and time.monotonic() < deadline:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=2) as conn:
                        conn.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                        if conn.recv(64).startswith(b"HTTP/1.1 200"):
                            served += 1
                except OSError:
                    time.sleep(0.2)
            assert served >= 6, "gunicorn never served requests"
            time.sleep(1)
        finally:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=30)

    stdout = stdout_file.read_text()
    stderr = stderr_file.read_text()
    for marker in ("Booting worker with pid", "Started server process",
                   "Uvicorn worker initialized", "Shutting down: Master"):
        assert marker in stdout, f"{marker!r} not on stdout:\n{stdout}"
    # Two workers booted means the max_requests recycle really happened.
    assert stdout.count("Booting worker with pid") >= 2, stdout
    assert "[INFO]" not in stderr, f"INFO lines still on stderr:\n{stderr}"
