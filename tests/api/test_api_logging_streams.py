"""The API's application log must stay on stdout (SCRUM-6632).

The Docker GELF driver labels stderr as ERROR, so a root handler on stderr would
turn every INFO line the API logs into an ERROR record and an alert. The API does
not configure root logging itself: the first module to call logging.basicConfig()
during import wins (today lit_processing/utils/s3_utils.py, which passes
stream=sys.stdout). Several other imported modules call basicConfig() without a
stream, which defaults to stderr, so a change in import order could flip this.
"""
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

PROBE = """
import logging
import agr_literature_service.api.main  # noqa: F401
for handler in logging.root.handlers:
    print("ROOT-HANDLER", getattr(getattr(handler, "stream", None), "name", type(handler).__name__))
"""


def test_api_root_logging_is_on_stdout():
    result = subprocess.run([sys.executable, "-c", PROBE], cwd=REPO_ROOT, capture_output=True, text=True,
                            timeout=120)
    assert result.returncode == 0, result.stderr[-2000:]
    streams = [line.split(" ", 1)[1] for line in result.stdout.splitlines() if line.startswith("ROOT-HANDLER ")]
    assert streams, result.stdout[-2000:]
    assert set(streams) == {"<stdout>"}, streams
