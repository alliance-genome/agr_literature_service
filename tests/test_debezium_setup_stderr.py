"""Guards debezium/setup.sh against writing routine output to stderr.

The dbz_setup container ships its streams to logs.alliancegenome.org through the
Docker GELF driver, which files every stderr line as ERROR. curl's progress meter
and psql's "does not exist, skipping" NOTICEs both go to stderr, so a normal
rebuild used to raise ~40 ERROR records across a dozen alert groups.
"""

import re
from pathlib import Path

SETUP = Path(__file__).resolve().parents[1] / "debezium" / "setup.sh"


def _commands(name):
    """Lines of setup.sh (comments skipped) that run the given command."""
    lines = SETUP.read_text().splitlines()
    return [line for line in lines
            if not line.lstrip().startswith("#") and re.search(rf"(^|\s|\(|=){name}\s", line)]


def test_every_curl_is_silent_but_shows_errors():
    curls = _commands("curl")
    assert curls, "setup.sh has no curl calls; update this test"
    for line in curls:
        flags = re.search(r"curl\s+((?:-\S+\s+)*)", line).group(1)
        silent = re.search(r"(^|\s)-[a-zA-Z]*s", flags)
        assert silent, f"curl without -s writes its progress meter to stderr: {line.strip()}"


def test_publication_drop_suppresses_notices():
    drops = [line for line in _commands("psql") if "DROP PUBLICATION IF EXISTS" in line]
    assert drops, "setup.sh no longer drops publications; update this test"
    for line in drops:
        assert "client_min_messages=warning" in line, (
            f"DROP ... IF EXISTS prints a NOTICE per missing publication on stderr: {line.strip()[:120]}")
