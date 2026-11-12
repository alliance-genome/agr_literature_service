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


ROOT = SETUP.parents[1]


def test_connector_is_rebuilt_on_every_restart():
    # The connector's image carries the Jersey logging config (SCRUM-6805). Without --build,
    # compose reused the image built before that fix, and prod kept sending 17 Jersey startup
    # warnings to stderr on every rebuild.
    starts = [line.strip() for line in (ROOT / "Makefile").read_text().splitlines()
              if re.search(r"\bup\b.*\bdbz_connector\b", line)]
    assert starts, "Makefile no longer starts dbz_connector; update this test"
    for line in starts:
        assert "--build" in line, f"dbz_connector started without --build: {line}"


def test_ksqldb_jvm_warnings_are_silenced_before_cms():
    # ksql-run-class defaults to the CMS collector, and Java 11 warns on stderr that it is
    # deprecated. -XX:-PrintWarnings only works if it comes before the option it would warn about.
    dockerfile = (ROOT / "docker" / "ksqldb.dockerfile").read_text()
    opts = re.search(r'ENV KSQL_JVM_PERFORMANCE_OPTS="([^"]*)"', dockerfile)
    assert opts, "KSQL_JVM_PERFORMANCE_OPTS is no longer set; update this test"
    flags = opts.group(1).split()
    assert flags[0] == "-XX:-PrintWarnings"
    assert "-XX:+UseConcMarkSweepGC" in flags
