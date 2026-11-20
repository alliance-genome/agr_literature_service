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


def test_zookeeper_startup_lines_are_kept_off_stderr():
    # zkServer.sh echoes "ZooKeeper JMX enabled by default" and "Using config: ..." to
    # stderr, so every rebuild raised two ERROR records. The service is built from a
    # dockerfile that moves just those two lines to stdout.
    compose = (ROOT / "docker-compose.yaml").read_text()
    service = re.search(r"\n  dbz_zookeeper:\n((?:    .*\n|\n)*?)\n  \w", compose)
    assert service, "docker-compose.yaml no longer defines dbz_zookeeper; update this test"
    assert "dockerfile: docker/debezium_zookeeper.dockerfile" in service.group(1)
    assert "image:" not in service.group(1), "dbz_zookeeper uses the stock image again"
    dockerfile = (ROOT / "docker" / "debezium_zookeeper.dockerfile").read_text()
    for line in ('echo "ZooKeeper JMX enabled by default"', 'echo "Using config: $ZOOCFG"'):
        assert line in dockerfile and dockerfile.count(line) >= 2, (
            f"the dockerfile must both patch and check {line}")
    zookeeper_starts = [line for line in (ROOT / "Makefile").read_text().splitlines()
                        if re.search(r"\bup\b.*\bdbz_zookeeper\b", line)]
    assert zookeeper_starts and all("--build" in line for line in zookeeper_starts)
