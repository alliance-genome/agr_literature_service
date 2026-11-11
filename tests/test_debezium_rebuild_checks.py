"""SCRUM-6850: an empty table must not break a Debezium rebuild.

On 2026-11-10 a stage rebuild ran ~22 h and then refused promotion. manual_indexing_tag
was empty, so Debezium created no topic for it, ksqlDB never created the source table,
and every statement depending on it failed, including the private index's
topic_entity_tags. setup.sh carried on regardless.

The Makefile now pre-creates every source topic, and debezium/rebuild_checks.sh lets
setup.sh skip empty tables in Gate 1 and stop when ksqlDB is missing an object.
psql and curl are stubbed on PATH here.
"""

import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHECKS = ROOT / "debezium" / "rebuild_checks.sh"
SETUP = ROOT / "debezium" / "setup.sh"
MAKEFILE = ROOT / "Makefile"
CONNECTOR = ROOT / "debezium" / "postgres-source-unified.json"


def _include_list():
    match = re.search(r'"table\.include\.list":\s*"([^"]*)"', CONNECTOR.read_text())
    return [t.replace("public.", "") for t in match.group(1).split(",")]


def _makefile_target(name):
    """The recipe lines of a Makefile target."""
    lines = MAKEFILE.read_text().splitlines()
    start = lines.index(f"{name}:")
    recipe = []
    for line in lines[start + 1:]:
        if not line.startswith("\t"):
            break
        recipe.append(line.strip())
    return recipe


def test_makefile_table_list_matches_the_connector():
    # Run the Makefile's own pipeline ($$ is make's escape for $).
    line = next(lo for lo in MAKEFILE.read_text().splitlines() if lo.startswith("DEBEZIUM_SOURCE_TABLES ="))
    command = re.search(r"\$\(shell (.*)\)$", line).group(1).replace("$$", "$")
    tables = subprocess.run(["bash", "-c", command], cwd=ROOT, capture_output=True, text=True,
                            check=True).stdout.split()
    assert tables == _include_list()
    assert "manual_indexing_tag" in tables


@pytest.mark.parametrize("target", ["restart-debezium-local", "restart-debezium-aws",
                                    "run-debezium-integration-tests", "run-debezium-start-test-env"])
def test_every_rebuild_precreates_all_topics_before_ksql(target):
    recipe = _makefile_target(target)
    create = [i for i, line in enumerate(recipe) if "debezium-create-source-topics" in line]
    ksql = [i for i, line in enumerate(recipe) if "dbz_ksql_server" in line and " up " in line]
    assert create, f"{target} does not pre-create the source topics"
    assert ksql and create[0] < ksql[0], f"{target} starts ksqlDB before the topics exist"


def test_no_target_hardcodes_a_single_topic():
    assert "--topic 'abc.public." not in MAKEFILE.read_text()


@pytest.fixture
def run_checks(tmp_path):
    """Run a snippet with rebuild_checks.sh sourced and psql/curl stubbed.

    empty: tables whose stubbed "SELECT EXISTS" answers f. ksql_names: what the
    stubbed SHOW TABLES returns.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir()

    def _run(snippet, empty=(), broken=(), ksql_names=()):
        (bindir / "psql").write_text(
            "#!/bin/bash\n"
            'sql="${@: -1}"\n'
            + "".join(f'case "$sql" in *" {t})") echo f; exit 0;; esac\n' for t in empty)
            + "".join(f'case "$sql" in *" {t})") echo "ERROR" >&2; exit 2;; esac\n' for t in broken)
            + "echo t\n")
        names = ",".join(f'{{"type":"TABLE","name":"{n}","topic":"t"}}' for n in ksql_names)
        (bindir / "curl").write_text(
            "#!/bin/bash\n"
            f"echo '[{{\"@type\":\"tables\",\"tables\":[{names}]}}]'\n")
        for stub in ("psql", "curl"):
            (bindir / stub).chmod(0o755)
        proc = subprocess.run(
            ["bash", "-c", f'source "{CHECKS}"; {snippet}'],
            env={"PATH": f"{bindir}:/usr/bin:/bin:/usr/local/bin"},
            capture_output=True, text=True)
        return proc

    return _run


def test_gate1_counts_only_tables_with_rows(run_checks):
    proc = run_checks('count_nonempty_source_tables "public.reference,public.manual_indexing_tag,public.mod" '
                      '&& echo "count=$NONEMPTY_SOURCE_TABLES"',
                      empty=["public.manual_indexing_tag"])
    assert proc.returncode == 0
    assert "count=2" in proc.stdout
    assert "public.manual_indexing_tag is empty" in proc.stdout
    assert proc.stderr == ""


def test_gate1_count_fails_when_a_table_cannot_be_checked(run_checks):
    proc = run_checks('count_nonempty_source_tables "public.reference,public.gone"; echo "rc=$?"',
                      broken=["public.gone"])
    assert "rc=1" in proc.stdout


def _write_statements(tmp_path):
    stmtdir = tmp_path / "stmts"
    stmtdir.mkdir()
    (stmtdir / "0001.sql").write_text(
        "CREATE TABLE manual_indexing_tag (\n manual_indexing_tag_id string PRIMARY KEY\n"
        ") WITH (KAFKA_TOPIC='abc.public.manual_indexing_tag', VALUE_FORMAT='json');")
    (stmtdir / "0002.sql").write_text("CREATE TABLE spine_mit AS SELECT 1 FROM spine_obs;")
    (stmtdir / "0003.sql").write_text("create or replace stream reference_joined AS SELECT 1 FROM x;")
    return stmtdir


def test_created_object_names(run_checks, tmp_path):
    stmtdir = _write_statements(tmp_path)
    proc = run_checks(f'ksql_created_objects "{stmtdir}"')
    assert proc.stdout.split() == ["MANUAL_INDEXING_TAG", "REFERENCE_JOINED", "SPINE_MIT"]


def test_verify_passes_when_every_object_exists(run_checks, tmp_path):
    stmtdir = _write_statements(tmp_path)
    proc = run_checks(f'verify_ksql_objects "{stmtdir}" http://ksql/ksql',
                      ksql_names=["MANUAL_INDEXING_TAG", "SPINE_MIT", "REFERENCE_JOINED", "OTHER"])
    assert proc.returncode == 0, proc.stdout
    assert "3 tables/streams expected, 0 missing" in proc.stdout


def test_verify_fails_on_an_acknowledged_but_missing_table(run_checks, tmp_path):
    # 2026-11-10: the source table was reported OK but SHOW TABLES did not list it.
    stmtdir = _write_statements(tmp_path)
    proc = run_checks(f'verify_ksql_objects "{stmtdir}" http://ksql/ksql',
                      ksql_names=["SPINE_MIT", "REFERENCE_JOINED"])
    assert proc.returncode == 1
    assert "MISSING after setup: MANUAL_INDEXING_TAG" in proc.stdout
    assert proc.stderr == ""


def test_verify_fails_when_ksql_cannot_be_listed(run_checks, tmp_path):
    stmtdir = _write_statements(tmp_path)
    proc = run_checks(f'verify_ksql_objects "{stmtdir}" http://ksql/ksql')
    assert proc.returncode == 1
    assert "listed no tables or streams" in proc.stdout


def test_setup_stops_before_the_sinks_when_ksql_fails():
    text = SETUP.read_text()
    stop = text.index("if ! submit_ksql_statements /ksql_queries.ksql; then")
    block = text[stop:text.index("fi\n", stop)]
    assert "exit 1" in block
    assert 'set_reindex_status "error"' in block
    # the sinks are created after the check, so a failed setup never repoints them
    assert stop < text.index("/elasticsearch-sink.json")
    assert "WARNING: one or more ksql statements failed" not in text
