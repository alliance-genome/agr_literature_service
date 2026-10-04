"""The large-scale threshold must be one number everywhere (SCRUM-6614).

The search indexer's collapse cutoff lives in debezium/ksql_queries.ksql
(``CASE WHEN TAG_COUNT <= N``) and the loaders' curator-email reporting uses
LARGE_SCALE_THRESHOLD from lit_processing/data_ingest/utils/large_scale.py.
If they drift, the emails say papers are summarized in search when they are
not (or the reverse) — so drift fails CI here instead.
"""
import re
from pathlib import Path

from agr_literature_service.lit_processing.data_ingest.utils.large_scale import (
    LARGE_SCALE_THRESHOLD,
)

KSQL = Path(__file__).resolve().parents[1] / "debezium" / "ksql_queries.ksql"


def test_ksql_collapse_threshold_equals_the_shared_constant():
    thresholds = re.findall(r"TAG_COUNT\s*<=\s*(\d+)", KSQL.read_text())
    # exactly one CASE decides the collapse; a second occurrence would mean a
    # new consumer that this test (and the shared constant) should cover
    assert len(thresholds) == 1
    assert int(thresholds[0]) == LARGE_SCALE_THRESHOLD


def test_loaders_reexport_the_shared_constant():
    from agr_literature_service.lit_processing.data_ingest.for_migration import (
        sgd_reference_tag_utils,
        zfin_reference_tag_utils,
    )
    assert sgd_reference_tag_utils.LARGE_SCALE_THRESHOLD is LARGE_SCALE_THRESHOLD
    assert zfin_reference_tag_utils.LARGE_SCALE_THRESHOLD is LARGE_SCALE_THRESHOLD


DEBEZIUM = KSQL.parent


def test_shell_threshold_equals_the_shared_constant():
    # status_manager.sh (sourced by setup.sh) carries the shell copy: Gate 3's
    # expected-tag count and the collect_list cap below both read it.
    shell = re.findall(r"^LARGE_SCALE_THRESHOLD=(\d+)\s*$",
                       (DEBEZIUM / "status_manager.sh").read_text(), re.MULTILINE)
    assert shell == [str(LARGE_SCALE_THRESHOLD)]


def test_tet_group_collect_list_is_capped_per_query():
    # ksqlDB's collect_list is unlimited by default, and the uncapped group
    # list made each tag re-serialise its whole group (O(n^2), the 2026-10-03
    # prod reindex crawled at ~1 tag/s). setup.sh must send the cap with the
    # topic_entity_tag_groups statement only -- a server-wide limit would
    # truncate authors and the other per-reference lists.
    setup = (DEBEZIUM / "setup.sh").read_text()
    assert re.search(r"CREATE\[\[:space:\]\]\+TABLE\[\[:space:\]\]\+topic_entity_tag_groups", setup)
    assert '"ksql.functions.collect_list.limit"' in setup
    assert '"${LARGE_SCALE_THRESHOLD}"' in setup
    compose = (DEBEZIUM.parent / "docker-compose.yaml").read_text()
    assert "collect_list.limit" not in compose  # never server-wide
