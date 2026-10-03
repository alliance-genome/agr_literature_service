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
    # status_manager.sh carries the shell copy that Gate 3's expected-tag
    # count reads.
    shell = re.findall(r"^LARGE_SCALE_THRESHOLD=(\d+)\s*$",
                       (DEBEZIUM / "status_manager.sh").read_text(), re.MULTILINE)
    assert shell == [str(LARGE_SCALE_THRESHOLD)]


UDAF = DEBEZIUM.parent / "docker" / "ksqldb" / "TetGroupCollectUdaf.java"


def test_udaf_threshold_equals_the_shared_constant():
    # TET_GROUP_COLLECT drops a group's list once it passes THRESHOLD; a
    # different number than the CASE would summarize groups whose list was
    # already dropped (or keep huge lists).
    java = re.findall(r"static final int THRESHOLD = (\d+);", UDAF.read_text())
    assert java == [str(LARGE_SCALE_THRESHOLD)]


def test_tet_groups_use_the_bounded_udaf():
    # collect_list kept every tag of a group, so each tag re-serialised the
    # whole group: O(n^2), ~1 tag/s on the 2026-10-03 prod reindex and still
    # a 130 KB state per big-group tag even with the list capped at 250. The group aggregate must
    # use TET_GROUP_COLLECT, which keeps only the first tag past the threshold.
    stmt = re.search(r"CREATE TABLE topic_entity_tag_groups AS(.*?)EMIT CHANGES",
                     KSQL.read_text(), re.DOTALL).group(1)
    assert "tet_group_collect(map(" in stmt
    assert "collect_list(" not in stmt


def test_ksqldb_image_ships_and_loads_the_udaf():
    # ksqlDB loads UDFs only from ksql.extension.dir: the image must put the
    # jar there and the server must be pointed at it, or every
    # topic_entity_tag_groups statement fails with an unknown function.
    dockerfile = (DEBEZIUM.parent / "docker" / "ksqldb.dockerfile").read_text()
    assert "TetGroupCollectUdaf.java" in dockerfile
    assert "/etc/ksqldb/ext/" in dockerfile
    compose = (DEBEZIUM.parent / "docker-compose.yaml").read_text()
    assert "KSQL_KSQL_EXTENSION_DIR=/etc/ksqldb/ext" in compose
