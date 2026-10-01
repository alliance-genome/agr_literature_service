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
