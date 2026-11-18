"""Tests for backfill_deprecated_data_context.py (SCRUM-6815)."""
import csv
from unittest.mock import patch

import pytest

from agr_literature_service.api.models import ModModel, ReferenceModel, TagSourceModel, TopicEntityTagModel
from agr_literature_service.lit_processing.oneoff_scripts import backfill_deprecated_data_context as script
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ...fixtures import db  # noqa

DEPRECATED = "ATP:0000325"
ROOT = "ATP:0000323"


@pytest.fixture
def tags(db):  # noqa
    """One topic-only and one entity tag on ATP:0000325 for WB, ZFIN, SGD and FB, a WB topic
    tag on the root, a tag already on ATP:0000405, and an MGI entity tag."""
    populate_test_mods()
    reference = ReferenceModel(curie="AGRKB:101000000681500", category="research_article")
    db.add(reference)
    db.flush()
    sources = {}
    for abbreviation in ("WB", "ZFIN", "SGD", "FB", "MGI"):
        mod_id = db.query(ModModel.mod_id).filter(ModModel.abbreviation == abbreviation).scalar()
        source = TagSourceModel(source_evidence_assertion="ECO:0006156", source_method=f"{abbreviation} pipeline",
                                description="test", data_provider=abbreviation,
                                secondary_data_provider_id=mod_id)
        db.add(source)
        db.flush()
        sources[abbreviation] = source.tag_source_id

    ids = {}

    def add(key, abbreviation, data_context, entity=None):
        tag = TopicEntityTagModel(reference_id=reference.reference_id, topic="ATP:0000005",
                                  entity_type="ATP:0000005" if entity else None, entity=entity,
                                  entity_id_validation="alliance" if entity else None,
                                  species="NCBITaxon:6239", tag_source_id=sources[abbreviation], negated=False,
                                  data_novelty="ATP:0000335", data_context=data_context)
        db.add(tag)
        db.flush()
        ids[key] = tag.topic_entity_tag_id

    for abbreviation in ("WB", "ZFIN", "SGD", "FB"):
        add(f"{abbreviation} topic", abbreviation, DEPRECATED)
        add(f"{abbreviation} entity", abbreviation, DEPRECATED, entity=f"{abbreviation}:gene1")
    add("WB root topic", "WB", ROOT)
    add("FB already 405", "FB", "ATP:0000405")
    add("MGI entity", "MGI", DEPRECATED, entity="MGI:1")
    db.commit()
    return ids


def _contexts(db, ids):  # noqa
    db.expire_all()
    return {key: db.query(TopicEntityTagModel.data_context).filter(
        TopicEntityTagModel.topic_entity_tag_id == tag_id).scalar() for key, tag_id in ids.items()}


def _run(db, tmp_path, **kwargs):  # noqa
    id_log = tmp_path / "ids.csv"
    with patch.object(script, "create_postgres_session", return_value=db):
        result = script.run(kwargs.pop("commit", True), kwargs.pop("include_wb_topic_root", False),
                            str(id_log), **kwargs)
    return result, id_log


class TestBackfill:

    def test_moves_each_mod_by_the_table(self, db, tags, tmp_path):  # noqa
        result, _ = _run(db, tmp_path)
        assert _contexts(db, tags) == {
            "WB topic": "ATP:0000405", "WB entity": "ATP:0000406",
            "ZFIN topic": "ATP:0000405", "ZFIN entity": "ATP:0000406",
            "SGD topic": "ATP:0000405", "SGD entity": "ATP:0000405",
            "FB topic": "ATP:0000405", "FB entity": "ATP:0000405",
            "WB root topic": ROOT,              # only with --include-wb-topic-root
            "FB already 405": "ATP:0000405",
            "MGI entity": "ATP:0000405",
        }
        assert result["left_on_deprecated"] == 0

    def test_dry_run_writes_nothing(self, db, tags, tmp_path):  # noqa
        planned, id_log = _run(db, tmp_path, commit=False)
        assert planned == {"WB/ZFIN tags with an entity": 2, "every other tag": 7}
        assert all(context in (DEPRECATED, ROOT, "ATP:0000405") for context in _contexts(db, tags).values())
        assert _contexts(db, tags)["WB entity"] == DEPRECATED
        assert not id_log.exists()

    def test_wb_topic_root_moves_only_when_asked(self, db, tags, tmp_path):  # noqa
        _run(db, tmp_path, include_wb_topic_root=True)
        assert _contexts(db, tags)["WB root topic"] == "ATP:0000405"

    def test_every_change_is_logged_for_an_undo(self, db, tags, tmp_path):  # noqa
        _, id_log = _run(db, tmp_path, batch_size=2)
        with open(id_log) as f:
            rows = list(csv.reader(f))
        assert rows[0] == ["topic_entity_tag_id", "old_data_context", "new_data_context"]
        logged = {int(r[0]): (r[1], r[2]) for r in rows[1:]}
        assert len(logged) == 9
        assert logged[tags["ZFIN entity"]] == (DEPRECATED, "ATP:0000406")
        assert logged[tags["SGD entity"]] == (DEPRECATED, "ATP:0000405")
        assert tags["FB already 405"] not in logged

    def test_limit_caps_the_total(self, db, tags, tmp_path):  # noqa
        result, _ = _run(db, tmp_path, limit=3, batch_size=2)
        moved = sum(v for k, v in result.items() if k != "left_on_deprecated")
        assert moved == 3
        assert result["left_on_deprecated"] == 6
