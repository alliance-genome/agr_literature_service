"""Tests for zfin_reference_tag_utils.py (shared ZFIN reference-tag helpers)."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from agr_literature_service.lit_processing.data_ingest.for_migration import (
    zfin_reference_tag_utils as util,
)


class TestBuildZfinPubToRefCurie:

    def test_maps_pub_curie_to_reference_curie(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = [
            ("ZFIN:ZDB-PUB-1", "AGRKB:101000000000001"),
            ("ZFIN:ZDB-PUB-2", "AGRKB:101000000000002"),
        ]
        assert util.build_zfin_pub_to_ref_curie(db) == {
            "ZFIN:ZDB-PUB-1": "AGRKB:101000000000001",
            "ZFIN:ZDB-PUB-2": "AGRKB:101000000000002",
        }


class TestBuildZfinCorpusRefCuries:

    def test_returns_set_of_reference_curies(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = [
            ("AGRKB:101000000000001",),
            ("AGRKB:101000000000002",),
        ]
        assert util.build_zfin_corpus_ref_curies(db) == {
            "AGRKB:101000000000001", "AGRKB:101000000000002",
        }


class TestLoadExistingEntityPairs:

    def test_returns_reference_entity_pairs(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = [
            ("AGRKB:1", "ZFIN:ZDB-GENE-1"),
            ("AGRKB:2", "ZFIN:ZDB-GENE-2"),
        ]
        result = util.load_existing_entity_pairs(db, 229, "ATP:0000005")
        assert result == {("AGRKB:1", "ZFIN:ZDB-GENE-1"), ("AGRKB:2", "ZFIN:ZDB-GENE-2")}
        # Pin the topic + entity_type scoping so the skip set is exactly this
        # loader's pure entity tags (not any mixed tag on the shared source).
        sql, params = db.execute.call_args[0]
        assert "tet.topic = :atp" in str(sql)
        assert "tet.entity_type = :atp" in str(sql)
        assert params == {"sid": 229, "atp": "ATP:0000005"}


def _make_source_db(existing_first, existing_second=None):
    """db whose query() returns a mod row for ModModel and, for the source model,
    yields ``existing_first`` then ``existing_second`` from one_or_none()."""
    db = MagicMock()
    mod_q = MagicMock()
    mod_q.filter_by.return_value.one.return_value = MagicMock(mod_id=5)
    src_q = MagicMock()
    src_q.filter_by.return_value.one_or_none.side_effect = [existing_first, existing_second]
    db.query.side_effect = lambda model: mod_q if model is util.ModModel else src_q
    return db


class TestGetOrCreateSource:

    def test_returns_existing_source_id(self):
        db = _make_source_db(MagicMock(topic_entity_tag_source_id=229))
        assert util.get_or_create_source(db) == 229
        db.add.assert_not_called()

    def test_creates_source_when_absent(self):
        db = _make_source_db(None)
        with patch.object(util, "TopicEntityTagSourceModel",
                          return_value=MagicMock(topic_entity_tag_source_id=500)):
            assert util.get_or_create_source(db) == 500
        db.add.assert_called_once()
        db.commit.assert_called_once()

    def test_recovers_from_unique_constraint_race(self):
        db = _make_source_db(None, MagicMock(topic_entity_tag_source_id=229))
        db.commit.side_effect = IntegrityError("stmt", "params", Exception("dup"))
        with patch.object(util, "TopicEntityTagSourceModel",
                          return_value=MagicMock(topic_entity_tag_source_id=999)):
            assert util.get_or_create_source(db) == 229
        db.rollback.assert_called_once()


class TestResolveReferenceCurie:

    def test_prefers_publication_map(self):
        db = MagicMock()
        result = util.resolve_reference_curie(
            db, "ZFIN:ZDB-PUB-1", None, {"ZFIN:ZDB-PUB-1": "AGRKB:1"}, {})
        assert result == "AGRKB:1"
        db.query.assert_not_called()

    @patch.object(util, "get_reference_id_by_pmid", return_value=1234)
    def test_pmid_token_fallback(self, mock_get_ref):
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one.return_value = MagicMock(curie="AGRKB:2")
        result = util.resolve_reference_curie(db, "PMID:999", None, {}, {})
        assert result == "AGRKB:2"
        mock_get_ref.assert_called_once_with(db, "999")

    @patch.object(util, "get_reference_id_by_pmid", return_value=1234)
    def test_separate_pmid_argument_fallback(self, mock_get_ref):
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one.return_value = MagicMock(curie="AGRKB:3")
        result = util.resolve_reference_curie(db, "ZFIN:ZDB-PUB-X", "999", {}, {})
        assert result == "AGRKB:3"

    def test_unresolved_tallies_prefix(self):
        db = MagicMock()
        unresolved = {}
        result = util.resolve_reference_curie(
            db, "ZFIN:ZDB-PUB-X", None, {}, {}, unresolved)
        assert result is None
        assert unresolved == {"ZFIN": 1}


class TestFormatNotInCorpusSection:

    def test_empty(self):
        assert util.format_not_in_corpus_section({}, "log.txt") == ""

    def test_lists_small_set_without_cap_notice(self):
        section = util.format_not_in_corpus_section(
            {"AGRKB:9": "ZFIN:ZDB-PUB-9"}, "log.txt")
        assert "Papers not in ZFIN corpus (1)" in section
        assert "ZFIN:ZDB-PUB-9 (AGRKB:9)" in section
        assert "more" not in section

    def test_caps_large_list_and_points_at_log(self):
        refs = {f"AGRKB:{i}": f"ZFIN:ZDB-PUB-{i}" for i in range(150)}
        section = util.format_not_in_corpus_section(refs, "not_in_corpus.log")
        assert "Papers not in ZFIN corpus (150)" in section
        assert f"and {150 - util.NOT_IN_CORPUS_REPORT_CAP} more" in section
        assert "not_in_corpus.log" in section


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
