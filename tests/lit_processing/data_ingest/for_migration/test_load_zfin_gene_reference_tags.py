"""Tests for load_zfin_gene_reference_tags.py"""

from unittest.mock import MagicMock, patch

import pytest

from agr_literature_service.lit_processing.data_ingest.for_migration import (
    load_zfin_gene_reference_tags as mod,
)


class TestParseGenePublication:

    def _write(self, tmp_path, lines):
        f = tmp_path / "gene_publication.txt"
        f.write_text("\n".join(lines) + "\n")
        return str(f)

    def test_parses_valid_rows_and_skips_header(self, tmp_path):
        path = self._write(tmp_path, [
            "Gene Symbol\tGene ID\tPublication ID\tPublication Type\tPubMed ID",
            "fgf8a\tZDB-GENE-990415-72\tZDB-PUB-150729-10\tJournal\t26020745",
        ])
        rows = list(mod.parse_gene_publication(path))
        assert rows == [("ZDB-GENE-990415-72", "ZDB-PUB-150729-10", "26020745")]

    def test_blank_or_missing_pmid_yields_empty_string(self, tmp_path):
        path = self._write(tmp_path, [
            "etv5b\tZDB-GENE-991228-4\tZDB-PUB-060503-2\tCuration\t",  # blank pmid
            "abc\tZDB-GENE-1\tZDB-PUB-1",                              # only 3 fields
        ])
        rows = list(mod.parse_gene_publication(path))
        assert rows == [
            ("ZDB-GENE-991228-4", "ZDB-PUB-060503-2", ""),
            ("ZDB-GENE-1", "ZDB-PUB-1", ""),
        ]

    def test_skips_non_gene_or_non_pub_prefixes_and_short_lines(self, tmp_path):
        path = self._write(tmp_path, [
            "x\tZDB-ALT-2\tZDB-PUB-9\tJournal\t1",   # entity not a gene
            "y\tZDB-GENE-3\tZDB-ALT-9\tJournal\t2",  # reference not a pub
            "justonecolumn",                          # too few fields
        ])
        assert list(mod.parse_gene_publication(path)) == []


class TestBuildZfinPubToRefCurie:

    def test_maps_pub_curie_to_reference_curie(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = [
            ("ZFIN:ZDB-PUB-1", "AGRKB:101000000000001"),
            ("ZFIN:ZDB-PUB-2", "AGRKB:101000000000002"),
        ]
        result = mod.build_zfin_pub_to_ref_curie(db)
        assert result == {
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
        result = mod.build_zfin_corpus_ref_curies(db)
        assert result == {"AGRKB:101000000000001", "AGRKB:101000000000002"}


def _make_source_db(existing_source):
    """Build a mock db whose query() returns the mod row for ModModel and the
    given (existing or None) source for TopicEntityTagSourceModel."""
    db = MagicMock()
    mod_q = MagicMock()
    mod_q.filter_by.return_value.one.return_value = MagicMock(mod_id=5)
    src_q = MagicMock()
    src_q.filter_by.return_value.one_or_none.return_value = existing_source
    db.query.side_effect = lambda model: mod_q if model is mod.ModModel else src_q
    return db


class TestGetOrCreateSource:

    def test_returns_existing_source_id_without_creating(self):
        db = _make_source_db(MagicMock(topic_entity_tag_source_id=229))
        assert mod.get_or_create_source(db) == 229
        db.add.assert_not_called()
        db.commit.assert_not_called()

    def test_creates_source_when_absent(self):
        db = _make_source_db(None)
        with patch.object(mod, "TopicEntityTagSourceModel",
                          return_value=MagicMock(topic_entity_tag_source_id=500)):
            assert mod.get_or_create_source(db) == 500
        db.add.assert_called_once()
        db.commit.assert_called_once()


class TestResolveReferenceCurie:

    def test_prefers_zfin_publication_match(self):
        db = MagicMock()
        pub_map = {"ZFIN:ZDB-PUB-1": "AGRKB:1"}
        result = mod.resolve_reference_curie(db, "ZDB-PUB-1", "999", pub_map, {})
        assert result == "AGRKB:1"
        db.query.assert_not_called()  # no PMID fallback needed

    @patch.object(mod, "get_reference_id_by_pmid", return_value=1234)
    def test_falls_back_to_pmid(self, mock_get_ref):
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one.return_value = MagicMock(curie="AGRKB:2")
        cache = {}
        result = mod.resolve_reference_curie(db, "ZDB-PUB-X", "999", {}, cache)
        assert result == "AGRKB:2"
        assert cache == {"999": "AGRKB:2"}
        mock_get_ref.assert_called_once_with(db, "999")

    @patch.object(mod, "get_reference_id_by_pmid", return_value=None)
    def test_returns_none_when_pmid_not_found(self, mock_get_ref):
        db = MagicMock()
        cache = {}
        assert mod.resolve_reference_curie(db, "ZDB-PUB-X", "999", {}, cache) is None
        assert cache == {"999": None}

    def test_returns_none_when_no_match_and_no_pmid(self):
        db = MagicMock()
        assert mod.resolve_reference_curie(db, "ZDB-PUB-X", "", {}, {}) is None


class TestBuildTagPayload:

    def test_pure_entity_gene_tag_fields(self):
        payload = mod._build_tag_payload("AGRKB:1", "ZFIN:ZDB-GENE-1", source_id=229)
        data = payload.dict()
        assert data["reference_curie"] == "AGRKB:1"
        assert data["topic"] == mod.GENE_ATP
        assert data["entity_type"] == mod.GENE_ATP
        assert data["entity"] == "ZFIN:ZDB-GENE-1"
        assert data["entity_id_validation"] == mod.ENTITY_ID_VALIDATION
        assert data["species"] == mod.DANIO_RERIO_TAXON
        assert data["data_novelty"] == mod.EXISTING_DATA_NOVELTY_ATP
        assert data["negated"] is False
        assert data["topic_entity_tag_source_id"] == 229
        # created_by/updated_by come from set_global_user_id, not the payload.
        assert data.get("created_by") is None
        assert data.get("updated_by") is None


class TestComposeReportMessage:

    def _counts(self, **overrides):
        counts = {
            "total_pairs": 10, "created": 6, "skipped_duplicate": 2,
            "duplicate_in_file": 1, "missing_reference": 1, "not_in_corpus": 0,
            "errors": 0,
        }
        counts.update(overrides)
        return counts

    def test_download_failed_message(self):
        msg = mod.compose_report_message({"download_failed": True})
        assert "Failed to download" in msg

    def test_includes_counts(self):
        msg = mod.compose_report_message(self._counts())
        assert "Entity tags created: 6" in msg
        assert "Total gene-reference pairs in file: 10" in msg

    def test_lists_not_in_corpus_papers(self):
        counts = self._counts(
            not_in_corpus=1,
            not_in_corpus_refs={"AGRKB:9": "ZFIN:ZDB-PUB-9"},
        )
        msg = mod.compose_report_message(counts)
        assert "Papers not in ZFIN corpus (1)" in msg
        assert "ZFIN:ZDB-PUB-9 (AGRKB:9)" in msg


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
