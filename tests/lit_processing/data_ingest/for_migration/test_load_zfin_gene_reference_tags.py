"""Tests for load_zfin_gene_reference_tags.py"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

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

    def test_yields_non_gene_entities_but_skips_non_pub_and_short_lines(self, tmp_path):
        # Non-gene entity rows are yielded (the loop counts them); rows without a
        # ZDB-PUB publication id or with too few fields are dropped by the parser.
        path = self._write(tmp_path, [
            "x\tZDB-LINCRNAG-9\tZDB-PUB-5\tJournal\t3",  # non-gene entity -> yielded
            "y\tZDB-GENE-3\tZDB-ALT-9\tJournal\t2",      # reference not a pub -> skipped
            "justonecolumn",                              # too few fields -> skipped
        ])
        assert list(mod.parse_gene_publication(path)) == [
            ("ZDB-LINCRNAG-9", "ZDB-PUB-5", "3"),
        ]


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
            "duplicate_in_file": 1, "skipped_non_gene": 0, "missing_reference": 1,
            "not_in_corpus": 0, "errors": 0,
        }
        counts.update(overrides)
        return counts

    def test_download_failed_message(self):
        assert "Failed to download" in mod.compose_report_message({"download_failed": True})

    def test_includes_counts(self):
        msg = mod.compose_report_message(self._counts(skipped_non_gene=4))
        assert "Entity tags created: 6" in msg
        assert "Total gene-reference pairs in file: 10" in msg
        assert "Non-gene entity rows skipped: 4" in msg

    def test_flags_abort(self):
        msg = mod.compose_report_message(self._counts(aborted=True))
        assert "RUN ABORTED" in msg


class TestLoadLoop:
    """Drive the main loop with the DB/session and create_tag mocked."""

    @patch.object(mod, "write_id_log")
    @patch.object(mod, "load_existing_entity_pairs", return_value=set())
    @patch.object(mod, "build_zfin_corpus_ref_curies", return_value={"AGRKB:1", "AGRKB:2"})
    @patch.object(mod, "build_zfin_pub_to_ref_curie", return_value={
        "ZFIN:ZDB-PUB-1": "AGRKB:1",  # in corpus
        "ZFIN:ZDB-PUB-2": "AGRKB:2",  # in corpus
        "ZFIN:ZDB-PUB-3": "AGRKB:3",  # resolves but NOT in corpus
    })
    @patch.object(mod, "get_or_create_source", return_value=229)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    @patch.object(mod, "create_tag")
    def test_counts_each_branch(self, mock_create_tag, mock_session, *_mocks):
        mock_session.return_value = MagicMock()
        mock_create_tag.side_effect = [
            (1, False),                                   # GENE-1/PUB-1 -> created
            HTTPException(status_code=409, detail="dup"),  # GENE-3/PUB-2 -> skipped
        ]
        rows = [
            ("ZDB-GENE-1", "ZDB-PUB-1", ""),   # created
            ("ZDB-GENE-1", "ZDB-PUB-1", ""),   # duplicate within file
            ("ZDB-LINCRNAG-9", "ZDB-PUB-1", ""),  # non-gene entity
            ("ZDB-GENE-2", "ZDB-PUB-3", ""),   # resolves but not in corpus
            ("ZDB-GENE-3", "ZDB-PUB-2", ""),   # create_tag 409 -> skipped_duplicate
        ]
        with patch.object(mod, "parse_gene_publication", return_value=iter(rows)):
            counts = mod.load_zfin_gene_reference_tags(input_file="ignored.txt")

        assert counts["total_pairs"] == 5
        assert counts["created"] == 1
        assert counts["duplicate_in_file"] == 1
        assert counts["skipped_non_gene"] == 1
        assert counts["not_in_corpus"] == 1
        assert counts["skipped_duplicate"] == 1
        assert counts["missing_reference"] == 0
        assert counts["errors"] == 0
        assert counts["not_in_corpus_refs"] == {"AGRKB:3": "ZFIN:ZDB-PUB-3"}
        assert mock_create_tag.call_count == 2

    @patch.object(mod, "write_id_log")
    @patch.object(mod, "load_existing_entity_pairs",
                  return_value={("AGRKB:1", "ZFIN:ZDB-GENE-1")})
    @patch.object(mod, "build_zfin_corpus_ref_curies", return_value={"AGRKB:1"})
    @patch.object(mod, "build_zfin_pub_to_ref_curie", return_value={"ZFIN:ZDB-PUB-1": "AGRKB:1"})
    @patch.object(mod, "get_or_create_source", return_value=229)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    @patch.object(mod, "create_tag")
    def test_already_loaded_pair_skipped_without_create_tag(
            self, mock_create_tag, mock_session, *_mocks):
        mock_session.return_value = MagicMock()
        rows = [("ZDB-GENE-1", "ZDB-PUB-1", "")]
        with patch.object(mod, "parse_gene_publication", return_value=iter(rows)):
            counts = mod.load_zfin_gene_reference_tags(input_file="ignored.txt")
        assert counts["skipped_duplicate"] == 1
        assert counts["created"] == 0
        mock_create_tag.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
