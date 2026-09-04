"""Tests for load_zfin_allele_reference_tags.py"""

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from agr_literature_service.lit_processing.data_ingest.for_migration import (
    load_zfin_allele_reference_tags as mod,
)


class TestExtractIngestRecords:

    def test_bare_list(self):
        records = [{"primary_external_id": "ZFIN:ZDB-ALT-1"}]
        assert mod._extract_ingest_records(records) == records

    def test_allele_ingest_set_wrapper(self):
        records = [{"primary_external_id": "ZFIN:ZDB-ALT-1"}]
        assert mod._extract_ingest_records({"allele_ingest_set": records}) == records

    def test_data_wrapper(self):
        records = [{"primary_external_id": "ZFIN:ZDB-ALT-1"}]
        assert mod._extract_ingest_records({"data": records}) == records

    def test_falls_back_to_first_list_value(self):
        records = [{"primary_external_id": "ZFIN:ZDB-ALT-1"}]
        assert mod._extract_ingest_records({"metadata": {}, "weird_key": records}) == records

    def test_empty_or_non_container(self):
        assert mod._extract_ingest_records({}) == []
        assert mod._extract_ingest_records(None) == []


class TestParseAlleleRecords:

    def _write(self, tmp_path, data):
        f = tmp_path / "allele.json"
        f.write_text(json.dumps(data))
        return str(f)

    def test_parses_valid_record(self, tmp_path):
        path = self._write(tmp_path, {"allele_ingest_set": [{
            "internal": False,
            "obsolete": False,
            "primary_external_id": "ZFIN:ZDB-ALT-000209-24",
            "taxon_curie": "NCBITaxon:7955",
            "reference_curies": ["ZFIN:ZDB-PUB-1", "PMID:123"],
        }]})
        rows = list(mod.parse_allele_records(path))
        assert rows == [(
            "ZFIN:ZDB-ALT-000209-24", "NCBITaxon:7955",
            ["ZFIN:ZDB-PUB-1", "PMID:123"],
        )]

    def test_skips_internal_and_obsolete(self, tmp_path):
        path = self._write(tmp_path, {"allele_ingest_set": [
            {"internal": True, "primary_external_id": "ZFIN:ZDB-ALT-1"},
            {"obsolete": True, "primary_external_id": "ZFIN:ZDB-ALT-2"},
        ]})
        assert list(mod.parse_allele_records(path)) == []

    def test_skips_record_without_primary_external_id(self, tmp_path):
        path = self._write(tmp_path, {"allele_ingest_set": [
            {"internal": False, "obsolete": False, "reference_curies": ["ZFIN:ZDB-PUB-1"]},
        ]})
        assert list(mod.parse_allele_records(path)) == []

    def test_defaults_taxon_and_references(self, tmp_path):
        path = self._write(tmp_path, {"allele_ingest_set": [
            {"primary_external_id": "ZFIN:ZDB-ALT-3"},
        ]})
        rows = list(mod.parse_allele_records(path))
        assert rows == [("ZFIN:ZDB-ALT-3", mod.DANIO_RERIO_TAXON, [])]


class TestBuildTagPayload:

    def test_pure_entity_allele_tag_fields(self):
        payload = mod._build_tag_payload(
            "AGRKB:1", "ZFIN:ZDB-ALT-1", "NCBITaxon:7955", source_id=229)
        data = payload.dict()
        assert data["reference_curie"] == "AGRKB:1"
        assert data["topic"] == mod.ALLELE_ATP
        assert data["entity_type"] == mod.ALLELE_ATP
        assert data["entity"] == "ZFIN:ZDB-ALT-1"
        assert data["entity_id_validation"] == mod.ENTITY_ID_VALIDATION
        assert data["species"] == "NCBITaxon:7955"
        assert data["data_novelty"] == mod.EXISTING_DATA_NOVELTY_ATP
        assert data["data_context"] == mod.EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP
        assert data["negated"] is False
        assert data["topic_entity_tag_source_id"] == 229
        assert data.get("created_by") is None


class TestComposeReportMessage:

    def _counts(self, **overrides):
        counts = {
            "total_alleles": 4, "total_pairs": 10, "created": 6,
            "skipped_duplicate": 2, "duplicate_in_file": 1, "missing_reference": 1,
            "not_in_corpus": 0, "skipped_over_cap": 0, "papers_over_cap": 0,
            "errors": 0,
        }
        counts.update(overrides)
        return counts

    def test_download_failed_message(self):
        assert "Failed to download" in mod.compose_report_message({"download_failed": True})

    def test_includes_counts(self):
        msg = mod.compose_report_message(self._counts())
        assert "Alleles in file: 4" in msg
        assert "Entity tags created: 6" in msg
        assert "Total allele-reference pairs in file: 10" in msg

    def test_flags_abort(self):
        assert "RUN ABORTED" in mod.compose_report_message(self._counts(aborted=True))


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
            (1, False),                                    # ALT-1/PUB-1 -> created
            HTTPException(status_code=409, detail="dup"),  # ALT-4/PUB-2 -> skipped
        ]
        records = [
            ("ZFIN:ZDB-ALT-1", "NCBITaxon:7955", ["ZFIN:ZDB-PUB-1", "ZFIN:ZDB-PUB-1"]),
            ("ZFIN:ZDB-ALT-2", "NCBITaxon:7955", ["ZFIN:ZDB-PUB-3"]),   # not in corpus
            ("ZFIN:ZDB-ALT-3", "NCBITaxon:7955", ["ZFIN:ZDB-PUB-X"]),   # missing ref
            ("ZFIN:ZDB-ALT-4", "NCBITaxon:7955", ["ZFIN:ZDB-PUB-2"]),   # 409 -> skipped
        ]
        # side_effect (not return_value) so both the counting pass and the main
        # loop each get a fresh iterator over the records.
        with patch.object(mod, "parse_allele_records", side_effect=lambda *a, **k: iter(records)):
            counts = mod.load_zfin_allele_reference_tags(input_file="ignored.json")

        assert counts["total_alleles"] == 4
        assert counts["total_pairs"] == 5
        assert counts["created"] == 1
        assert counts["duplicate_in_file"] == 1
        assert counts["not_in_corpus"] == 1
        assert counts["missing_reference"] == 1
        assert counts["skipped_duplicate"] == 1
        assert counts["errors"] == 0
        assert counts["not_in_corpus_refs"] == {"AGRKB:3": "ZFIN:ZDB-PUB-3"}
        assert mock_create_tag.call_count == 2

    @patch("agr_literature_service.lit_processing.data_ingest.for_migration."
           "zfin_reference_tag_utils.MAX_ASSOCIATIONS_PER_PAPER", 2)
    @patch.object(mod, "write_id_log")
    @patch.object(mod, "load_existing_entity_pairs", return_value=set())
    @patch.object(mod, "build_zfin_corpus_ref_curies", return_value={"AGRKB:1", "AGRKB:2"})
    @patch.object(mod, "build_zfin_pub_to_ref_curie", return_value={
        "ZFIN:ZDB-PUB-1": "AGRKB:1",
        "ZFIN:ZDB-PUB-2": "AGRKB:2",
    })
    @patch.object(mod, "get_or_create_source", return_value=229)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    @patch.object(mod, "create_tag")
    def test_skips_papers_over_association_cap(self, mock_create_tag, mock_session, *_mocks):
        mock_session.return_value = MagicMock()
        mock_create_tag.return_value = (1, False)
        # PUB-1 is referenced by 3 distinct alleles (over the patched cap of 2) so
        # none of its tags load; PUB-2 is referenced by 1 and loads normally.
        records = [
            ("ZFIN:ZDB-ALT-1", "NCBITaxon:7955", ["ZFIN:ZDB-PUB-1"]),
            ("ZFIN:ZDB-ALT-2", "NCBITaxon:7955", ["ZFIN:ZDB-PUB-1"]),
            ("ZFIN:ZDB-ALT-3", "NCBITaxon:7955", ["ZFIN:ZDB-PUB-1", "ZFIN:ZDB-PUB-2"]),
        ]
        with patch.object(mod, "parse_allele_records", side_effect=lambda *a, **k: iter(records)):
            counts = mod.load_zfin_allele_reference_tags(input_file="ignored.json")
        assert counts["papers_over_cap"] == 1
        assert counts["skipped_over_cap"] == 3
        assert counts["created"] == 1
        mock_create_tag.assert_called_once()

    @patch.object(mod, "write_id_log")
    @patch.object(mod, "load_existing_entity_pairs", return_value=set())
    @patch.object(mod, "build_zfin_corpus_ref_curies", return_value={"AGRKB:1"})
    @patch.object(mod, "build_zfin_pub_to_ref_curie", return_value={"ZFIN:ZDB-PUB-1": "AGRKB:1"})
    @patch.object(mod, "get_or_create_source", return_value=229)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    @patch.object(mod, "create_tag", side_effect=RuntimeError("boom"))
    def test_aborts_after_consecutive_errors(self, mock_create_tag, mock_session, *_mocks):
        db = MagicMock()
        mock_session.return_value = db
        # Distinct alleles (one shared reference) so every pair reaches create_tag.
        records = [(f"ZFIN:ZDB-ALT-{i}", "NCBITaxon:7955", ["ZFIN:ZDB-PUB-1"])
                   for i in range(40)]
        with patch.object(mod, "parse_allele_records", side_effect=lambda *a, **k: iter(records)):
            counts = mod.load_zfin_allele_reference_tags(input_file="ignored.json")
        assert counts.get("aborted") is True
        assert counts["errors"] == mod.ABORT_AFTER_CONSECUTIVE_ERRORS
        assert db.rollback.call_count == mod.ABORT_AFTER_CONSECUTIVE_ERRORS
        assert counts["total_pairs"] == mod.ABORT_AFTER_CONSECUTIVE_ERRORS


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
