"""Tests for load_zfin_allele_reference_tags.py"""

import json

import pytest

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
            "reference_curies": ["ZFIN:ZDB-PUB-1", "ZFIN:ZDB-PUB-2"],
        }]})
        rows = list(mod.parse_allele_records(path))
        assert rows == [(
            "ZFIN:ZDB-ALT-000209-24", "NCBITaxon:7955",
            ["ZFIN:ZDB-PUB-1", "ZFIN:ZDB-PUB-2"],
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
        assert data["negated"] is False
        assert data["topic_entity_tag_source_id"] == 229
        assert data.get("created_by") is None


class TestComposeReportMessage:

    def _counts(self, **overrides):
        counts = {
            "total_alleles": 4, "total_pairs": 10, "created": 6,
            "skipped_duplicate": 2, "duplicate_in_file": 1, "missing_reference": 1,
            "not_in_corpus": 0, "errors": 0,
        }
        counts.update(overrides)
        return counts

    def test_download_failed_message(self):
        msg = mod.compose_report_message({"download_failed": True})
        assert "Failed to download" in msg

    def test_includes_counts(self):
        msg = mod.compose_report_message(self._counts())
        assert "Alleles in file: 4" in msg
        assert "Entity tags created: 6" in msg
        assert "Total allele-reference pairs in file: 10" in msg

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
