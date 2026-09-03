"""Unit tests for load_mgi_allele_reference_tags (SCRUM-6495). Same layout as
test_load_zfin_allele_reference_tags: the parse/payload/report helpers are pure,
and the main loop is driven with the DB/session and create_tag mocked."""
import gzip
import json
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from agr_literature_service.lit_processing.data_ingest.for_migration import (
    load_mgi_allele_reference_tags as mod,
)


class TestExtractIngestRecords:
    def test_bare_list(self):
        assert mod._extract_ingest_records([{"a": 1}]) == [{"a": 1}]

    def test_allele_ingest_set_wrapper(self):
        data = {"linkml_version": "2.13.0", "allele_ingest_set": [{"a": 1}]}
        assert mod._extract_ingest_records(data) == [{"a": 1}]

    def test_falls_back_to_first_list_value(self):
        data = {"something_else_set": [{"a": 1}]}
        assert mod._extract_ingest_records(data) == [{"a": 1}]

    def test_empty_or_non_container(self):
        assert mod._extract_ingest_records({}) == []
        assert mod._extract_ingest_records(None) == []


class TestParseAlleleRecords:
    def _write(self, tmp_path, records, gz=False):
        payload = {"allele_ingest_set": records}
        if gz:
            fn = tmp_path / "alleles.json.gz"
            with gzip.open(fn, "wt") as f:
                json.dump(payload, f)
        else:
            fn = tmp_path / "alleles.json"
            fn.write_text(json.dumps(payload))
        return str(fn)

    def test_parses_valid_record(self, tmp_path):
        fn = self._write(tmp_path, [{
            "primary_external_id": "MGI:1855930",
            "taxon_curie": "NCBITaxon:10090",
            "reference_curies": ["PMID:123", "MGI:6414854"],
        }])
        assert list(mod.parse_allele_records(fn)) == [
            ("MGI:1855930", "NCBITaxon:10090", ["PMID:123", "MGI:6414854"]),
        ]

    def test_reads_gzipped_file(self, tmp_path):
        fn = self._write(tmp_path, [{
            "primary_external_id": "MGI:1",
            "reference_curies": ["PMID:9"],
        }], gz=True)
        assert list(mod.parse_allele_records(fn)) == [
            ("MGI:1", mod.MUS_MUSCULUS_TAXON, ["PMID:9"]),
        ]

    def test_skips_internal_obsolete_and_missing_id(self, tmp_path):
        fn = self._write(tmp_path, [
            {"primary_external_id": "MGI:1", "internal": True},
            {"primary_external_id": "MGI:2", "obsolete": True},
            {"taxon_curie": "NCBITaxon:10090"},
            {"primary_external_id": "MGI:3"},
        ])
        assert list(mod.parse_allele_records(fn)) == [
            ("MGI:3", mod.MUS_MUSCULUS_TAXON, []),
        ]


class TestCountAssociationsPerPaper:
    def test_counts_by_resolved_reference(self):
        """The same paper listed under its MGI J-number by one allele and its
        PMID by another counts as ONE paper with two alleles — the raw tokens
        must not split the count under the cap."""
        records = [
            ("MGI:A1", "NCBITaxon:10090", ["MGI:J1"]),
            ("MGI:A2", "NCBITaxon:10090", ["PMID:111"]),
            ("MGI:A3", "NCBITaxon:10090", ["PMID:222", "MGI:UNKNOWN"]),
        ]
        pub_map = {"MGI:J1": "AGRKB:1", "PMID:111": "AGRKB:1", "PMID:222": "AGRKB:2"}
        with patch.object(mod, "parse_allele_records",
                          side_effect=lambda *a, **k: iter(records)):
            by_paper = mod.count_allele_associations_per_paper("ignored", pub_map)
        assert by_paper == {"AGRKB:1": {"MGI:A1", "MGI:A2"}, "AGRKB:2": {"MGI:A3"}}


class TestBuildTagPayload:
    def test_pure_entity_allele_tag_fields(self):
        payload = mod._build_tag_payload("AGRKB:1", "MGI:1855930",
                                         "NCBITaxon:10090", 42)
        assert payload.reference_curie == "AGRKB:1"
        assert payload.topic == mod.ALLELE_ATP
        assert payload.entity_type == mod.ALLELE_ATP
        assert payload.entity == "MGI:1855930"
        assert payload.species == "NCBITaxon:10090"
        assert payload.negated is False
        assert payload.topic_entity_tag_source_id == 42


class TestComposeReport:
    def test_download_failed_message(self):
        assert "Failed to discover/download" in mod.compose_report_message(
            {"download_failed": True})

    def test_includes_counts(self):
        counts = {"total_alleles": 5, "total_pairs": 9, "created": 3,
                  "skipped_duplicate": 2, "duplicate_in_file": 1,
                  "missing_reference": 1, "not_in_corpus": 1,
                  "papers_over_cap": 1, "skipped_over_cap": 4, "errors": 0}
        msg = mod.compose_report_message(counts)
        assert "Alleles in file: 5" in msg
        assert "Entity tags created: 3" in msg
        assert "RUN ABORTED" not in msg

    def test_flags_abort(self):
        counts = {"aborted": True, "total_alleles": 0, "total_pairs": 0,
                  "created": 0, "skipped_duplicate": 0, "duplicate_in_file": 0,
                  "missing_reference": 0, "not_in_corpus": 0,
                  "papers_over_cap": 0, "skipped_over_cap": 0, "errors": 25}
        assert "RUN ABORTED" in mod.compose_report_message(counts)


class TestLoadLoop:
    """Drive the main loop with the DB/session and create_tag mocked."""

    @patch.object(mod, "write_id_log")
    @patch.object(mod, "load_existing_entity_pairs", return_value=set())
    @patch.object(mod, "build_mgi_corpus_ref_curies", return_value={"AGRKB:1", "AGRKB:2"})
    @patch.object(mod, "build_pub_to_ref_curie", return_value={
        "MGI:J1": "AGRKB:1",    # in corpus
        "PMID:111": "AGRKB:1",  # same paper as MGI:J1
        "PMID:222": "AGRKB:2",  # in corpus
        "PMID:333": "AGRKB:3",  # resolves but NOT in corpus
    })
    @patch.object(mod, "get_or_create_source", return_value=88)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    @patch.object(mod, "create_tag")
    def test_counts_each_branch(self, mock_create_tag, mock_session, *_mocks):
        mock_session.return_value = MagicMock()
        mock_create_tag.side_effect = [
            (1, False),                                    # A1/AGRKB:1 -> created
            HTTPException(status_code=409, detail="dup"),  # A4/AGRKB:2 -> skipped
        ]
        records = [
            # dual-listed paper: MGI:J1 and PMID:111 both resolve to AGRKB:1, so
            # the second token is an in-file duplicate.
            ("MGI:A1", "NCBITaxon:10090", ["MGI:J1", "PMID:111"]),
            ("MGI:A2", "NCBITaxon:10090", ["PMID:333"]),   # not in corpus
            ("MGI:A3", "NCBITaxon:10090", ["PMID:999"]),   # missing ref
            ("MGI:A4", "NCBITaxon:10090", ["PMID:222"]),   # 409 -> skipped
        ]
        # side_effect (not return_value) so both the counting pass and the main
        # loop each get a fresh iterator over the records.
        with patch.object(mod, "parse_allele_records", side_effect=lambda *a, **k: iter(records)):
            counts = mod.load_mgi_allele_reference_tags(input_file="ignored.json")

        assert counts["total_alleles"] == 4
        assert counts["total_pairs"] == 5
        assert counts["created"] == 1
        assert counts["duplicate_in_file"] == 1
        assert counts["not_in_corpus"] == 1
        assert counts["missing_reference"] == 1
        assert counts["skipped_duplicate"] == 1
        assert counts["errors"] == 0
        assert counts["not_in_corpus_refs"] == {"AGRKB:3": "PMID:333"}
        assert counts["unresolved_prefixes"] == {"PMID": 1}
        assert mock_create_tag.call_count == 2

    # Patch the constant where select_over_cap_papers reads it (the zfin utils
    # module), not the loader's imported copy.
    @patch("agr_literature_service.lit_processing.data_ingest.for_migration."
           "zfin_reference_tag_utils.MAX_ASSOCIATIONS_PER_PAPER", 2)
    @patch.object(mod, "write_id_log")
    @patch.object(mod, "load_existing_entity_pairs", return_value=set())
    @patch.object(mod, "build_mgi_corpus_ref_curies", return_value={"AGRKB:1", "AGRKB:2"})
    @patch.object(mod, "build_pub_to_ref_curie", return_value={
        "MGI:J1": "AGRKB:1",
        "PMID:111": "AGRKB:1",
        "PMID:222": "AGRKB:2",
    })
    @patch.object(mod, "get_or_create_source", return_value=88)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    @patch.object(mod, "create_tag")
    def test_over_cap_counted_on_resolved_reference(self, mock_create_tag,
                                                    mock_session, *_mocks):
        """AGRKB:1 is referenced by 3 distinct alleles, split across its MGI and
        PMID curies (2 + 1 raw tokens). Counting resolved references pushes it
        over the patched cap of 2, so none of its tags load; AGRKB:2 loads."""
        mock_session.return_value = MagicMock()
        mock_create_tag.return_value = (1, False)
        records = [
            ("MGI:A1", "NCBITaxon:10090", ["MGI:J1"]),
            ("MGI:A2", "NCBITaxon:10090", ["PMID:111"]),
            ("MGI:A3", "NCBITaxon:10090", ["MGI:J1", "PMID:222"]),
        ]
        with patch.object(mod, "parse_allele_records", side_effect=lambda *a, **k: iter(records)):
            counts = mod.load_mgi_allele_reference_tags(input_file="ignored.json")
        assert counts["papers_over_cap"] == 1
        assert counts["skipped_over_cap"] == 3
        assert counts["created"] == 1
        mock_create_tag.assert_called_once()

    @patch.object(mod, "write_id_log")
    @patch.object(mod, "load_existing_entity_pairs",
                  return_value={("AGRKB:1", "MGI:A1")})
    @patch.object(mod, "build_mgi_corpus_ref_curies", return_value={"AGRKB:1"})
    @patch.object(mod, "build_pub_to_ref_curie", return_value={"PMID:111": "AGRKB:1"})
    @patch.object(mod, "get_or_create_source", return_value=88)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    @patch.object(mod, "create_tag")
    def test_existing_pairs_skip_create(self, mock_create_tag, mock_session, *_mocks):
        mock_session.return_value = MagicMock()
        records = [("MGI:A1", "NCBITaxon:10090", ["PMID:111"])]
        with patch.object(mod, "parse_allele_records", side_effect=lambda *a, **k: iter(records)):
            counts = mod.load_mgi_allele_reference_tags(input_file="ignored.json")
        assert counts["skipped_duplicate"] == 1
        assert counts["created"] == 0
        mock_create_tag.assert_not_called()

    @patch.object(mod, "write_id_log")
    @patch.object(mod, "load_existing_entity_pairs", return_value=set())
    @patch.object(mod, "build_mgi_corpus_ref_curies", return_value={"AGRKB:1"})
    @patch.object(mod, "build_pub_to_ref_curie", return_value={"PMID:111": "AGRKB:1"})
    @patch.object(mod, "get_or_create_source", return_value=88)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    @patch.object(mod, "create_tag", side_effect=RuntimeError("boom"))
    def test_aborts_after_consecutive_errors(self, mock_create_tag, mock_session, *_mocks):
        db = MagicMock()
        mock_session.return_value = db
        # Distinct alleles (one shared reference) so every pair reaches create_tag;
        # the shared paper stays under the cap because the default cap is 250.
        records = [(f"MGI:A{i}", "NCBITaxon:10090", ["PMID:111"])
                   for i in range(40)]
        with patch.object(mod, "parse_allele_records", side_effect=lambda *a, **k: iter(records)):
            counts = mod.load_mgi_allele_reference_tags(input_file="ignored.json")
        assert counts.get("aborted") is True
        assert counts["errors"] == mod.ABORT_AFTER_CONSECUTIVE_ERRORS
        assert db.rollback.call_count == mod.ABORT_AFTER_CONSECUTIVE_ERRORS
        assert counts["total_pairs"] == mod.ABORT_AFTER_CONSECUTIVE_ERRORS
