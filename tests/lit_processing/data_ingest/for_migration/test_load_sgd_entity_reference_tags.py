"""Tests for load_sgd_entity_reference_tags.py"""

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

from agr_literature_service.lit_processing.data_ingest.for_migration import (
    load_sgd_entity_reference_tags as mod,
)
from agr_literature_service.lit_processing.data_ingest.for_migration import (
    sgd_reference_tag_utils as util,
)

HEADER = "reference_sgdid\tentity_type\tentity_name\tentity_sgdid\tdate_created\tcreated_by\ttopic"


class TestParseReferencesWithEntities:

    def _write(self, tmp_path, lines):
        f = tmp_path / "references_with_entities.tsv"
        f.write_text("\n".join(lines) + "\n")
        return str(f)

    def test_parses_entity_and_topic_only_rows(self, tmp_path):
        path = self._write(tmp_path, [
            HEADER,
            "S000039113\tgene\tACT1\tS000002284\t2021-11-01 10:00:30\tEDITH\tPrimary Literature",
            "S000039404\t\t\t\t2013-01-28 00:00:00\tOTTO\tOmics",
        ])
        assert list(mod.parse_references_with_entities(path)) == [
            ("S000039113", "gene", "S000002284", "2021-11-01 10:00:30", "EDITH",
             "Primary Literature"),
            ("S000039404", "", "", "2013-01-28 00:00:00", "OTTO", "Omics"),
        ]

    def test_older_dumps_degrade_to_empty_strings(self, tmp_path):
        path = self._write(tmp_path, [
            "S1\tgene\tACT1\tS2",                       # 4-col dump
            "S1\tgene\tACT1\tS2\t2021-01-01",           # 5-col dump
            "S1\tgene\tACT1\tS2\t2021-01-01\tOTTO",     # 6-col dump
        ])
        assert list(mod.parse_references_with_entities(path)) == [
            ("S1", "gene", "S2", "", "", ""),
            ("S1", "gene", "S2", "2021-01-01", "", ""),
            ("S1", "gene", "S2", "2021-01-01", "OTTO", ""),
        ]

    def test_skips_header_and_malformed_rows(self, tmp_path):
        path = self._write(tmp_path, [HEADER, "toofew\tcols"])
        assert list(mod.parse_references_with_entities(path)) == []


class TestCountAssociationsPerPaper:

    def test_ignores_topic_only_rows(self, tmp_path):
        f = tmp_path / "dump.tsv"
        f.write_text(
            "S1\tgene\tACT1\tS2\t\t\tReviews\n"
            "S1\t\t\t\t\tOTTO\tReviews\n"      # topic-only: no cap counting
        )
        result = mod.count_associations_per_paper(str(f))
        assert dict(result) == {("SGD:S1", "gene"): {"S2"}}


def _loader_patches(**overrides):
    """The standard patch set for driving the load loop without a DB."""
    ref_map = overrides.get("ref_map", {"SGD:S1": "AGRKB:1", "SGD:S3": "AGRKB:3"})
    corpus = overrides.get("corpus", {"AGRKB:1"})
    existing = overrides.get("existing", {})
    abc = overrides.get("abc", set())
    return [
        patch.object(mod, "write_id_log"),
        patch.object(mod, "load_abc_entity_tags", return_value=abc),
        patch.object(mod, "load_existing_entity_tags", return_value=existing),
        patch.object(mod, "build_sgd_corpus_ref_curies", return_value=corpus),
        patch.object(mod, "build_sgd_ref_curie_map", return_value=ref_map),
        patch.object(mod, "get_or_create_source", return_value=230),
        patch.object(mod, "set_global_user_id"),
        patch.object(mod, "create_postgres_session", return_value=MagicMock()),
        patch.object(mod, "resolve_sgd_created_by",
                     side_effect=lambda db, sgd_id, cache: sgd_id or None),
    ]


def _run_load(rows, create_tag_mock, **overrides):
    with ExitStack() as stack:
        for p in _loader_patches(**overrides):
            stack.enter_context(p)
        stack.enter_context(patch.object(util, "create_tag", create_tag_mock))
        stack.enter_context(patch.object(mod, "parse_references_with_entities",
                                         side_effect=lambda *a, **k: iter(rows)))
        return mod.load_sgd_entity_reference_tags(input_file="ignored.tsv")


class TestLoadLoop:

    def test_topic_only_and_entity_rows_create_tags(self):
        create_tag = MagicMock(return_value=(1, False))
        rows = [
            # entity row -> pure entity tag
            ("S1", "gene", "S2", "2021-11-01 10:00:30", "EDITH", "Reviews"),
            # topic-only row -> root-topic tag identified by display_tag
            ("S1", "", "", "2013-01-28 00:00:00", "OTTO", "Omics"),
        ]
        counts = _run_load(rows, create_tag)
        assert counts["created"] == 2
        assert counts["errors"] == 0
        payloads = [call.args[1] for call in create_tag.call_args_list]
        entity_payload, topic_only_payload = payloads
        assert entity_payload.topic == "ATP:0000005"
        assert entity_payload.display_tag == "ATP:0000130"
        assert topic_only_payload.topic == mod.ROOT_TOPIC_ATP
        assert topic_only_payload.entity is None
        assert topic_only_payload.display_tag == "ATP:0000148"

    def test_topic_only_row_with_unknown_topic_is_skipped(self):
        create_tag = MagicMock(return_value=(1, False))
        rows = [("S1", "", "", "", "OTTO", "Renamed Section")]
        counts = _run_load(rows, create_tag)
        assert counts["unknown_topic"] == 1
        assert counts["created"] == 0
        create_tag.assert_not_called()

    def test_entity_row_with_unknown_topic_is_counted_but_loaded(self):
        create_tag = MagicMock(return_value=(1, False))
        rows = [("S1", "gene", "S2", "", "", "Renamed Section")]
        counts = _run_load(rows, create_tag)
        assert counts["entity_unknown_topic"] == 1
        assert counts["created"] == 1
        # loaded without a display_tag: create_tag's topic-ATP stamping applies
        assert create_tag.call_args.args[1].display_tag is None

    def test_entity_row_with_empty_topic_is_not_counted(self):
        # a pre-topic dump has an empty column; that is expected degradation
        create_tag = MagicMock(return_value=(1, False))
        rows = [("S1", "gene", "S2", "", "", "")]
        counts = _run_load(rows, create_tag)
        assert counts["entity_unknown_topic"] == 0
        assert counts["created"] == 1

    def test_reference_and_corpus_gating(self):
        create_tag = MagicMock(return_value=(1, False))
        rows = [
            ("S9", "", "", "", "", "Reviews"),   # unknown reference
            ("S3", "", "", "", "", "Reviews"),   # resolves but not in corpus
        ]
        counts = _run_load(rows, create_tag)
        assert counts["missing_reference"] == 1
        assert counts["not_in_corpus"] == 1
        create_tag.assert_not_called()

    def test_topic_only_row_skipped_when_curated_in_abc(self):
        create_tag = MagicMock(return_value=(1, False))
        rows = [("S1", "", "", "", "", "Reviews")]
        counts = _run_load(rows, create_tag,
                           abc={("AGRKB:1", mod.ROOT_TOPIC_ATP, "ATP:0000130")})
        assert counts["skipped_in_abc"] == 1
        create_tag.assert_not_called()


class TestComposeReportMessage:

    def test_includes_new_counts(self):
        counts = mod.new_counts()
        counts.update(total_associations=10, created=5, unknown_topic=2,
                      entity_unknown_topic=3)
        msg = mod.compose_report_message(counts, "dump.tsv")
        assert "Total associations (entity and topic-only) in file: 10" in msg
        assert "Topic-only annotations with an unknown SGD topic skipped: 2" in msg
        assert "unmappable SGD topic" in msg

    def test_flags_abort(self):
        counts = mod.new_counts()
        counts["aborted"] = True
        assert "RUN ABORTED" in mod.compose_report_message(counts, "dump.tsv")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
