"""Tests for update_sgd_entity_reference_tags.py"""

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest
import requests

from agr_literature_service.lit_processing.data_ingest.for_migration import (
    update_sgd_entity_reference_tags as mod,
)
from agr_literature_service.lit_processing.data_ingest.for_migration import (
    sgd_reference_tag_utils as util,
)


class TestFetchReferencesWithEntities:

    @patch.object(mod.requests, "get")
    def test_returns_references_list(self, mock_get):
        response = MagicMock()
        response.json.return_value = {"references": [{"sgdid": "S1"}]}
        mock_get.return_value = response
        assert mod.fetch_references_with_entities(30) == [{"sgdid": "S1"}]

    @patch.object(mod.requests, "get")
    def test_non_dict_payload_returns_none(self, mock_get):
        # a bare list (or error page decoded as JSON) must not escape as an
        # AttributeError further down
        response = MagicMock()
        response.json.return_value = ["not", "a", "dict"]
        mock_get.return_value = response
        assert mod.fetch_references_with_entities(30) is None

    def test_request_failure_returns_none(self):
        with patch.object(mod.requests, "get",
                          side_effect=requests.RequestException("down")):
            assert mod.fetch_references_with_entities(30) is None


def _updater_patches(**overrides):
    ref_map = overrides.get("ref_map", {"SGD:S1": "AGRKB:1"})
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


def _run_update(references, create_tag_mock, **overrides):
    with ExitStack() as stack:
        for p in _updater_patches(**overrides):
            stack.enter_context(p)
        stack.enter_context(patch.object(util, "create_tag", create_tag_mock))
        stack.enter_context(patch.object(mod, "fetch_references_with_entities",
                                         return_value=references))
        return mod.update_sgd_entity_reference_tags(days_added=30)


class TestUpdateLoop:

    def test_topics_list_creates_topic_only_tags(self):
        create_tag = MagicMock(return_value=(1, False))
        references = [{
            "sgdid": "S1", "date_created": "2026-07-08", "created_by": "STACIA",
            "entities": [],
            "topics": [{"topic": "Reviews", "date_created": "2026-07-09 09:20:11",
                        "created_by": "NASH"}],
        }]
        counts = _run_update(references, create_tag)
        assert counts["total_associations"] == 1
        assert counts["created"] == 1
        payload = create_tag.call_args.args[1]
        assert payload.topic == mod.ROOT_TOPIC_ATP
        assert payload.entity is None
        assert payload.display_tag == "ATP:0000130"
        # per-annotation curator, not the reference-level one
        assert payload.created_by == "NASH"

    def test_missing_topics_field_from_old_backend_is_graceful(self):
        create_tag = MagicMock(return_value=(1, False))
        references = [{
            "sgdid": "S1", "date_created": "2026-07-08", "created_by": "STACIA",
            "entities": [{"entity_type": "gene", "entity_sgdid": "S2",
                          "topic": "Primary Literature"}],
        }]
        counts = _run_update(references, create_tag)
        assert counts["created"] == 1
        assert counts["unknown_topic"] == 0

    def test_unknown_topic_only_topic_is_skipped_and_counted(self):
        create_tag = MagicMock(return_value=(1, False))
        references = [{
            "sgdid": "S1", "entities": [],
            "topics": [{"topic": "Bogus Section"}],
        }]
        counts = _run_update(references, create_tag)
        assert counts["unknown_topic"] == 1
        create_tag.assert_not_called()

    def test_unknown_entity_topic_is_counted_but_loaded(self):
        create_tag = MagicMock(return_value=(1, False))
        references = [{
            "sgdid": "S1",
            "entities": [{"entity_type": "gene", "entity_sgdid": "S2",
                          "topic": "Bogus Section"}],
        }]
        counts = _run_update(references, create_tag)
        assert counts["entity_unknown_topic"] == 1
        assert counts["created"] == 1
        assert create_tag.call_args.args[1].display_tag is None

    def test_entity_falls_back_to_reference_level_curator_and_date(self):
        create_tag = MagicMock(return_value=(1, False))
        references = [{
            "sgdid": "S1", "date_created": "2026-07-08", "created_by": "STACIA",
            "entities": [{"entity_type": "gene", "entity_sgdid": "S2"}],
        }]
        _run_update(references, create_tag)
        payload = create_tag.call_args.args[1]
        assert payload.created_by == "STACIA"
        assert payload.date_created is not None

    def test_reference_gating_counts_topics_too(self):
        create_tag = MagicMock(return_value=(1, False))
        references = [{
            "sgdid": "S9",  # not in the ABC
            "entities": [{"entity_type": "gene", "entity_sgdid": "S2"}],
            "topics": [{"topic": "Reviews"}],
        }]
        counts = _run_update(references, create_tag)
        assert counts["missing_reference"] == 2
        create_tag.assert_not_called()

    def test_fetch_failure_reported(self):
        with patch.object(mod, "fetch_references_with_entities", return_value=None):
            counts = mod.update_sgd_entity_reference_tags(days_added=30)
        assert counts.get("fetch_failed") is True
        assert "Failed to fetch" in mod.compose_report_message(counts, 30)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
