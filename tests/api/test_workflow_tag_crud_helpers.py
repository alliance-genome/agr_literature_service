"""
Unit tests for pure / near-pure helpers in workflow_tag_crud.

These avoid the database and A-Team network by exercising the functions
directly (mocking the ontology lookups where needed).
"""
import pytest
from unittest.mock import patch
from fastapi import HTTPException

from agr_literature_service.api.crud import workflow_tag_crud as wtc


@pytest.fixture
def clean_atp_state(monkeypatch):
    """Snapshot and reset the module-level ATP caches, and stub the loader."""
    orig_cache = dict(wtc._atp_id_cache)
    orig_name = dict(wtc.name_to_atp)
    orig_pre_status = wtc._pre_curation_status_cache
    orig_pre_proc = wtc._pre_curation_process_cache

    wtc._atp_id_cache.clear()
    wtc.name_to_atp.clear()
    wtc._pre_curation_status_cache = None
    wtc._pre_curation_process_cache = None
    monkeypatch.setattr(wtc, "_ensure_atp_loaded", lambda: None)

    yield

    wtc._atp_id_cache.clear()
    wtc._atp_id_cache.update(orig_cache)
    wtc.name_to_atp.clear()
    wtc.name_to_atp.update(orig_name)
    wtc._pre_curation_status_cache = orig_pre_status
    wtc._pre_curation_process_cache = orig_pre_proc


class TestGetFieldAndStatus:

    @pytest.mark.parametrize("atp,expected", [
        ("file needed", ("file", "needed")),
        ("reference classification complete", ("reference classification", "complete")),
        ("text conversion failed", ("text conversion", "failed")),
        ("reference classification in progress", ("reference classification", "in progress")),
    ])
    def test_valid_statuses(self, atp, expected):
        assert wtc.get_field_and_status(atp) == expected

    def test_unapproved_status_raises_422(self):
        with pytest.raises(HTTPException) as exc:
            wtc.get_field_and_status("something unexpected")
        assert exc.value.status_code == 422


class TestBuildProcessInfo:

    def test_process_only_no_subprocess(self):
        info = wtc._build_process_info(
            "ATP:tag", {"ATP:tag": "ATP:proc"}, {}, {"ATP:proc": "Proc"})
        assert info == {"workflow_process": "ATP:proc", "workflow_process_name": "Proc"}

    def test_process_and_distinct_subprocess(self):
        info = wtc._build_process_info(
            "ATP:tag", {"ATP:tag": "ATP:proc"}, {"ATP:tag": "ATP:sub"},
            {"ATP:proc": "Proc", "ATP:sub": "Sub"})
        assert info["workflow_subprocess"] == "ATP:sub"
        assert info["workflow_subprocess_name"] == "Sub"

    def test_subprocess_equal_to_process_is_dropped(self):
        info = wtc._build_process_info(
            "ATP:tag", {"ATP:tag": "ATP:proc"}, {"ATP:tag": "ATP:proc"},
            {"ATP:proc": "Proc"})
        assert "workflow_subprocess" not in info

    def test_no_process_gives_none(self):
        info = wtc._build_process_info("ATP:tag", {}, {}, {})
        assert info == {"workflow_process": None, "workflow_process_name": None}


class TestResolveProcessHierarchy:

    def test_resolves_process_and_subprocess_and_fetches_names(self):
        atp_curie_to_name = {}
        ancestors = {"ATP:tagA": ["ATP:sub", "ATP:proc", "ATP:0000177"]}
        with patch.object(wtc, "get_workflow_process_from_tag",
                          side_effect=lambda t: ancestors.get(t)), \
                patch.object(wtc, "get_map_ateam_curies_to_names",
                             return_value={"ATP:proc": "Proc", "ATP:sub": "Sub"}) as get_names:
            process_cache, subprocess_cache = wtc._resolve_process_hierarchy(
                ["ATP:tagA"], atp_curie_to_name)

        assert process_cache == {"ATP:tagA": "ATP:proc"}
        assert subprocess_cache == {"ATP:tagA": "ATP:sub"}
        get_names.assert_called_once()
        assert atp_curie_to_name["ATP:proc"] == "Proc"

    def test_root_at_index_zero_uses_tag_as_process(self):
        with patch.object(wtc, "get_workflow_process_from_tag",
                          return_value=["ATP:0000335"]), \
                patch.object(wtc, "get_map_ateam_curies_to_names", return_value={}):
            process_cache, subprocess_cache = wtc._resolve_process_hierarchy(
                ["ATP:tagB"], {"ATP:tagB": "Tag B"})
        assert process_cache == {"ATP:tagB": "ATP:tagB"}
        assert subprocess_cache == {}

    def test_tag_without_ancestors_is_skipped(self):
        with patch.object(wtc, "get_workflow_process_from_tag", return_value=None), \
                patch.object(wtc, "get_map_ateam_curies_to_names", return_value={}):
            process_cache, subprocess_cache = wtc._resolve_process_hierarchy(
                ["ATP:tagC"], {})
        assert process_cache == {}
        assert subprocess_cache == {}


class TestGetAtpIdByName:

    def test_cache_hit(self, clean_atp_state):
        wtc._atp_id_cache["some name"] = "ATP:1234567"
        assert wtc.get_atp_id_by_name("some name") == "ATP:1234567"

    def test_resolved_from_name_to_atp_and_cached(self, clean_atp_state):
        wtc.name_to_atp["file needed"] = "ATP:0000141"
        assert wtc.get_atp_id_by_name("file needed") == "ATP:0000141"
        # cached for next time
        assert wtc._atp_id_cache["file needed"] == "ATP:0000141"

    def test_fallback_used_when_missing(self, clean_atp_state):
        assert wtc.get_atp_id_by_name("unknown name", fallback="ATP:0000999") == "ATP:0000999"

    def test_raises_when_missing_and_no_fallback(self, clean_atp_state):
        with pytest.raises(ValueError):
            wtc.get_atp_id_by_name("totally unknown name")


class TestGetProcessStatus:

    def test_priority_order_complete_wins(self):
        mapping = {"complete": ["ATP:c"], "needed": ["ATP:n"]}
        assert wtc._get_process_status(["ATP:c", "ATP:n"], mapping) == ("complete", "ATP:c")

    def test_falls_through_to_needed(self):
        mapping = {"complete": ["ATP:c"], "needed": ["ATP:n"]}
        assert wtc._get_process_status(["ATP:n"], mapping) == ("needed", "ATP:n")

    def test_no_match_returns_none_none(self):
        assert wtc._get_process_status(["ATP:x"], {"complete": ["ATP:c"]}) == (None, None)


class TestPreCurationMappings:

    def test_status_mapping_structure_and_cache(self, clean_atp_state):
        mapping = wtc._get_pre_curation_status_atp_mapping()
        assert set(mapping.keys()) >= {
            "file_upload", "text_conversion", "email_extraction",
            "topic_classification", "entity_extraction", "curation_classification"}
        assert mapping["file_upload"]["needed"] == ["ATP:0000141"]
        # second call returns the cached object
        assert wtc._get_pre_curation_status_atp_mapping() is mapping

    def test_process_ids_structure_and_cache(self, clean_atp_state):
        procs = wtc._get_pre_curation_process_atp_ids()
        assert procs["file_upload"] == "ATP:0000140"
        assert procs["curation_classification"] == "ATP:0000311"
        assert wtc._get_pre_curation_process_atp_ids() is procs


class TestModHasWorkflowData:

    def test_true_when_inside_corpus_present(self, clean_atp_state):
        assert wtc._mod_has_workflow_data({"inside_corpus": True}) is True

    def test_true_when_a_process_has_status(self, clean_atp_state):
        mod_data = {"inside_corpus": None, "file_upload": {"status": "complete"}}
        assert wtc._mod_has_workflow_data(mod_data) is True

    def test_false_when_no_data(self, clean_atp_state):
        mod_data = {"inside_corpus": None, "file_upload": {"status": None}}
        assert wtc._mod_has_workflow_data(mod_data) is False
