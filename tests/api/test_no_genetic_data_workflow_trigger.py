"""
SCRUM-6672: a FlyBase curator's "no genetic data" verdict (the "genetic data" topic
ATP:0000402, negated, on the curator source ATP:0000036 / abc_literature_system) moves the
paper's manual indexing status to "won't manually index" (ATP:0000343) with the curation tag
"no genetic data" (ATP:0000207).

Only curator verdicts count (not the pipeline's), only for FB, and the trigger only ever adds:
flipping the verdict back leaves the workflow tag alone. A paper already at 343 keeps any
curation tag it has, and a transition FB's table does not allow is logged, not raised, so the
verdict still saves.
"""
from unittest.mock import patch

import pytest

from agr_literature_service.api.crud import topic_entity_tag_crud as tet_crud
from agr_literature_service.api.crud.tag_source_crud import get_or_create_abc_source
from agr_literature_service.api.models import (ModModel, ReferenceModel, TagSourceModel, TopicEntityTagModel,
                                               WorkflowTagModel, WorkflowTransitionModel)
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import load_name_to_atp_and_relationships_mock
from ..fixtures import db  # noqa

ATP_MOCK = "agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships"
MANUAL_INDEXING = "ATP:0000273"
MI_NEEDED = "ATP:0000274"
MI_IN_PROGRESS = "ATP:0000276"
MI_WONT = "ATP:0000343"
FILE_NEEDED = "ATP:0000141"
GENETIC_DATA = "ATP:0000402"
NO_GENETIC_DATA = "ATP:0000207"
RETRACTED = "ATP:0000344"
REFERENCE_CURIE = "AGRKB:101000300672"


@pytest.fixture
def fb_ref(db):  # noqa
    load_name_to_atp_and_relationships_mock()
    populate_test_mods()
    ref = ReferenceModel(curie=REFERENCE_CURIE, category="research_article")
    db.add(ref)
    db.commit()
    db.refresh(ref)
    mods = {mod.abbreviation: mod for mod in db.query(ModModel).filter(ModModel.abbreviation.in_(["FB", "WB"]))}
    for mod in mods.values():
        for transition_from in (MANUAL_INDEXING, MI_IN_PROGRESS):
            db.add(WorkflowTransitionModel(mod_id=mod.mod_id, transition_from=transition_from,
                                           transition_to=MI_WONT, transition_type="any"))
    db.commit()
    return db, mods, ref


def _mi_tags(db, ref, mod):  # noqa
    """The paper's manual indexing tag(s) for the MOD, as (workflow tag, curation tag)."""
    db.expire_all()
    return sorted((tag.workflow_tag_id, tag.curation_tag) for tag in db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == ref.reference_id, WorkflowTagModel.mod_id == mod.mod_id,
        WorkflowTagModel.workflow_tag_id != FILE_NEEDED).all())


def _classifier_source(db, mod):  # noqa
    source = TagSourceModel(source_evidence_assertion="ECO:0008004", source_method="abc_document_classifier",
                            data_provider=mod.abbreviation, secondary_data_provider_id=mod.mod_id)
    db.add(source)
    db.commit()
    return source


def _tag(db, ref, source, negated=True, topic=GENETIC_DATA):  # noqa
    tag = TopicEntityTagModel(reference_id=ref.reference_id, topic=topic, tag_source_id=source.tag_source_id,
                              negated=negated, data_novelty=tet_crud.CURATOR_VALIDATION_DATA_NOVELTY,
                              data_context=tet_crud.CURATOR_VALIDATION_DATA_CONTEXT)
    db.add(tag)
    db.flush()
    db.refresh(tag, ['tag_source'])
    return tag


def _mi_tag(db, ref, mod, workflow_tag_id, curation_tag=None):  # noqa
    db.add(WorkflowTagModel(reference_id=ref.reference_id, mod_id=mod.mod_id, workflow_tag_id=workflow_tag_id,
                            curation_tag=curation_tag))
    db.commit()


@patch(ATP_MOCK, load_name_to_atp_and_relationships_mock)
class TestNoGeneticDataTrigger:

    def test_curator_verdict_sets_wont_manually_index(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        tet_crud.add_no_genetic_data_workflow_tag(db, _tag(db, ref, get_or_create_abc_source(db, "FB")))
        db.commit()
        assert _mi_tags(db, ref, mods["FB"]) == [(MI_WONT, NO_GENETIC_DATA)]

    def test_moves_the_current_manual_indexing_state(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        _mi_tag(db, ref, mods["FB"], MI_IN_PROGRESS)
        tet_crud.add_no_genetic_data_workflow_tag(db, _tag(db, ref, get_or_create_abc_source(db, "FB")))
        db.commit()
        assert _mi_tags(db, ref, mods["FB"]) == [(MI_WONT, NO_GENETIC_DATA)]

    def test_already_wont_manually_index_keeps_its_curation_tag(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        _mi_tag(db, ref, mods["FB"], MI_WONT, RETRACTED)
        tet_crud.add_no_genetic_data_workflow_tag(db, _tag(db, ref, get_or_create_abc_source(db, "FB")))
        db.commit()
        assert _mi_tags(db, ref, mods["FB"]) == [(MI_WONT, RETRACTED)]

    def test_already_wont_manually_index_without_curation_tag_gets_it(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        _mi_tag(db, ref, mods["FB"], MI_WONT)
        tet_crud.add_no_genetic_data_workflow_tag(db, _tag(db, ref, get_or_create_abc_source(db, "FB")))
        db.commit()
        assert _mi_tags(db, ref, mods["FB"]) == [(MI_WONT, NO_GENETIC_DATA)]

    def test_pipeline_verdict_does_nothing(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        tet_crud.add_no_genetic_data_workflow_tag(db, _tag(db, ref, _classifier_source(db, mods["FB"])))
        db.commit()
        assert _mi_tags(db, ref, mods["FB"]) == []

    def test_genetic_data_present_or_other_topic_does_nothing(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        curator = get_or_create_abc_source(db, "FB")
        tet_crud.add_no_genetic_data_workflow_tag(db, _tag(db, ref, curator, negated=False))
        tet_crud.add_no_genetic_data_workflow_tag(db, _tag(db, ref, curator, topic="ATP:0000122"))
        db.commit()
        assert _mi_tags(db, ref, mods["FB"]) == []

    def test_other_mods_are_not_affected(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        tet_crud.add_no_genetic_data_workflow_tag(db, _tag(db, ref, get_or_create_abc_source(db, "WB")))
        db.commit()
        assert _mi_tags(db, ref, mods["WB"]) == []

    def test_any_other_error_is_logged_not_raised(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        tag = _tag(db, ref, get_or_create_abc_source(db, "FB"))
        # The module logger is patched rather than read through caplog: an earlier test's logging
        # config can stop records reaching caplog, depending on the test order.
        with patch.object(tet_crud.logger, "warning") as warning, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.transition_to_workflow_status",
                      side_effect=RuntimeError("boom")):
            tet_crud.add_no_genetic_data_workflow_tag(db, tag)
        db.commit()
        assert _mi_tags(db, ref, mods["FB"]) == []
        assert db.query(TopicEntityTagModel).filter(
            TopicEntityTagModel.topic_entity_tag_id == tag.topic_entity_tag_id).one_or_none() is not None
        assert "RuntimeError: boom" in warning.call_args.args[0]

    def test_a_transition_fb_does_not_allow_is_logged_not_raised(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        _mi_tag(db, ref, mods["FB"], MI_NEEDED)  # no 274 -> 343 row in this test's table
        tag = _tag(db, ref, get_or_create_abc_source(db, "FB"))
        with patch.object(tet_crud.logger, "warning") as warning:
            tet_crud.add_no_genetic_data_workflow_tag(db, tag)
        db.commit()
        assert _mi_tags(db, ref, mods["FB"]) == [(MI_NEEDED, None)]
        assert db.query(TopicEntityTagModel).filter(
            TopicEntityTagModel.topic_entity_tag_id == tag.topic_entity_tag_id).one_or_none() is not None
        assert "SCRUM-6672" in warning.call_args.args[0]


@patch(ATP_MOCK, load_name_to_atp_and_relationships_mock)
class TestCuratorPaths:
    """The trigger is wired into every path that can make a curator "no genetic data" verdict."""

    @pytest.fixture(autouse=True)
    def _no_hierarchy_lookups(self):
        with patch("agr_literature_service.api.crud.topic_entity_tag_crud.atp_return_invalid_ids",
                   return_value=[]), \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_ancestors", return_value=[]), \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_descendants", return_value=[]), \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_curie_to_name_from_all_tets",
                      return_value={}), \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.build_curie_to_name_map",
                      return_value={}):
            yield

    def test_validate_creates_then_flips(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        tet_crud.validate_topic(db, REFERENCE_CURIE, GENETIC_DATA, "FB", negated=False)
        assert _mi_tags(db, ref, mods["FB"]) == []

        # Yes -> No on the same verdict row (SCRUM-6852): not through create_tag
        result = tet_crud.validate_topic(db, REFERENCE_CURIE, GENETIC_DATA, "FB", negated=True)
        assert result["outcome"] == "updated"
        assert _mi_tags(db, ref, mods["FB"]) == [(MI_WONT, NO_GENETIC_DATA)]

        # No -> Yes again: the trigger only adds
        tet_crud.validate_topic(db, REFERENCE_CURIE, GENETIC_DATA, "FB", negated=False)
        assert _mi_tags(db, ref, mods["FB"]) == [(MI_WONT, NO_GENETIC_DATA)]

    def test_validate_new_no_verdict(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        result = tet_crud.validate_topic(db, REFERENCE_CURIE, GENETIC_DATA, "FB", negated=True)
        assert result["outcome"] == "created"
        assert _mi_tags(db, ref, mods["FB"]) == [(MI_WONT, NO_GENETIC_DATA)]

    def test_patch_to_negated(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        tag = _tag(db, ref, get_or_create_abc_source(db, "FB"), negated=False)
        db.commit()
        tet_crud.apply_tag_patch(db, tag, tet_crud.TopicEntityTagSchemaUpdate(negated=True))
        db.commit()
        assert _mi_tags(db, ref, mods["FB"]) == [(MI_WONT, NO_GENETIC_DATA)]

    def test_patch_resending_negated_does_not_retrigger(self, fb_ref):  # noqa
        db, mods, ref = fb_ref  # noqa
        # The verdict was recorded earlier; since then someone moved the paper to "in progress".
        tag = _tag(db, ref, get_or_create_abc_source(db, "FB"))
        _mi_tag(db, ref, mods["FB"], MI_IN_PROGRESS)
        # An edit form sends the whole tag back with only the note changed.
        tet_crud.apply_tag_patch(db, tag, tet_crud.TopicEntityTagSchemaUpdate(negated=True, note="edited"))
        db.commit()
        assert _mi_tags(db, ref, mods["FB"]) == [(MI_IN_PROGRESS, None)]
