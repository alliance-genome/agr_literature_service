"""
Unit tests for mod_corpus_association_crud.batch_update_corpus.

These exercise the CRUD function directly against the db fixture and mock the
workflow/TET/xref helpers it delegates to, so no Elasticsearch, A-team, or
Cognito dependencies are required.
"""
from unittest.mock import patch

import pytest

from agr_literature_service.api.crud import mod_corpus_association_crud as mca_crud
from agr_literature_service.api.models import (ModModel, ReferenceModel,
                                               ModCorpusAssociationModel,
                                               WorkflowTagModel)
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import db # noqa

HELPERS = "agr_literature_service.api.crud.mod_corpus_association_crud"


def _make_reference(db, curie): # noqa
    ref = ReferenceModel(curie=curie, category="research_article")
    db.add(ref)
    db.commit()
    db.refresh(ref)
    return ref


def _make_mca(db, reference, mod, corpus): # noqa
    mca = ModCorpusAssociationModel(
        reference_id=reference.reference_id,
        mod_id=mod.mod_id,
        corpus=corpus,
        mod_corpus_sort_source=ModCorpusSortSourceType.Mod_pubmed_search
    )
    db.add(mca)
    db.commit()
    db.refresh(mca)
    return mca


def _get_mod(db, abbreviation): # noqa
    return db.query(ModModel).filter(ModModel.abbreviation == abbreviation).first()


@pytest.fixture
def wb_setup(db): # noqa
    populate_test_mods()
    mod = _get_mod(db, "WB")
    ref = _make_reference(db, "AGRKB:101000000001")
    return db, mod, ref


class TestBatchUpdateCorpus:

    def test_missing_id_reported_as_failure(self, db): # noqa
        results = mca_crud.batch_update_corpus(db, [999999], corpus=False)
        assert len(results) == 1
        assert results[0].success is False
        assert results[0].mod_corpus_association_id == 999999
        assert "not found" in results[0].message
        assert results[0].reference_curie is None

    def test_move_out_success_without_manual_tags(self, wb_setup): # noqa
        db, mod, ref = wb_setup  # noqa
        mca = _make_mca(db, ref, mod, corpus=True)
        with patch(f"{HELPERS}.has_manual_tet", return_value=False), \
                patch(f"{HELPERS}.delete_non_manual_tets") as del_non_manual, \
                patch(f"{HELPERS}.delete_manual_tets") as del_manual, \
                patch(f"{HELPERS}.delete_workflow_tags") as del_wft, \
                patch(f"{HELPERS}.set_mod_curie_to_invalid") as set_invalid:
            results = mca_crud.batch_update_corpus(db, [mca.mod_corpus_association_id], corpus=False)

        assert results[0].success is True
        assert results[0].reference_curie == ref.curie
        del_non_manual.assert_called_once()
        del_manual.assert_not_called()
        del_wft.assert_called_once()
        set_invalid.assert_called_once()

        db.refresh(mca)
        assert mca.corpus is False
        assert mca.mod_corpus_sort_source == ModCorpusSortSourceType.Manual_creation

    def test_move_out_blocked_by_manual_tags(self, wb_setup): # noqa
        db, mod, ref = wb_setup  # noqa
        mca = _make_mca(db, ref, mod, corpus=True)
        with patch(f"{HELPERS}.has_manual_tet", return_value=True), \
                patch(f"{HELPERS}.delete_non_manual_tets") as del_non_manual:
            results = mca_crud.batch_update_corpus(db, [mca.mod_corpus_association_id],
                                                   corpus=False, force_out=False)

        assert results[0].success is False
        assert "force_out=true" in results[0].message
        del_non_manual.assert_not_called()

        db.refresh(mca)
        assert mca.corpus is True

    def test_move_out_force_deletes_manual_tags(self, wb_setup): # noqa
        db, mod, ref = wb_setup  # noqa
        mca = _make_mca(db, ref, mod, corpus=True)
        with patch(f"{HELPERS}.has_manual_tet", return_value=True), \
                patch(f"{HELPERS}.delete_non_manual_tets"), \
                patch(f"{HELPERS}.delete_manual_tets") as del_manual, \
                patch(f"{HELPERS}.delete_workflow_tags"), \
                patch(f"{HELPERS}.set_mod_curie_to_invalid"):
            results = mca_crud.batch_update_corpus(db, [mca.mod_corpus_association_id],
                                                   corpus=False, force_out=True)

        assert results[0].success is True
        del_manual.assert_called_once()

        db.refresh(mca)
        assert mca.corpus is False

    def test_move_in_success(self, wb_setup): # noqa
        db, mod, ref = wb_setup  # noqa
        mca = _make_mca(db, ref, mod, corpus=False)
        with patch(f"{HELPERS}.check_xref_and_generate_mod_id") as check_xref, \
                patch(f"{HELPERS}.get_current_workflow_status", return_value=None), \
                patch(f"{HELPERS}.transition_to_workflow_status") as transition:
            results = mca_crud.batch_update_corpus(db, [mca.mod_corpus_association_id], corpus=True)

        assert results[0].success is True
        check_xref.assert_called_once()
        transition.assert_called_once()

        db.refresh(mca)
        assert mca.corpus is True

    def test_move_in_zfin_adds_workflow_tag(self, db): # noqa
        populate_test_mods()
        mod = _get_mod(db, "ZFIN")
        ref = _make_reference(db, "AGRKB:101000000002")
        mca = _make_mca(db, ref, mod, corpus=False)
        with patch(f"{HELPERS}.check_xref_and_generate_mod_id"), \
                patch(f"{HELPERS}.get_current_workflow_status", return_value=None), \
                patch(f"{HELPERS}.transition_to_workflow_status"), \
                patch.dict(mca_crud.name_to_atp,
                           {"pre-indexing prioritization needed": "ATP:0000306"}, clear=False):
            results = mca_crud.batch_update_corpus(db, [mca.mod_corpus_association_id], corpus=True)

        assert results[0].success is True
        wft = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == ref.reference_id,
            WorkflowTagModel.workflow_tag_id == "ATP:0000306").first()
        assert wft is not None

    def test_exception_is_rolled_back_and_reported(self, wb_setup): # noqa
        db, mod, ref = wb_setup  # noqa
        mca = _make_mca(db, ref, mod, corpus=True)
        with patch(f"{HELPERS}.has_manual_tet", return_value=False), \
                patch(f"{HELPERS}.delete_non_manual_tets", side_effect=RuntimeError("boom")):
            results = mca_crud.batch_update_corpus(db, [mca.mod_corpus_association_id], corpus=False)

        assert results[0].success is False
        assert "boom" in results[0].message

        db.refresh(mca)
        assert mca.corpus is True

    def test_noop_when_already_in_target_state(self, wb_setup): # noqa
        db, mod, ref = wb_setup  # noqa
        mca = _make_mca(db, ref, mod, corpus=True)
        with patch(f"{HELPERS}.check_xref_and_generate_mod_id") as check_xref, \
                patch(f"{HELPERS}.has_manual_tet") as has_manual:
            results = mca_crud.batch_update_corpus(db, [mca.mod_corpus_association_id], corpus=True)

        assert results[0].success is True
        check_xref.assert_not_called()
        has_manual.assert_not_called()

    def test_mixed_batch_reports_each_item(self, wb_setup): # noqa
        db, mod, ref = wb_setup  # noqa
        mca = _make_mca(db, ref, mod, corpus=True)
        with patch(f"{HELPERS}.has_manual_tet", return_value=False), \
                patch(f"{HELPERS}.delete_non_manual_tets"), \
                patch(f"{HELPERS}.delete_manual_tets"), \
                patch(f"{HELPERS}.delete_workflow_tags"), \
                patch(f"{HELPERS}.set_mod_curie_to_invalid"):
            results = mca_crud.batch_update_corpus(
                db, [mca.mod_corpus_association_id, 888888], corpus=False)

        assert len(results) == 2
        by_id = {r.mod_corpus_association_id: r for r in results}
        assert by_id[mca.mod_corpus_association_id].success is True
        assert by_id[888888].success is False
