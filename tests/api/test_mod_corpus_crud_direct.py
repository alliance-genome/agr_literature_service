"""
Direct unit tests for mod_corpus_association_crud create/patch/destroy/show
paths that the API-level tests don't exercise (MOD-specific indexing tags,
error branches). Workflow/TET/xref helpers are mocked.
"""
import pytest
from unittest.mock import patch
from fastapi import HTTPException

from agr_literature_service.api.crud import mod_corpus_association_crud as mca_crud
from agr_literature_service.api.models import (ModModel, ReferenceModel,
                                               ModCorpusAssociationModel,
                                               WorkflowTagModel)
from agr_literature_service.api.schemas import (ModCorpusAssociationSchemaPost,
                                                ModCorpusAssociationSchemaUpdate,
                                                ModCorpusSortSourceType)
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import db # noqa

HELPERS = "agr_literature_service.api.crud.mod_corpus_association_crud"
FILE_NEEDED = "ATP:0000141"
SGD_INDEX = "ATP:0000274"
ZFIN_INDEX = "ATP:0000306"


def _reference(db, curie): # noqa
    ref = ReferenceModel(curie=curie, category="research_article")
    db.add(ref)
    db.commit()
    db.refresh(ref)
    return ref


def _mca(db, reference, mod, corpus=None): # noqa
    mca = ModCorpusAssociationModel(
        reference_id=reference.reference_id, mod_id=mod.mod_id, corpus=corpus,
        mod_corpus_sort_source=ModCorpusSortSourceType.Mod_pubmed_search)
    db.add(mca)
    db.commit()
    db.refresh(mca)
    return mca


def _mod(db, abbreviation): # noqa
    return db.query(ModModel).filter(ModModel.abbreviation == abbreviation).first()


@pytest.fixture
def seeded(db): # noqa
    populate_test_mods()
    ref = _reference(db, "AGRKB:101000300001")
    return db, ref


class TestCreate:

    def test_zfin_corpus_adds_pre_indexing_tag(self, seeded): # noqa
        db, ref = seeded  # noqa
        payload = ModCorpusAssociationSchemaPost(
            mod_abbreviation="ZFIN", reference_curie=ref.curie, corpus=True,
            mod_corpus_sort_source=ModCorpusSortSourceType.Mod_pubmed_search)
        with patch(f"{HELPERS}.check_xref_and_generate_mod_id"), \
                patch(f"{HELPERS}.get_current_workflow_status", return_value="ATP:0000135"):
            new_id = mca_crud.create(db, payload)
        assert isinstance(new_id, int)
        tag = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == ref.reference_id,
            WorkflowTagModel.workflow_tag_id == ZFIN_INDEX).first()
        assert tag is not None

    def test_sgd_corpus_adds_manual_indexing_tag(self, seeded): # noqa
        db, ref = seeded  # noqa
        payload = ModCorpusAssociationSchemaPost(
            mod_abbreviation="SGD", reference_curie=ref.curie, corpus=True,
            mod_corpus_sort_source=ModCorpusSortSourceType.Mod_pubmed_search)
        with patch(f"{HELPERS}.check_xref_and_generate_mod_id"), \
                patch(f"{HELPERS}.get_current_workflow_status", return_value="ATP:0000135"):
            mca_crud.create(db, payload)
        tag = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == ref.reference_id,
            WorkflowTagModel.workflow_tag_id == SGD_INDEX).first()
        assert tag is not None

    def test_unknown_reference_raises_422(self, seeded): # noqa
        db, _ = seeded  # noqa
        payload = ModCorpusAssociationSchemaPost(
            mod_abbreviation="WB", reference_curie="AGRKB:000000000000", corpus=False,
            mod_corpus_sort_source=ModCorpusSortSourceType.Mod_pubmed_search)
        with pytest.raises(HTTPException) as exc:
            mca_crud.create(db, payload)
        assert exc.value.status_code == 422

    def test_duplicate_raises_422(self, seeded): # noqa
        db, ref = seeded  # noqa
        payload = ModCorpusAssociationSchemaPost(
            mod_abbreviation="WB", reference_curie=ref.curie, corpus=False,
            mod_corpus_sort_source=ModCorpusSortSourceType.Mod_pubmed_search)
        mca_crud.create(db, payload)
        with pytest.raises(HTTPException) as exc:
            mca_crud.create(db, payload)
        assert exc.value.status_code == 422


class TestDestroy:

    def test_destroy_removes_file_needed_tag(self, seeded): # noqa
        db, ref = seeded  # noqa
        mod = _mod(db, "WB")
        mca = _mca(db, ref, mod, corpus=True)
        db.add(WorkflowTagModel(reference_id=ref.reference_id, mod_id=mod.mod_id,
                                workflow_tag_id=FILE_NEEDED))
        db.commit()
        with patch(f"{HELPERS}.get_current_workflow_status", return_value=FILE_NEEDED):
            mca_crud.destroy(db, mca.mod_corpus_association_id)
        assert db.query(ModCorpusAssociationModel).filter(
            ModCorpusAssociationModel.mod_corpus_association_id == mca.mod_corpus_association_id
        ).first() is None
        assert db.query(WorkflowTagModel).filter(
            WorkflowTagModel.workflow_tag_id == FILE_NEEDED,
            WorkflowTagModel.reference_id == ref.reference_id).first() is None

    def test_destroy_unknown_raises_404(self, seeded): # noqa
        db, _ = seeded  # noqa
        with pytest.raises(HTTPException) as exc:
            mca_crud.destroy(db, 987654)
        assert exc.value.status_code == 404


class TestPatch:

    def test_patch_unknown_raises_404(self, seeded): # noqa
        db, _ = seeded  # noqa
        update = ModCorpusAssociationSchemaUpdate(mod_corpus_sort_source=ModCorpusSortSourceType.Dqm_files)
        with pytest.raises(HTTPException) as exc:
            mca_crud.patch(db, 987654, update.model_dump(exclude_unset=True))
        assert exc.value.status_code == 404

    def test_patch_reference_curie_not_found_raises_422(self, seeded): # noqa
        db, ref = seeded  # noqa
        mod = _mod(db, "WB")
        mca = _mca(db, ref, mod, corpus=False)
        update = ModCorpusAssociationSchemaUpdate(reference_curie="AGRKB:000000000000")
        with pytest.raises(HTTPException) as exc:
            mca_crud.patch(db, mca.mod_corpus_association_id, update.model_dump(exclude_unset=True))
        assert exc.value.status_code == 422

    def test_patch_corpus_false_blocked_by_manual_tags(self, seeded): # noqa
        db, ref = seeded  # noqa
        mod = _mod(db, "WB")
        mca = _mca(db, ref, mod, corpus=True)
        update = ModCorpusAssociationSchemaUpdate(corpus=False)
        with patch(f"{HELPERS}.has_manual_tet", return_value=True):
            with pytest.raises(HTTPException) as exc:
                mca_crud.patch(db, mca.mod_corpus_association_id, update.model_dump(exclude_unset=True))
        assert exc.value.status_code == 422

    def test_patch_sgd_corpus_true_adds_index_tag(self, seeded): # noqa
        db, ref = seeded  # noqa
        mod = _mod(db, "SGD")
        mca = _mca(db, ref, mod, corpus=False)
        update = ModCorpusAssociationSchemaUpdate(corpus=True, index_wft_id=SGD_INDEX)
        with patch(f"{HELPERS}.check_xref_and_generate_mod_id"), \
                patch(f"{HELPERS}.get_current_workflow_status", return_value="ATP:0000135"):
            mca_crud.patch(db, mca.mod_corpus_association_id, update.model_dump(exclude_unset=True))
        tag = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == ref.reference_id,
            WorkflowTagModel.workflow_tag_id == SGD_INDEX).first()
        assert tag is not None


class TestShowErrors:

    def test_show_by_reference_mod_unknown_mod(self, seeded): # noqa
        db, ref = seeded  # noqa
        with pytest.raises(HTTPException) as exc:
            mca_crud.show_by_reference_mod_abbreviation(db, ref.curie, "NOPE")
        assert exc.value.status_code == 404

    def test_show_by_reference_mod_unknown_reference(self, seeded): # noqa
        db, _ = seeded  # noqa
        with pytest.raises(HTTPException) as exc:
            mca_crud.show_by_reference_mod_abbreviation(db, "AGRKB:000000000000", "WB")
        assert exc.value.status_code == 404

    def test_show_by_reference_mod_no_association(self, seeded): # noqa
        db, ref = seeded  # noqa
        with pytest.raises(HTTPException) as exc:
            mca_crud.show_by_reference_mod_abbreviation(db, ref.curie, "WB")
        assert exc.value.status_code == 404

    def test_show_changesets_unknown_raises_404(self, seeded): # noqa
        db, _ = seeded  # noqa
        with pytest.raises(HTTPException) as exc:
            mca_crud.show_changesets(db, 987654)
        assert exc.value.status_code == 404
