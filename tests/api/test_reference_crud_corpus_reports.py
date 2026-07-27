"""
Unit tests for reference_crud corpus / recently-* reporting helpers.

The SQL-only report functions are exercised against an empty (freshly-seeded)
database; add_to_corpus mocks the workflow/xref helpers it delegates to.
"""
import pytest
from unittest.mock import patch
from fastapi import HTTPException

from agr_literature_service.api.crud import reference_crud
from agr_literature_service.api.models import (ReferenceModel, ModModel,
                                               ModCorpusAssociationModel)
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import db # noqa

HELPERS = "agr_literature_service.api.crud.reference_crud"


def _reference(db, curie): # noqa
    ref = ReferenceModel(curie=curie, category="research_article")
    db.add(ref)
    db.commit()
    db.refresh(ref)
    return ref


def _mod(db, abbreviation): # noqa
    return db.query(ModModel).filter(ModModel.abbreviation == abbreviation).first()


@pytest.fixture
def seeded(db): # noqa
    populate_test_mods()
    ref = _reference(db, "AGRKB:101000400001")
    return db, ref


class TestAddToCorpus:

    def test_creates_new_corpus_association(self, seeded): # noqa
        db, ref = seeded  # noqa
        with patch(f"{HELPERS}.check_xref_and_generate_mod_id") as check_xref, \
                patch(f"{HELPERS}.get_current_workflow_status", return_value=None), \
                patch(f"{HELPERS}.transition_to_workflow_status") as transition:
            reference_crud.add_to_corpus(db, "WB", ref.curie)
        mca = db.query(ModCorpusAssociationModel).filter_by(
            reference_id=ref.reference_id).first()
        assert mca is not None
        assert mca.corpus is True
        check_xref.assert_called_once()
        transition.assert_called_once()

    def test_flips_existing_association_to_true(self, seeded): # noqa
        db, ref = seeded  # noqa
        mod = _mod(db, "WB")
        db.add(ModCorpusAssociationModel(
            reference_id=ref.reference_id, mod_id=mod.mod_id, corpus=False,
            mod_corpus_sort_source=ModCorpusSortSourceType.Mod_pubmed_search))
        db.commit()
        with patch(f"{HELPERS}.check_xref_and_generate_mod_id"), \
                patch(f"{HELPERS}.get_current_workflow_status", return_value="ATP:0000135"), \
                patch(f"{HELPERS}.transition_to_workflow_status"):
            reference_crud.add_to_corpus(db, "WB", ref.curie)
        mca = db.query(ModCorpusAssociationModel).filter_by(
            reference_id=ref.reference_id).one()
        assert mca.corpus is True

    def test_unknown_reference_raises_422(self, seeded): # noqa
        db, _ = seeded  # noqa
        with pytest.raises(HTTPException) as exc:
            reference_crud.add_to_corpus(db, "WB", "AGRKB:000000000000")
        assert exc.value.status_code == 422

    def test_unknown_mod_raises_422(self, seeded): # noqa
        db, ref = seeded  # noqa
        with pytest.raises(HTTPException) as exc:
            reference_crud.add_to_corpus(db, "NOPE", ref.curie)
        assert exc.value.status_code == 422


class TestRecentlySortedAndDeleted:

    def test_get_past_to_present_date_range(self):
        ts, start, end = reference_crud.get_past_to_present_date_range(7)
        assert isinstance(ts, str)
        assert (end - start).days == 8  # 7 days back + 1 day forward

    def test_recently_sorted_pmids_empty(self, seeded): # noqa
        db, _ = seeded  # noqa
        _, start, end = reference_crud.get_past_to_present_date_range(7)
        result = reference_crud.get_recently_sorted_pmids_without_mod_paper_id(
            db, "WB", start, end)
        assert result == []

    def test_recently_sorted_references_pmid_only(self, seeded): # noqa
        db, _ = seeded  # noqa
        assert reference_crud.get_recently_sorted_references(db, "WB", 7, pmid_only=True) == []

    def test_recently_sorted_references_full_empty(self, seeded): # noqa
        db, _ = seeded  # noqa
        result = reference_crud.get_recently_sorted_references(db, "WB", 7)
        assert result["data"] == []
        assert result["metaData"]["dataProvider"]["mod"] == "WB"

    def test_recently_deleted_references_empty(self, seeded): # noqa
        db, _ = seeded  # noqa
        result = reference_crud.get_recently_deleted_references(db, "WB", 7)
        assert result["data"] == []
        assert result["metaData"]["dataProvider"]["mod"] == "WB"

    def test_get_obsolete_mod_curies_empty(self, seeded): # noqa
        db, _ = seeded  # noqa
        assert reference_crud.get_obsolete_mod_curies(db, "WB") == []
