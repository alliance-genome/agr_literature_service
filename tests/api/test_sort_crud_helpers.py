"""
Unit tests for small, self-contained helpers in sort_crud.

These call the helpers directly against the db fixture (or as pure functions),
avoiding the Elasticsearch/auth paths exercised by the API-level sort tests.
"""
import pytest

from agr_literature_service.api.crud import sort_crud
from agr_literature_service.api.models import (ModModel, ReferenceModel,
                                               ModCorpusAssociationModel)
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import db # noqa


class TestConvertXrefCurieToUrl:

    def test_known_prefix_substitutes_local_id(self):
        urls = {"WB": "https://wormbase.org/[%s]"}
        assert sort_crud.convert_xref_curie_to_url("WB:WBPaper00000001", urls) == \
            "https://wormbase.org/WBPaper00000001"

    def test_unknown_prefix_returns_none(self):
        assert sort_crud.convert_xref_curie_to_url("PMID:12345", {"WB": "x/[%s]"}) is None

    def test_only_first_colon_splits(self):
        urls = {"DOI": "https://doi.org/[%s]"}
        assert sort_crud.convert_xref_curie_to_url("DOI:10.1000/abc:def", urls) == \
            "https://doi.org/10.1000/abc:def"


@pytest.fixture
def need_review_setup(db): # noqa
    populate_test_mods()
    mod = db.query(ModModel).filter(ModModel.abbreviation == "WB").first()
    ref = ReferenceModel(curie="AGRKB:101000000010", category="research_article")
    db.add(ref)
    db.commit()
    db.refresh(ref)
    # corpus == None -> needs review
    mca = ModCorpusAssociationModel(
        reference_id=ref.reference_id, mod_id=mod.mod_id, corpus=None,
        mod_corpus_sort_source=ModCorpusSortSourceType.Mod_pubmed_search)
    db.add(mca)
    db.commit()
    return db, mod, ref


class TestGetNeedReviewSortSources:

    def test_returns_sorted_distinct_sources_for_mod(self, need_review_setup): # noqa
        db, mod, _ = need_review_setup  # noqa
        sources = sort_crud.get_need_review_sort_sources("WB", db)
        assert sources == ["mod_pubmed_search"]

    def test_returns_empty_for_mod_without_needs_review(self, need_review_setup): # noqa
        db, _, _ = need_review_setup  # noqa
        assert sort_crud.get_need_review_sort_sources("SGD", db) == []


class TestBuildNeedReviewBaseQuery:

    def test_valid_sort_source_filters_query(self, need_review_setup): # noqa
        db, _, ref = need_review_setup  # noqa
        query = sort_crud._build_need_review_base_query("WB", db, sort_source="mod_pubmed_search")
        results = query.all()
        assert [r.reference_id for r in results] == [ref.reference_id]

    def test_invalid_sort_source_is_ignored(self, need_review_setup): # noqa
        db, _, ref = need_review_setup  # noqa
        # An unknown sort_source logs a warning and does not add the filter,
        # so the base needs-review row is still returned.
        query = sort_crud._build_need_review_base_query("WB", db, sort_source="not_a_real_source")
        results = query.all()
        assert ref.reference_id in [r.reference_id for r in results]

    def test_no_sort_source_returns_all_needs_review(self, need_review_setup): # noqa
        db, _, ref = need_review_setup  # noqa
        query = sort_crud._build_need_review_base_query("WB", db)
        assert ref.reference_id in [r.reference_id for r in query.all()]
