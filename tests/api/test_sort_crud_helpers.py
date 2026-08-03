"""
Unit tests for small, self-contained helpers in sort_crud.

These call the helpers directly against the db fixture (or as pure functions),
avoiding the Elasticsearch/auth paths exercised by the API-level sort tests.
"""
from unittest.mock import patch

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


HELPERS = "agr_literature_service.api.crud.sort_crud"


class TestSearchEsForCuries:

    def test_returns_curies_and_total_from_search(self):
        fake_result = {
            "hits": [{"curie": "AGRKB:1"}, {"curie": "AGRKB:2"}, {"no_curie": "x"}],
            "return_count": 2,
        }
        with patch(f"{HELPERS}.search_crud.search_references", return_value=fake_result):
            curies, total = sort_crud._search_es_for_curies("WB", "kinase")
        assert curies == ["AGRKB:1", "AGRKB:2"]
        assert total == 2

    def test_search_exception_returns_empty(self):
        with patch(f"{HELPERS}.search_crud.search_references", side_effect=RuntimeError("es down")):
            curies, total = sort_crud._search_es_for_curies("WB", "kinase")
        assert curies == []
        assert total == 0


class TestShowNeedReview:

    def test_search_query_with_no_es_matches_returns_empty(self, need_review_setup): # noqa
        db, _, _ = need_review_setup  # noqa
        with patch(f"{HELPERS}._search_es_for_curies", return_value=([], 0)):
            result = sort_crud.show_need_review("WB", None, db, search_query="nomatch")
        assert result == {"total_count": 0, "references": []}

    def test_sort_source_path_counts_and_sorts(self, need_review_setup): # noqa
        db, _, ref = need_review_setup  # noqa
        # patch schema-building so we exercise the query/sort/limit branches only
        with patch(f"{HELPERS}.show_sort_result", return_value=[]) as show_result:
            result = sort_crud.show_need_review(
                "WB", 5, db, sort_source="mod_pubmed_search",
                sort_by="date_published", sort_order="asc")
        assert result["total_count"] == 1
        passed_refs = show_result.call_args[0][0]
        assert ref.reference_id in [r.reference_id for r in passed_refs]

    def test_search_query_with_matches_filters_by_curie(self, need_review_setup): # noqa
        db, _, ref = need_review_setup  # noqa
        with patch(f"{HELPERS}._search_es_for_curies", return_value=([ref.curie], 1)), \
                patch(f"{HELPERS}.show_sort_result", return_value=[]) as show_result:
            result = sort_crud.show_need_review("WB", None, db, search_query="hit")
        assert result["total_count"] == 1
        passed_refs = show_result.call_args[0][0]
        assert [r.reference_id for r in passed_refs] == [ref.reference_id]


class TestGetReferencefileMod:

    def test_no_rows_returns_empty_list(self, db): # noqa
        populate_test_mods()
        assert sort_crud.get_referencefile_mod(999999, db) == []


class TestRecentlySorted:

    def test_get_mod_curators_returns_empty_maps_when_no_rows(self, db): # noqa
        populate_test_mods()
        name_to_email, email_to_id = sort_crud.get_mod_curators(db, "WB")
        assert name_to_email == {}
        assert email_to_id == {}

    def test_get_recently_sorted_reference_ids_no_data(self, db): # noqa
        populate_test_mods()
        ids = sort_crud.get_recently_sorted_reference_ids(db, "WB", count=10,
                                                          curator_id=None, day=7)
        assert ids == []

    def test_get_recently_sorted_reference_ids_with_curator_and_count(self, db): # noqa
        populate_test_mods()
        # curator_id and count exercise the optional SQL clauses
        ids = sort_crud.get_recently_sorted_reference_ids(db, "WB", count=5,
                                                          curator_id="123", day=30)
        assert ids == []

    def test_show_recently_sorted_no_data(self, db): # noqa
        populate_test_mods()
        result = sort_crud.show_recently_sorted(db, "WB", count=10,
                                                curator_email=None, day=7)
        assert result == {"curator_data": {}, "data": []}
