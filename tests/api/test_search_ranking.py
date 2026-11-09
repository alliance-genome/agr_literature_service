"""Unit tests for the pure Elasticsearch query-building / ranking helpers in
``agr_literature_service.api.crud.search_ranking``.

Every function here is a pure transform (query string / config -> ES DSL dict),
so no DB, ES, or network access is needed.
"""
from agr_literature_service.api.crud import search_ranking as sr


class TestFieldHelpers:
    def test_fields_with_boost(self):
        assert sr._fields_with_boost({"title": 5.0, "abstract": 2.0}) == [
            "title^5.0", "abstract^2.0",
        ]

    def test_exported_phrase_fields(self):
        assert "title^6.0" in sr.PHRASE_FIELDS
        assert sr.ALL_TEXT_FIELDS == ["title", "keywords", "abstract", "citation", "mesh_terms.heading_term"]

    def test_mesh_ranks_below_every_other_text_field(self):
        # SCRUM-6490: MeSH has the lowest weight in each all-fields boost set
        for boosts in (sr.BOOST_PHRASE_FIELDS, sr.BOOST_BEST_FIELDS, sr.BOOST_PREFIX_FIELDS):
            others = [w for f, w in boosts.items() if f != sr.MESH_FIELD]
            assert boosts[sr.MESH_FIELD] < min(others)


class TestRescoreWindow:
    def test_floor_is_200(self):
        assert sr._rescore_window(0) == 200
        assert sr._rescore_window(None) == 200

    def test_scales_with_size(self):
        assert sr._rescore_window(100) == 500  # 100 * 5

    def test_rescore_exact_phrase_structure(self):
        body = sr.rescore_exact_phrase("gene x", ["title^6.0"], size=50, weight=3.0)
        assert body["window_size"] == 250
        q = body["query"]
        assert q["rescore_query_weight"] == 3.0
        mm = q["rescore_query"]["multi_match"]
        assert mm["query"] == "gene x"
        assert mm["type"] == "phrase"
        assert mm["fields"] == ["title^6.0"]


class TestRecencyBoost:
    def test_wraps_existing_query(self):
        es_body = {"query": {"match": {"title": "x"}}}
        sr.apply_balanced_recency_boost(es_body)
        fs = es_body["query"]["function_score"]
        assert fs["query"] == {"match": {"title": "x"}}
        assert fs["boost_mode"] == "multiply"
        assert len(fs["functions"]) == 2

    def test_defaults_to_match_all_when_no_query(self):
        es_body: dict = {}
        sr.apply_balanced_recency_boost(es_body, field="date_created")
        fs = es_body["query"]["function_score"]
        assert fs["query"] == {"match_all": {}}
        # the decay function should reference the custom field
        assert any("exp" in fn and "date_created" in fn["exp"] for fn in fs["functions"])

    def test_multiplier_is_bounded(self):
        # SCRUM-2278: floor of 1.0 plus a capped bonus, summed, so date alone can
        # never move a paper by more than 1 + RECENCY_MAX_BONUS
        es_body: dict = {"query": {"match": {"title": "x"}}}
        sr.apply_balanced_recency_boost(es_body)
        fs = es_body["query"]["function_score"]
        assert fs["score_mode"] == "sum"
        assert {"weight": 1.0} in fs["functions"]
        assert fs["max_boost"] == 1.0 + sr.RECENCY_MAX_BONUS
        assert all("gauss" not in fn for fn in fs["functions"])

    def test_bonus_only_for_dated_papers(self):
        es_body: dict = {}
        sr.apply_balanced_recency_boost(es_body)
        decay = next(fn for fn in es_body["query"]["function_score"]["functions"] if "exp" in fn)
        assert decay["filter"] == {"exists": {"field": "date_published_start"}}
        assert decay["weight"] == sr.RECENCY_MAX_BONUS


class TestNestedAuthorHelpers:
    def test_match_any_without_boost(self):
        node = sr.nested_author_name_match_any("Doe")
        assert "boost" not in node["nested"]
        assert node["nested"]["query"]["match"]["authors.name"]["query"] == "Doe"

    def test_match_any_with_boost(self):
        node = sr.nested_author_name_match_any("Doe", boost=10.0)
        assert node["nested"]["boost"] == 10.0

    def test_exact_keyword(self):
        node = sr.nested_author_name_exact_keyword("Jane Doe")
        assert node["nested"]["query"]["term"]["authors.name.keyword"] == "Jane Doe"
        assert node["nested"]["boost"] == 8.0

    def test_prefix_keyword_appends_wildcard(self):
        node = sr.nested_author_name_prefix_keyword("Step")
        assert node["nested"]["query"]["wildcard"]["authors.name.keyword"] == "Step*"

    def test_match_prefix(self):
        node = sr.nested_author_name_match_prefix("Arthur C")
        assert node["nested"]["query"]["match_phrase_prefix"]["authors.name"]["query"] == "Arthur C"

    def test_exact_token_uses_and_operator(self):
        node = sr.nested_author_name_exact_token("West")
        mm = node["nested"]["query"]["match"]["authors.name"]
        assert mm["operator"] == "and"
        assert node["nested"]["boost"] == 12.0


class TestAuthorBucket:
    def test_full_name_bucket(self):
        fs = sr.build_author_bucket_function_score(
            "Jane Doe", is_full_name=True, partial_match=False,
        )["function_score"]
        # full-name path uses a term keyword exact clause
        exact = fs["functions"][0]["filter"]["nested"]["query"]
        assert exact["term"]["authors.name.keyword"] == "Jane Doe"
        assert fs["functions"][0]["weight"] == 2.0
        assert fs["boost_mode"] == "replace"

    def test_single_token_partial_adds_prefix_clauses(self):
        full = sr.build_author_bucket_function_score(
            "West", is_full_name=False, partial_match=False,
        )["function_score"]
        partial = sr.build_author_bucket_function_score(
            "West", is_full_name=False, partial_match=True,
        )["function_score"]
        assert len(partial["query"]["bool"]["should"]) > len(full["query"]["bool"]["should"])

    def test_orcid_clause_appended(self):
        orcid = {"nested": {"path": "cross_references", "query": {"term": {"x": "0000"}}}}
        fs = sr.build_author_bucket_function_score(
            "Doe", is_full_name=True, partial_match=False, orcid_nested_clause=orcid,
        )["function_score"]
        assert orcid in fs["query"]["bool"]["should"]

    def test_author_bucket_sort_order(self):
        srt = sr.author_bucket_sort(order="asc")
        assert srt[0] == {"_score": {"order": "desc"}}
        assert srt[1]["date_published_start"]["order"] == "asc"


class TestQueryStringHelpers:
    def test_strip_orcid_prefix(self):
        assert sr.strip_orcid_prefix_for_free_text("  ORCID: 0000-1") == "0000-1"
        assert sr.strip_orcid_prefix_for_free_text("orcid:abc") == "abc"
        assert sr.strip_orcid_prefix_for_free_text(None) == ""

    def test_strip_trailing_wildcards(self):
        assert sr.strip_trailing_wildcards("boo*") == "boo"
        assert sr.strip_trailing_wildcards("what??") == "what"
        assert sr.strip_trailing_wildcards(None) == ""

    def test_compute_minimum_should_match_thresholds(self):
        assert sr.compute_minimum_should_match("a b c") == "100%"          # <6
        assert sr.compute_minimum_should_match(" ".join(["w"] * 6)) == "80%"
        assert sr.compute_minimum_should_match(" ".join(["w"] * 10)) == "75%"
        assert sr.compute_minimum_should_match(" ".join(["w"] * 16)) == "70%"
        assert sr.compute_minimum_should_match(None) == "100%"


class TestAddSimpleTextFieldQuery:
    def _empty_body(self):
        return {"query": {"bool": {"must": []}}}

    def test_title_adds_keyword_term_and_prefix(self):
        body = self._empty_body()
        sr.add_simple_text_field_query(body, "title", "boo*", partial_match=True)
        should = body["query"]["bool"]["must"][0]["bool"]["should"]
        clause_types = [list(c.keys())[0] for c in should]
        assert "match_phrase" in clause_types
        assert "match" in clause_types
        assert "match_phrase_prefix" in clause_types  # partial_match=True
        assert "term" in clause_types                 # title field

    def test_non_title_without_partial(self):
        body = self._empty_body()
        sr.add_simple_text_field_query(body, "abstract", "hydrogen", partial_match=False)
        should = body["query"]["bool"]["must"][0]["bool"]["should"]
        clause_types = [list(c.keys())[0] for c in should]
        assert "match_phrase_prefix" not in clause_types
        assert "term" not in clause_types


class TestBuildQueries:
    def test_id_xref_author_helpers_toggle(self):
        with_author = sr.build_id_xref_author_helpers("Doe", include_author=True)
        without = sr.build_id_xref_author_helpers("Doe", include_author=False)
        assert len(without) == 2  # only the two wildcard curie clauses
        assert len(with_author) > len(without)

    def test_build_all_text_query_shape(self):
        res = sr.build_all_text_query("Jane Doe", size_result_count=10)
        assert res["uses_rescore"] is False
        assert res["rescore"] is None
        must_should = res["must"][0]["bool"]["should"]
        # best_fields + phrase + phrase_prefix + author/id helpers
        assert len(must_should) > 3
        assert res["should"][0]["term"]["title.keyword"]["boost"] == sr.BOOST_EXACT_TITLE_KEYWORD

    def test_build_all_text_query_without_helpers(self):
        res = sr.build_all_text_query("Jane Doe", include_id_author_helpers=False)
        must_should = res["must"][0]["bool"]["should"]
        assert len(must_should) == 3  # only the three multi_match clauses


class TestScoringAndSort:
    def test_text_search_with_rescore_drops_sort(self):
        es_body = {"query": {"match": {"title": "x"}}, "sort": ["existing"]}
        sr.apply_scoring_and_sort(es_body, is_text_search=True, uses_rescore=True)
        assert "sort" not in es_body
        assert "function_score" in es_body["query"]

    def test_text_search_without_rescore_sets_score_first_sort(self):
        es_body = {"query": {"match": {"title": "x"}}}
        sr.apply_scoring_and_sort(es_body, is_text_search=True, uses_rescore=False)
        assert es_body["sort"][0] == {"_score": {"order": "desc"}}

    def test_non_text_search_is_recency_first(self):
        es_body: dict = {}
        sr.apply_scoring_and_sort(es_body, is_text_search=False, uses_rescore=False, order="asc")
        assert es_body["sort"][0]["date_published_start"]["order"] == "asc"
        # _score comes after the date fields for non-text search
        assert es_body["sort"][2] == {"_score": {"order": "desc"}}

    def test_explicit_date_sort_is_date_first_even_for_text_search(self):
        # User picked "Oldest first" on a text search: chronological order must
        # drive the sort, with score only as a tie-breaker (SCRUM-6451).
        es_body = {"query": {"match": {"title": "x"}}}
        sr.apply_scoring_and_sort(
            es_body, is_text_search=True, uses_rescore=False, order="asc",
            explicit_date_sort=True,
        )
        assert es_body["sort"][0]["date_published_start"]["order"] == "asc"
        assert es_body["sort"][1]["date_created"]["order"] == "asc"
        assert es_body["sort"][2] == {"_score": {"order": "desc"}}
        # No recency function_score wrapping when the user asked for a date sort
        assert "function_score" not in es_body["query"]

    def test_explicit_date_sort_overrides_rescore(self):
        # ES rejects an explicit sort combined with rescore, so the rescore
        # block must be dropped when the user asks for a date sort.
        es_body = {"query": {"match": {"title": "x"}}, "rescore": {"window_size": 200}}
        sr.apply_scoring_and_sort(
            es_body, is_text_search=True, uses_rescore=True, order="desc",
            explicit_date_sort=True,
        )
        assert "rescore" not in es_body
        assert es_body["sort"][0]["date_published_start"]["order"] == "desc"


class TestContentGate:
    def test_content_tokens_drop_stopwords_and_short(self):
        toks = sr._content_tokens_for_gate("the DNA of a X")
        assert "the" not in toks and "of" not in toks and "a" not in toks
        assert "dna" in toks
        assert "x" not in toks  # single char dropped

    def test_msm_for_content_gate_thresholds(self):
        assert sr._msm_for_content_gate(1) == "100%"
        assert sr._msm_for_content_gate(4) == "80%"
        assert sr._msm_for_content_gate(8) == "70%"
        assert sr._msm_for_content_gate(12) == "60%"

    def test_build_content_gate_filter(self):
        f = sr.build_content_gate_filter("oxidative stress response")
        mm = f["multi_match"]
        assert mm["operator"] == "or"
        assert mm["query"] == "oxidative stress response"
        assert mm["minimum_should_match"] == "80%"  # 3 content tokens -> 80%
        # a paper that matches only on its MeSH terms passes the gate
        assert sr.MESH_FIELD in mm["fields"]

    def test_build_content_gate_filter_none_when_all_stopwords(self):
        assert sr.build_content_gate_filter("the of a") is None
