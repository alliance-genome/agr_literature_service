"""Unit tests for the pure date-range filter builders in
``agr_literature_service.api.crud.search_filters``.
"""
from agr_literature_service.api.crud import search_filters as sf


def _musts(es_body):
    return es_body["query"]["bool"]["filter"]["bool"]["must"]


class TestEnsureFilterStructure:
    def test_creates_nested_structure(self):
        body: dict = {}
        sf.ensure_filter_structure(body)
        assert _musts(body) == []

    def test_is_idempotent(self):
        body: dict = {}
        sf.ensure_filter_structure(body)
        _musts(body).append({"marker": True})
        sf.ensure_filter_structure(body)
        assert _musts(body) == [{"marker": True}]  # existing content preserved


class TestDateStrToMicroSeconds:
    def test_start_of_day(self):
        micros = sf.date_str_to_micro_seconds("2024-01-15", start=True)
        # 00:00:00 UTC -> divisible by a full day of microseconds
        assert micros % 1_000_000 == 0

    def test_end_of_day_is_after_start(self):
        start = sf.date_str_to_micro_seconds("2024-01-15", start=True)
        end = sf.date_str_to_micro_seconds("2024-01-15", start=False)
        assert end > start
        # end is 23:59:59.999000 -> within the same day (< 24h in micros)
        assert end - start < 86_400 * 1_000_000

    def test_ignores_time_component(self):
        with_time = sf.date_str_to_micro_seconds("2024-01-15T13:45:00", start=True)
        date_only = sf.date_str_to_micro_seconds("2024-01-15", start=True)
        assert with_time == date_only


class TestRangeAppenders:
    def test_pubmed_modified_none_is_noop(self):
        body: dict = {}
        sf.add_pubmed_modified_range(body, None)
        assert body == {}

    def test_pubmed_modified_appends_range(self):
        body: dict = {}
        sf.add_pubmed_modified_range(body, ("2024-01-01", "2024-02-01"))
        rng = _musts(body)[0]["range"]["date_last_modified_in_pubmed"]
        assert rng == {"gte": "2024-01-01", "lte": "2024-02-01"}

    def test_pubmed_arrive_appends_range(self):
        body: dict = {}
        sf.add_pubmed_arrive_range(body, ("2024-01-01", "2024-02-01"))
        assert "date_arrived_in_pubmed" in _musts(body)[0]["range"]

    def test_pubmed_arrive_none_is_noop(self):
        body: dict = {}
        sf.add_pubmed_arrive_range(body, None)
        assert body == {}

    def test_created_range_converts_to_micros(self):
        body: dict = {}
        sf.add_created_range(body, ("2024-01-01", "2024-01-31"))
        rng = _musts(body)[0]["range"]["date_created"]
        assert isinstance(rng["gte"], int) and isinstance(rng["lte"], int)
        assert rng["lte"] > rng["gte"]

    def test_created_range_none_is_noop(self):
        body: dict = {}
        sf.add_created_range(body, None)
        assert body == {}

    def test_published_overlap_has_three_should_clauses(self):
        body: dict = {}
        sf.add_published_interval_overlap(body, ("2024-01-01", "2024-12-31"))
        should = _musts(body)[0]["bool"]["should"]
        assert len(should) == 3

    def test_published_overlap_none_is_noop(self):
        body: dict = {}
        sf.add_published_interval_overlap(body, None)
        assert body == {}


class TestApplyAllDateFilters:
    def test_returns_false_when_nothing_applied(self):
        body: dict = {}
        assert sf.apply_all_date_filters(body) is False
        assert body == {}

    def test_applies_all_four_and_returns_true(self):
        body: dict = {}
        applied = sf.apply_all_date_filters(
            body,
            date_pubmed_modified=["2024-01-01", "2024-02-01"],
            date_pubmed_arrive=["2024-01-01", "2024-02-01"],
            date_published=["2024-01-01", "2024-12-31"],
            date_created=["2024-01-01", "2024-01-31"],
        )
        assert applied is True
        assert len(_musts(body)) == 4

    def test_applies_single_filter(self):
        body: dict = {}
        applied = sf.apply_all_date_filters(body, date_created=["2024-01-01", "2024-01-31"])
        assert applied is True
        assert len(_musts(body)) == 1
