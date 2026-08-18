from agr_literature_service.api.crud.search_crud import sort_authors_by_order


class TestSortAuthorsByOrder:

    def test_sorts_by_author_order(self):
        authors = [
            {"name": "Third A", "orcid": None, "author_order": "3"},
            {"name": "First A", "orcid": None, "author_order": "1"},
            {"name": "Second A", "orcid": None, "author_order": "2"},
        ]
        result = sort_authors_by_order(authors)
        assert [a["name"] for a in result] == ["First A", "Second A", "Third A"]

    def test_handles_integer_author_order(self):
        authors = [
            {"name": "B", "author_order": 2},
            {"name": "A", "author_order": 1},
        ]
        result = sort_authors_by_order(authors)
        assert [a["name"] for a in result] == ["A", "B"]

    def test_missing_or_null_order_sorts_last_preserving_relative_order(self):
        authors = [
            {"name": "No order 1"},
            {"name": "Second", "author_order": "2"},
            {"name": "Null order", "author_order": None},
            {"name": "First", "author_order": "1"},
        ]
        result = sort_authors_by_order(authors)
        assert [a["name"] for a in result] == [
            "First", "Second", "No order 1", "Null order",
        ]

    def test_none_and_empty_input(self):
        assert sort_authors_by_order(None) == []
        assert sort_authors_by_order([]) == []
