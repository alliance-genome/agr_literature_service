"""Unit tests for the TET table's multi-column server-side filters (SCRUM-6618).

apply_column_filters builds SQLAlchemy conditions for the infinite row model's
filter model: {"column": {"values": [...]} | {"contains": str} | {"range":
[min, max]}}, ANDed across columns. The tests compile the filtered query to SQL
and assert on the generated clauses, so they need no database.
"""
import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Query

from agr_literature_service.api.crud.topic_entity_tag_crud import apply_column_filters
from agr_literature_service.api.models import TopicEntityTagModel


def compiled(column_filters):
    query = Query(TopicEntityTagModel)
    filtered = apply_column_filters(query, column_filters)
    return str(filtered.statement.compile(compile_kwargs={"literal_binds": True}))


class TestApplyColumnFilters:

    def test_values_filter_compiles_to_in(self):
        sql = compiled({"topic": {"values": ["ATP:0000005", "ATP:0000122"]}})
        assert "topic_entity_tag.topic IN ('ATP:0000005', 'ATP:0000122')" in sql

    def test_bare_list_is_values_shorthand(self):
        sql = compiled({"topic": ["ATP:0000005"]})
        assert "topic_entity_tag.topic IN ('ATP:0000005')" in sql

    def test_null_in_values_matches_is_null(self):
        sql = compiled({"entity": {"values": ["SGD:S000001085", None]}})
        assert "topic_entity_tag.entity IN ('SGD:S000001085')" in sql
        assert "topic_entity_tag.entity IS NULL" in sql
        assert " OR " in sql

    def test_only_null_selected(self):
        sql = compiled({"entity": {"values": [None]}})
        assert "topic_entity_tag.entity IS NULL" in sql
        assert " IN (" not in sql

    def test_empty_values_is_a_noop(self):
        assert compiled({"topic": {"values": []}}) == compiled({})

    def test_contains_is_case_insensitive_substring(self):
        sql = compiled({"note": {"contains": "Dog2"}})
        assert "lower(" in sql.lower()
        assert "'%dog2%'" in sql.lower()

    def test_range_bounds_inclusive(self):
        sql = compiled({"confidence_score": {"range": [0.5, 0.9]}})
        assert "topic_entity_tag.confidence_score >= 0.5" in sql
        assert "topic_entity_tag.confidence_score <= 0.9" in sql

    def test_range_open_ended(self):
        sql = compiled({"confidence_score": {"range": [0.5, None]}})
        assert "topic_entity_tag.confidence_score >= 0.5" in sql
        assert "<=" not in sql

    def test_columns_are_anded(self):
        sql = compiled({
            "topic": {"values": ["ATP:0000005"]},
            "confidence_level": {"values": ["High"]},
        })
        assert "topic_entity_tag.topic IN ('ATP:0000005')" in sql
        assert "topic_entity_tag.confidence_level IN ('High')" in sql
        assert sql.count("AND") >= 1

    def test_tag_source_column_uses_exists(self):
        sql = compiled({"tag_source.source_method": {"values": ["abc_literature_system"]}})
        assert "EXISTS" in sql
        assert "source_method IN ('abc_literature_system')" in sql

    def test_secondary_data_provider_uses_nested_exists(self):
        sql = compiled({"secondary_data_provider": {"values": ["SGD"]}})
        assert sql.count("EXISTS") >= 2
        assert "abbreviation IN ('SGD')" in sql

    def test_unknown_column_is_422(self):
        with pytest.raises(HTTPException) as exc:
            compiled({"not_a_column": {"values": ["x"]}})
        assert exc.value.status_code == 422

    def test_unknown_tag_source_column_is_422(self):
        with pytest.raises(HTTPException) as exc:
            compiled({"tag_source.bogus": {"values": ["x"]}})
        assert exc.value.status_code == 422

    def test_unsupported_spec_is_422(self):
        with pytest.raises(HTTPException) as exc:
            compiled({"topic": {"equals": "x"}})
        assert exc.value.status_code == 422

    def test_none_filters_is_a_noop(self):
        query = Query(TopicEntityTagModel)
        assert apply_column_filters(query, None) is query

    # ---- review hardening: malformed input 422s instead of 500ing ----

    def test_relationship_and_non_column_attrs_are_422(self):
        for bad in ("tag_source", "metadata", "reference", "validated_by"):
            with pytest.raises(HTTPException) as exc:
                compiled({bad: {"values": ["x"]}})
            assert exc.value.status_code == 422

    def test_tag_source_relationship_attr_is_422(self):
        with pytest.raises(HTTPException) as exc:
            compiled({"tag_source.secondary_data_provider": {"values": ["SGD"]}})
        assert exc.value.status_code == 422

    def test_values_must_be_a_list(self):
        # a bare string must not be iterated char-by-char into IN ('a','b','c')
        with pytest.raises(HTTPException) as exc:
            compiled({"topic": {"values": "abc"}})
        assert exc.value.status_code == 422

    def test_range_must_be_a_two_element_list_of_numbers(self):
        for bad in (0.5, "0.5", [0.1, 0.2, 0.3], ["a", "b"], {"min": 0}):
            with pytest.raises(HTTPException) as exc:
                compiled({"confidence_score": {"range": bad}})
            assert exc.value.status_code == 422

    def test_contains_must_be_a_string(self):
        with pytest.raises(HTTPException) as exc:
            compiled({"note": {"contains": 42}})
        assert exc.value.status_code == 422

    def test_contains_escapes_like_wildcards(self):
        sql = compiled({"note": {"contains": "50%_done"}})
        assert "50\\%\\_done" in sql
        assert "ESCAPE" in sql.upper()


class TestByReferenceResponseModel:
    """Regression for the column_only 500 (SCRUM-6618): the endpoint returns a
    list of distinct strings on that path, and FastAPI rejected its own
    response when the return annotation lacked List[str]."""

    def test_return_annotation_accepts_all_three_shapes(self):
        from typing import get_type_hints
        from pydantic import TypeAdapter
        from agr_literature_service.api.routers import topic_entity_tag_router
        hints = get_type_hints(topic_entity_tag_router.show_all_reference_tags)
        adapter = TypeAdapter(hints["return"])
        # column_only path: distinct values as strings
        assert adapter.validate_python(["ATP:0000005", "NCBITaxon:559292"]) \
            == ["ATP:0000005", "NCBITaxon:559292"]
        # count_only path
        assert adapter.validate_python(522) == 522
        # empty result
        assert adapter.validate_python([]) == []
