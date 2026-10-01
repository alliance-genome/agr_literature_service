"""SCRUM-6620: server-side replacements for the Biblio page's client-side
aggregations over the capped 8,000-tag fetch.

- build_entity_counts_by_mod_query: distinct-entity counts per (owning MOD,
  entity type), compiled to SQL and asserted without a database.
- add_list_of_validating_tag_ids: the has_curator_validating_tag flag the
  Actions cell uses instead of scanning the full client-side tag list.
"""
from types import SimpleNamespace

from agr_literature_service.api.crud.topic_entity_tag_crud import (
    add_list_of_validating_tag_ids,
    build_entity_counts_by_mod_query,
)


def compiled_counts_sql(reference_id=12345):
    query = build_entity_counts_by_mod_query(reference_id)
    return str(query.statement.compile(compile_kwargs={"literal_binds": True}))


class TestEntityCountsByModQuery:

    def test_counts_distinct_entities(self):
        sql = compiled_counts_sql()
        assert "count(distinct(topic_entity_tag.entity))" in sql

    def test_groups_by_owning_mod_and_entity_type(self):
        sql = compiled_counts_sql()
        assert "GROUP BY mod.abbreviation, topic_entity_tag.entity_type" in sql

    def test_joins_source_to_owning_mod(self):
        sql = compiled_counts_sql()
        assert "tag_source.secondary_data_provider_id = mod.mod_id" in sql

    def test_scopes_to_the_reference_and_skips_topic_only_tags(self):
        sql = compiled_counts_sql(777)
        assert "topic_entity_tag.reference_id = 777" in sql
        assert "topic_entity_tag.entity IS NOT NULL" in sql
        assert "topic_entity_tag.entity_type IS NOT NULL" in sql


def _tag(validating_sources):
    """A duck-typed TET row whose validating tags carry the given source
    validation types (None = validating tag without a source)."""
    validated_by = [
        SimpleNamespace(
            topic_entity_tag_id=i + 1,
            tag_source=None if vt is None else SimpleNamespace(validation_type=vt),
        )
        for i, vt in enumerate(validating_sources)
    ]
    return SimpleNamespace(validated_by=validated_by)


class TestHasCuratorValidatingTag:

    def test_curator_sourced_validating_tag_sets_the_flag(self):
        data = {}
        add_list_of_validating_tag_ids(_tag(["author", "professional_biocurator"]), data)
        assert data["has_curator_validating_tag"] is True
        assert sorted(data["validating_tags"]) == [1, 2]

    def test_no_curator_sourced_validating_tag(self):
        data = {}
        add_list_of_validating_tag_ids(_tag(["author", None]), data)
        assert data["has_curator_validating_tag"] is False

    def test_no_validating_tags(self):
        data = {}
        add_list_of_validating_tag_ids(_tag([]), data)
        assert data["has_curator_validating_tag"] is False
        assert data["validating_tags"] == []

    def test_flag_survives_the_response_schema(self):
        # TopicEntityTagSchemaRelated uses extra='ignore', which silently
        # dropped the serializer's new key until it was declared on the schema
        # (review finding) — so FastAPI's response validation stripped it and
        # the UI never saw it. Round-trip a row through the schema like the
        # response model does and assert the flag survives.
        from typing import List
        from pydantic import TypeAdapter
        from agr_literature_service.api.schemas.topic_entity_tag_schemas import (
            TopicEntityTagSchemaRelated,
        )
        row = {
            "topic_entity_tag_id": 1,
            "topic": "ATP:0000005",
            "tag_source_id": 2,
            "date_created": "2026-10-01T00:00:00",
            "date_updated": "2026-10-01T00:00:00",
            "created_by": "u", "updated_by": "u",
            "validating_tags": [7],
            "has_curator_validating_tag": True,
        }
        dumped = TypeAdapter(List[TopicEntityTagSchemaRelated]).dump_python(
            TypeAdapter(List[TopicEntityTagSchemaRelated]).validate_python([row]))
        assert dumped[0]["has_curator_validating_tag"] is True
        # and the default when the serializer did not set it (no validating tags)
        row.pop("has_curator_validating_tag")
        dumped = TypeAdapter(List[TopicEntityTagSchemaRelated]).dump_python(
            TypeAdapter(List[TopicEntityTagSchemaRelated]).validate_python([row]))
        assert dumped[0]["has_curator_validating_tag"] is False
