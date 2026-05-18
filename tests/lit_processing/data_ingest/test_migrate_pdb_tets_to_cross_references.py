"""Tests for migrate_pdb_tets_to_cross_references.py"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from agr_literature_service.lit_processing.data_ingest.pdb_ingest import (
    migrate_pdb_tets_to_cross_references as mod,
)


def _make_db_with_tets(tets, source=None, existing_xref=None, ref_curie_map=None):
    """Build a MagicMock db whose query() returns different things depending
    on the model argument. ref_curie_map maps reference_id -> curie; defaults
    to a single shared curie."""
    db = MagicMock()
    if ref_curie_map is None:
        ref_curie_map = {}

    def query_side_effect(model):
        q = MagicMock()
        if model is mod.TopicEntityTagSourceModel:
            q.filter_by.return_value.one_or_none.return_value = source
        elif model is mod.TopicEntityTagModel:
            q.filter_by.return_value.all.return_value = tets
        elif model is mod.ReferenceModel:
            # Use filter_by side_effect to return per-reference_id curie.
            def ref_filter_by(**kwargs):
                rid = kwargs.get("reference_id")
                if rid in ref_curie_map:
                    return MagicMock(one_or_none=lambda: MagicMock(curie=ref_curie_map[rid]))
                return MagicMock(one_or_none=lambda: MagicMock(curie=f"AGR:R{rid}"))
            q.filter_by.side_effect = ref_filter_by
        elif model is mod.CrossReferenceModel:
            q.filter_by.return_value.one_or_none.return_value = existing_xref
        return q

    db.query.side_effect = query_side_effect
    return db


class TestMigrate:

    @patch.object(mod, "create_tag")
    @patch.object(mod.cross_reference_crud, "create")
    @patch.object(mod, "set_global_user_id")
    def test_exits_cleanly_when_source_missing(self, mock_set_uid, mock_create, mock_create_tag):
        db = _make_db_with_tets(tets=[], source=None)
        counts = mod.migrate(db=db)
        assert counts == {
            "migrated": 0, "skipped_duplicate": 0, "skipped_no_entity": 0,
            "errors": 0, "tets_deleted": 0,
            "topic_tet_created": 0, "topic_tet_skipped_duplicate": 0,
        }
        mock_create.assert_not_called()
        mock_create_tag.assert_not_called()
        db.execute.assert_not_called()
        db.delete.assert_not_called()

    @patch.object(mod, "create_tag", return_value={"status": "success"})
    @patch.object(mod.cross_reference_crud, "create", return_value=999)
    @patch.object(mod, "set_global_user_id")
    def test_migrates_each_tet_and_creates_one_topic_tet_per_reference(
        self, mock_set_uid, mock_create, mock_create_tag,
    ):
        source = MagicMock(topic_entity_tag_source_id=42)
        # Three TETs across two references; ref 100 has two PDB IDs.
        tet1 = MagicMock(topic_entity_tag_id=1, reference_id=100, entity="1abc")
        tet2 = MagicMock(topic_entity_tag_id=2, reference_id=100, entity="2DEF")
        tet3 = MagicMock(topic_entity_tag_id=3, reference_id=200, entity="3xyz")
        db = _make_db_with_tets(tets=[tet1, tet2, tet3], source=source)
        db.execute.return_value = MagicMock(rowcount=3)

        counts = mod.migrate(db=db)

        assert counts["migrated"] == 3
        assert counts["errors"] == 0
        assert counts["tets_deleted"] == 3
        # One topic-only TET per *distinct* reference, not per PDB ID.
        assert counts["topic_tet_created"] == 2
        assert mock_create_tag.call_count == 2
        # All cross_ref creates happened with the expected curies.
        curies = [call.args[1].curie for call in mock_create.call_args_list]
        assert curies == ["PDB:1ABC", "PDB:2DEF", "PDB:3XYZ"]
        # Topic-only TETs are entity-free.
        for call in mock_create_tag.call_args_list:
            tet_payload = call.args[1].dict()
            assert tet_payload["topic"] == mod.PROTEIN_STRUCTURE_ATP
            assert tet_payload["topic_entity_tag_source_id"] == 42
            assert tet_payload.get("entity") is None
        # Source row preserved (not deleted).
        db.delete.assert_not_called()

    @patch.object(mod, "create_tag", return_value={"status": "success"})
    @patch.object(mod.cross_reference_crud, "create", return_value=1)
    @patch.object(mod, "set_global_user_id")
    def test_null_entity_tets_still_get_topic_tet(
        self, mock_set_uid, mock_create, mock_create_tag,
    ):
        source = MagicMock(topic_entity_tag_source_id=42)
        tet_with = MagicMock(topic_entity_tag_id=1, reference_id=100, entity="1ABC")
        tet_null = MagicMock(topic_entity_tag_id=2, reference_id=200, entity=None)
        db = _make_db_with_tets(tets=[tet_with, tet_null], source=source)
        db.execute.return_value = MagicMock(rowcount=2)

        counts = mod.migrate(db=db)

        assert counts["migrated"] == 1
        assert counts["skipped_no_entity"] == 1
        assert counts["tets_deleted"] == 2
        # Both references (100 and 200) are touched -> 2 topic TETs.
        assert counts["topic_tet_created"] == 2
        assert mock_create_tag.call_count == 2
        assert mock_create.call_count == 1

    @patch.object(mod, "create_tag", return_value={"status": "success"})
    @patch.object(mod.cross_reference_crud, "create")
    @patch.object(mod, "set_global_user_id")
    def test_skips_existing_cross_reference(self, mock_set_uid, mock_create, mock_create_tag):
        source = MagicMock(topic_entity_tag_source_id=42)
        tet = MagicMock(topic_entity_tag_id=1, reference_id=100, entity="1ABC")
        existing = MagicMock(cross_reference_id=999)
        db = _make_db_with_tets(tets=[tet], source=source, existing_xref=existing)
        db.execute.return_value = MagicMock(rowcount=1)

        counts = mod.migrate(db=db)

        assert counts["migrated"] == 0
        assert counts["skipped_duplicate"] == 1
        assert counts["topic_tet_created"] == 1
        mock_create.assert_not_called()

    @patch.object(mod, "create_tag", return_value={"status": "success"})
    @patch.object(mod.cross_reference_crud, "create")
    @patch.object(mod, "set_global_user_id")
    def test_409_xref_counted_as_skipped_duplicate(self, mock_set_uid, mock_create, mock_create_tag):
        source = MagicMock(topic_entity_tag_source_id=42)
        tet = MagicMock(topic_entity_tag_id=1, reference_id=100, entity="1ABC")
        db = _make_db_with_tets(tets=[tet], source=source)
        db.execute.return_value = MagicMock(rowcount=1)
        mock_create.side_effect = HTTPException(status_code=409, detail="dup")

        counts = mod.migrate(db=db)

        assert counts["migrated"] == 0
        assert counts["skipped_duplicate"] == 1
        assert counts["errors"] == 0
        assert counts["topic_tet_created"] == 1

    @patch.object(mod, "create_tag", return_value={"status": "exists"})
    @patch.object(mod.cross_reference_crud, "create", return_value=1)
    @patch.object(mod, "set_global_user_id")
    def test_existing_topic_tet_counted_as_skipped_duplicate(
        self, mock_set_uid, mock_create, mock_create_tag,
    ):
        source = MagicMock(topic_entity_tag_source_id=42)
        tet = MagicMock(topic_entity_tag_id=1, reference_id=100, entity="1ABC")
        db = _make_db_with_tets(tets=[tet], source=source)
        db.execute.return_value = MagicMock(rowcount=1)

        counts = mod.migrate(db=db)

        assert counts["migrated"] == 1
        assert counts["topic_tet_created"] == 0
        assert counts["topic_tet_skipped_duplicate"] == 1

    @patch.object(mod, "create_tag", return_value={"status": "success"})
    @patch.object(mod.cross_reference_crud, "create")
    @patch.object(mod, "set_global_user_id")
    def test_errors_leave_tets_and_topic_tets_in_place(
        self, mock_set_uid, mock_create, mock_create_tag,
    ):
        source = MagicMock(topic_entity_tag_source_id=42)
        tet = MagicMock(topic_entity_tag_id=1, reference_id=100, entity="1ABC")
        db = _make_db_with_tets(tets=[tet], source=source)
        mock_create.side_effect = RuntimeError("boom")

        counts = mod.migrate(db=db)

        assert counts["errors"] == 1
        assert counts["migrated"] == 0
        # No deletes; no topic TET creates.
        assert counts["tets_deleted"] == 0
        assert counts["topic_tet_created"] == 0
        db.delete.assert_not_called()
        mock_create_tag.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
