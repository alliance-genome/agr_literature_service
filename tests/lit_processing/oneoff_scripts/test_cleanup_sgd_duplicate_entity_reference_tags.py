"""Tests for cleanup_sgd_duplicate_entity_reference_tags.py"""

from unittest.mock import MagicMock, patch

import pytest

from agr_literature_service.lit_processing.oneoff_scripts import (
    cleanup_sgd_duplicate_entity_reference_tags as mod,
)


class TestDuplicateLabel:

    def test_entity_duplicate_labels_as_entity_type_and_curie(self):
        assert mod._duplicate_label("ATP:0000005", "SGD:S1", None) == \
            ("gene", "SGD:S1")

    def test_topic_only_duplicate_labels_by_display_tag(self):
        assert mod._duplicate_label(None, None, "ATP:0000130") == \
            ("topic-only", "display:ATP:0000130")


class TestFindDuplicateTags:

    def test_sql_covers_both_tag_shapes(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = [
            (11, 1, "AGRKB:1", "ATP:0000005", "SGD:S1", None),
            (12, 1, "AGRKB:1", None, None, "ATP:0000148"),
        ]
        rows = mod.find_duplicate_tags(db, [230])
        assert rows == [
            (11, 1, "AGRKB:1", "ATP:0000005", "SGD:S1", None),
            (12, 1, "AGRKB:1", None, None, "ATP:0000148"),
        ]
        sql, params = db.execute.call_args[0]
        sql = str(sql)
        # pure entity arm: matched on entity_type/species/entity
        assert "sgd.topic = sgd.entity_type" in sql
        assert "abc.entity = sgd.entity" in sql
        # topic-only arm: root topic, no entity, matched on display_tag
        assert "sgd.topic = :root_topic" in sql
        assert "sgd.entity IS NULL" in sql
        assert "abc.display_tag = sgd.display_tag" in sql
        assert params["root_topic"] == mod.ROOT_TOPIC_ATP
        assert params["sids"] == [230]


class TestCleanupRun:

    def _patches(self, duplicates):
        return [
            patch.object(mod, "write_id_log"),
            patch.object(mod, "count_affected_dataset_entries", return_value=0),
            patch.object(mod, "find_duplicate_tags", return_value=duplicates),
            patch.object(mod, "find_sgd_source_ids", return_value=[230]),
            patch.object(mod, "set_global_user_id"),
        ]

    def test_dry_run_reports_without_deleting(self):
        duplicates = [
            (11, 1, "AGRKB:1", "ATP:0000005", "SGD:S1", None),
            (12, 2, "AGRKB:2", None, None, "ATP:0000130"),
        ]
        db = MagicMock()
        with patch.object(mod, "create_postgres_session", return_value=db):
            patches = self._patches(duplicates)
            for p in patches:
                p.start()
            try:
                counts = mod.cleanup_sgd_duplicate_entity_reference_tags(delete=False)
            finally:
                for p in patches:
                    p.stop()
        assert counts["duplicates"] == 2
        assert counts["by_type"] == {"gene": 1, "topic-only": 1}
        assert counts["affected_references"] == 2
        db.delete.assert_not_called()

    @patch.object(mod, "revalidate_all_tags")
    def test_delete_removes_tags_and_revalidates_per_reference(self, mock_revalidate):
        duplicates = [
            (11, 1, "AGRKB:1", "ATP:0000005", "SGD:S1", None),
            (12, 1, "AGRKB:1", None, None, "ATP:0000130"),
        ]
        db = MagicMock()
        tag = MagicMock()
        db.query.return_value.filter_by.return_value.one_or_none.return_value = tag
        with patch.object(mod, "create_postgres_session", return_value=db):
            patches = self._patches(duplicates)
            for p in patches:
                p.start()
            try:
                counts = mod.cleanup_sgd_duplicate_entity_reference_tags(delete=True)
            finally:
                for p in patches:
                    p.stop()
        assert counts["duplicates"] == 2
        assert db.delete.call_count == 2
        db.commit.assert_called_once()  # one commit for the single reference
        mock_revalidate.assert_called_once_with(curie_or_reference_id="1")

    def test_missing_source_short_circuits(self):
        db = MagicMock()
        with patch.object(mod, "create_postgres_session", return_value=db), \
                patch.object(mod, "set_global_user_id"), \
                patch.object(mod, "find_sgd_source_ids", return_value=[]):
            counts = mod.cleanup_sgd_duplicate_entity_reference_tags(delete=True)
        assert counts.get("source_missing") is True
        assert "nothing to clean up" in mod.compose_report_message(counts)


class TestComposeReportMessage:

    def test_lists_both_shapes(self):
        counts = {
            "deleted": False, "duplicates": 2,
            "by_type": {"gene": 1, "topic-only": 1},
            "affected_references": 2, "dataset_entries_affected": 0,
            "duplicate_rows": [
                (11, 1, "AGRKB:1", "ATP:0000005", "SGD:S1", None),
                (12, 2, "AGRKB:2", None, None, "ATP:0000130"),
            ],
        }
        msg = mod.compose_report_message(counts)
        assert "DRY-RUN" in msg
        assert "AGRKB:1\tgene\tSGD:S1" in msg
        assert "AGRKB:2\ttopic-only\tdisplay:ATP:0000130" in msg


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
