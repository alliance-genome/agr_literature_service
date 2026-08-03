"""Tests for cleanup_zfin_over_cap_reference_tags.py"""

from unittest.mock import MagicMock, call, patch

import pytest

from agr_literature_service.lit_processing.oneoff_scripts import (
    cleanup_zfin_over_cap_reference_tags as mod,
)


def _chained_query(all_return):
    """A MagicMock db whose query(...).filter(...).group_by(...).having(...)
    .order_by(...).all() (and .filter(...).all()) resolves to ``all_return``."""
    q = MagicMock()
    for method in ("filter", "group_by", "having", "order_by"):
        getattr(q, method).return_value = q
    q.all.return_value = all_return
    db = MagicMock()
    db.query.return_value = q
    return db, q


class TestFindOverCapReferences:

    def test_returns_reference_id_and_count_tuples(self):
        db, _q = _chained_query([(11, 300), (22, 260)])
        assert mod.find_over_cap_references(db, 229, mod.GENE_ATP) == [(11, 300), (22, 260)]

    def test_empty_when_no_paper_over_cap(self):
        db, _q = _chained_query([])
        assert mod.find_over_cap_references(db, 229, mod.ALLELE_ATP) == []


class TestDeleteReferenceTags:

    def test_deletes_each_tag_and_returns_count(self):
        tag1, tag2 = MagicMock(), MagicMock()
        db, _q = _chained_query([tag1, tag2])
        deleted = mod.delete_reference_tags(db, 229, mod.GENE_ATP, 11)
        assert deleted == 2
        db.delete.assert_has_calls([call(tag1), call(tag2)], any_order=True)


class TestComposeReportMessage:

    def test_source_missing(self):
        msg = mod.compose_report_message({"source_missing": True})
        assert "source not found" in msg

    def test_dry_run_mode_and_counts(self):
        msg = mod.compose_report_message({
            "deleted": False, "gene_papers": 2, "gene_tags": 700,
            "allele_papers": 1, "allele_tags": 300,
        })
        assert "DRY-RUN" in msg
        assert "Gene: 2 papers over cap, 700 tags" in msg
        assert "Allele: 1 papers over cap, 300 tags" in msg

    def test_delete_mode_shows_revalidated_count(self):
        msg = mod.compose_report_message({
            "deleted": True, "gene_papers": 1, "gene_tags": 300,
            "allele_papers": 0, "allele_tags": 0, "affected_references": 1,
        })
        assert "DELETED" in msg
        assert "References revalidated: 1" in msg


class TestCleanupOrchestration:

    @patch.object(mod, "revalidate_all_tags")
    @patch.object(mod, "delete_reference_tags")
    @patch.object(mod, "find_over_cap_references")
    @patch.object(mod, "find_zfin_source_id", return_value=229)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    def test_dry_run_reports_without_deleting(
            self, mock_session, _user, _src, mock_find, mock_delete, mock_reval):
        mock_session.return_value = MagicMock()
        # gene: two over-cap papers; allele: one.
        mock_find.side_effect = [[(11, 300), (22, 260)], [(33, 251)]]
        counts = mod.cleanup_zfin_over_cap_reference_tags(delete=False)
        assert counts["gene_papers"] == 2
        assert counts["gene_tags"] == 560
        assert counts["allele_papers"] == 1
        assert counts["allele_tags"] == 251
        assert counts["deleted"] is False
        mock_delete.assert_not_called()
        mock_reval.assert_not_called()

    @patch.object(mod, "revalidate_all_tags")
    @patch.object(mod, "delete_reference_tags", return_value=300)
    @patch.object(mod, "find_over_cap_references")
    @patch.object(mod, "find_zfin_source_id", return_value=229)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    def test_delete_removes_tags_and_revalidates_each_reference(
            self, mock_session, _user, _src, mock_find, mock_delete, mock_reval):
        db = MagicMock()
        mock_session.return_value = db
        mock_find.side_effect = [[(11, 300)], [(11, 300)]]  # ref 11 over cap for both types
        counts = mod.cleanup_zfin_over_cap_reference_tags(delete=True)
        assert counts["deleted"] is True
        # One over-cap paper per type -> two deletes, both on reference 11.
        assert mock_delete.call_count == 2
        db.commit.assert_called()
        # Reference 11 is affected by both types but revalidated once.
        assert counts["affected_references"] == 1
        mock_reval.assert_called_once_with(curie_or_reference_id="11")

    @patch.object(mod, "revalidate_all_tags")
    @patch.object(mod, "delete_reference_tags", return_value=300)
    @patch.object(mod, "find_over_cap_references")
    @patch.object(mod, "find_zfin_source_id", return_value=229)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    def test_no_revalidate_flag_skips_revalidation(
            self, mock_session, _user, _src, mock_find, _mock_delete, mock_reval):
        mock_session.return_value = MagicMock()
        mock_find.side_effect = [[(11, 300)], []]
        mod.cleanup_zfin_over_cap_reference_tags(delete=True, revalidate=False)
        mock_reval.assert_not_called()

    @patch.object(mod, "find_zfin_source_id", return_value=None)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "create_postgres_session")
    def test_missing_source_is_a_noop(self, mock_session, _user, _src):
        mock_session.return_value = MagicMock()
        counts = mod.cleanup_zfin_over_cap_reference_tags(delete=True)
        assert counts.get("source_missing") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
