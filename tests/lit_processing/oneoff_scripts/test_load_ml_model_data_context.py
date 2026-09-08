"""Tests for load_ml_model_data_context.py (SCRUM-5697)."""

from unittest.mock import MagicMock

from agr_literature_service.lit_processing.oneoff_scripts import (
    load_ml_model_data_context as mod,
)


class TestDataContextFor:
    """The curators' rules, per comment 97488 and Ceri Van Slyke's 2026-09-01
    answer: WB splits by what the model produces, every other MOD takes
    experimentally studied data, and a model that creates no tags gets nothing."""

    def test_wb_topic_classifier_gets_the_root_term(self):
        assert mod.data_context_for("WB", "biocuration_topic_classification") == "ATP:0000323"

    def test_wb_entity_extractor_gets_experimentally_studied(self):
        assert mod.data_context_for("WB", "biocuration_entity_extraction") == "ATP:0000325"

    def test_fb_topic_classifier_gets_experimentally_studied(self):
        assert mod.data_context_for("FB", "biocuration_topic_classification") == "ATP:0000325"

    def test_zfin_topic_classifier_gets_experimentally_studied(self):
        assert mod.data_context_for("ZFIN", "biocuration_topic_classification") == "ATP:0000325"

    def test_zfin_pretriage_classifier_gets_experimentally_studied(self):
        assert mod.data_context_for(
            "ZFIN", "biocuration_pretriage_priority_classification") == "ATP:0000325"

    def test_an_unrecognised_task_type_is_left_unset(self):
        """Allow-list, not deny-list: a task type this script has never seen
        cannot be assumed to create tags, so it gets nothing rather than the
        default. backfill_ml_model_file_classes.py takes the same stance on the
        same table."""
        assert mod.data_context_for("WB", "biocuration_embedding_generation") is None
        assert mod.data_context_for("FB", "some_future_metadata_only_task") is None

    def test_vectorizer_is_left_unset(self):
        """A tfidf_vectorization row is an artifact, not a tag producer, so it
        has no data context to carry."""
        assert mod.data_context_for("WB", "tfidf_vectorization") is None


class TestPlanUpdates:
    """Planning is separated from writing so --dry-run reports exactly what the
    real run would do."""

    def _row(self, ml_model_id, abbreviation, task_type, data_context=None):
        return mod.ModelRow(ml_model_id=ml_model_id, abbreviation=abbreviation,
                            task_type=task_type, data_context=data_context)

    def test_plans_a_value_for_every_tag_producing_model(self):
        rows = [self._row(33, "WB", "biocuration_topic_classification"),
                self._row(29, "WB", "biocuration_entity_extraction"),
                self._row(56, "FB", "biocuration_topic_classification")]
        plan = mod.plan_updates(rows)
        assert plan.to_set == {33: "ATP:0000323", 29: "ATP:0000325", 56: "ATP:0000325"}
        assert plan.skipped_no_tags == []
        assert plan.already_correct == []
        assert plan.conflicts == []

    def test_leaves_vectorizer_rows_alone(self):
        rows = [self._row(28, "WB", "tfidf_vectorization")]
        plan = mod.plan_updates(rows)
        assert plan.to_set == {}
        assert plan.skipped_no_tags == [28]

    def test_a_row_that_already_holds_the_right_term_is_not_rewritten(self):
        rows = [self._row(33, "WB", "biocuration_topic_classification",
                          data_context="ATP:0000323")]
        plan = mod.plan_updates(rows)
        assert plan.to_set == {}
        assert plan.already_correct == [33]

    def test_a_row_holding_a_different_term_is_reported_not_overwritten(self):
        """Never silently change a value a curator or an earlier run chose: a
        disagreement is a decision, so report it and leave the row as it is."""
        rows = [self._row(33, "WB", "biocuration_topic_classification",
                          data_context="ATP:0000325")]
        plan = mod.plan_updates(rows)
        assert plan.to_set == {}
        assert plan.conflicts == [(33, "ATP:0000325", "ATP:0000323")]

    def test_a_vectorizer_that_somehow_has_a_value_is_a_conflict(self):
        rows = [self._row(28, "WB", "tfidf_vectorization", data_context="ATP:0000325")]
        plan = mod.plan_updates(rows)
        assert plan.to_set == {}
        assert plan.conflicts == [(28, "ATP:0000325", None)]


class TestApplyUpdates:

    def test_dry_run_writes_nothing(self):
        db = MagicMock()
        written = mod.apply_updates(db, {33: "ATP:0000323"}, dry_run=True)
        assert written == 0
        db.execute.assert_not_called()
        db.commit.assert_not_called()

    def test_real_run_updates_each_row_and_commits_once(self):
        db = MagicMock()
        db.execute.return_value.rowcount = 1
        written = mod.apply_updates(db, {33: "ATP:0000323", 29: "ATP:0000325"}, dry_run=False)
        assert written == 2
        assert db.execute.call_count == 2
        params = [call[0][1] for call in db.execute.call_args_list]
        assert {"ml_model_id": 33, "data_context": "ATP:0000323"} in params
        assert {"ml_model_id": 29, "data_context": "ATP:0000325"} in params
        db.commit.assert_called_once()

    def test_only_null_rows_are_targeted_by_the_update(self):
        """The UPDATE carries its own data_context IS NULL guard, so a
        concurrent writer cannot be clobbered between plan and apply."""
        db = MagicMock()
        db.execute.return_value.rowcount = 1
        mod.apply_updates(db, {33: "ATP:0000323"}, dry_run=False)
        sql = str(db.execute.call_args[0][0])
        assert "data_context IS NULL" in sql
