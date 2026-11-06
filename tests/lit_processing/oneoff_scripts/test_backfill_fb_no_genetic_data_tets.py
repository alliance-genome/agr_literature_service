"""Tests for backfill_fb_no_genetic_data_tets.py (SCRUM-6670)."""
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from agr_literature_service.lit_processing.oneoff_scripts import backfill_fb_no_genetic_data_tets as mod
from ...fixtures import db  # noqa
from ...api.fixtures import auth_headers  # noqa
from ...api.test_reference import test_reference  # noqa
from ...api.test_mod import test_mod  # noqa
from ...api.test_ml_model import test_ml_model  # noqa


def _row(mit_id, reference_id, score, date_created=datetime(2026, 9, 1), date_updated=None):
    return {"manual_indexing_tag_id": mit_id, "reference_id": reference_id,
            "reference_curie": f"AGRKB:10100000000{reference_id:04d}", "confidence_score": score,
            "validation_by_biocurator": None, "note": None,
            "date_created": date_created, "date_updated": date_updated}


class TestConfidenceLevel:

    @pytest.mark.parametrize("score, level", [
        (0.0, "NEG"), (0.49, "NEG"), (0.5, "LOW"), (0.666, "LOW"), (0.667, "MEDIUM"),
        (0.832, "MEDIUM"), (0.833, "HIGH"), (1.0, "HIGH")])
    def test_matches_the_classifier_bands(self, score, level):
        assert mod.confidence_level(score) == level


class TestBuildPayload:

    def test_no_genetic_data_call_is_a_negated_tag(self):
        payload = mod.build_payload(_row(1, 7, 0.91), source_id=170)
        assert payload.topic == "ATP:0000402"
        assert payload.negated is True
        assert payload.confidence_score == 0.91
        assert payload.confidence_level == "HIGH"
        assert payload.species == "NCBITaxon:7214"
        assert payload.data_novelty == "ATP:0000335"
        assert payload.data_context == "ATP:0000405"
        assert payload.tag_source_id == 170
        assert payload.ml_model_id == 60
        assert payload.created_by == "default_user"
        assert payload.force_insertion is True

    def test_lowest_no_genetic_data_score_is_low(self):
        payload = mod.build_payload(_row(1, 7, 0.5), source_id=170)
        assert payload.negated is True
        assert payload.confidence_level == "LOW"

    def test_keeps_the_original_dates(self):
        # the weekly FB export reads only tags created in the last 6 days, so backfilled tags
        # must keep their own date or they are all sent again
        created = datetime(2025, 12, 1, 4, 0)
        payload = mod.build_payload(_row(1, 7, 0.7, date_created=created), source_id=170)
        assert payload.date_created == created
        assert payload.date_updated == created

    def test_rows_before_model_v3_get_model_46(self):
        assert mod.build_payload(_row(1, 7, 0.7, date_created=datetime(2026, 7, 28)), 170).ml_model_id == 46
        assert mod.build_payload(_row(1, 7, 0.7, date_created=datetime(2026, 7, 29)), 170).ml_model_id == 60


class TestPlan:

    def test_skips_tagged_references_rows_without_a_score_and_low_scores(self):
        rows = [_row(1, 1, 0.9), _row(2, 2, 0.95), _row(3, 3, None), _row(4, 4, 0.1)]
        todo = mod.plan(rows, already_tagged={2})
        assert [r["manual_indexing_tag_id"] for r in todo["create"]] == [1]
        assert [r["manual_indexing_tag_id"] for r in todo["skip_tagged"]] == [2]
        assert [r["manual_indexing_tag_id"] for r in todo["skip_no_score"]] == [3]
        # SCRUM-6670 comment 99232: "has genetic data" calls get no tag
        assert [r["manual_indexing_tag_id"] for r in todo["skip_low_score"]] == [4]

    def test_summary_counts_levels(self):
        summary = mod.summarize([_row(1, 1, 0.9), _row(2, 2, 0.6), _row(3, 3, 0.95)])
        assert summary == {"HIGH": 2, "LOW": 1}


class TestDeleteSourceRows:

    def test_deletes_tagged_and_low_score_rows_but_keeps_failed_ones(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 2
        # 30 scored >= 0.5 but has no tag (its create failed): kept for a re-run
        rows = [_row(10, 1, 0.9), _row(20, 2, 0.1), _row(30, 3, 0.8)]
        assert mod.delete_source_rows(session, rows, tagged={1}) == 2
        assert session.execute.call_args[0][1] == {"ids": [10, 20]}
        session.commit.assert_called_once()

    def test_nothing_to_delete(self):
        session = MagicMock()
        assert mod.delete_source_rows(session, [_row(10, 1, 0.9)], tagged=set()) == 0
        session.execute.assert_not_called()


class TestRun:

    def _patches(self, rows, tagged=frozenset()):
        return (patch.object(mod, "create_postgres_session", return_value=MagicMock()),
                patch.object(mod, "load_rows", return_value=rows),
                patch.object(mod, "find_source_id", return_value=170),
                patch.object(mod, "references_with_classifier_tag", return_value=set(tagged)),
                patch.object(mod, "set_global_user_id"),
                patch.object(mod, "create_tag"),
                patch.object(mod, "revalidate_all_tags"))

    def test_dry_run_writes_nothing(self):
        p = self._patches([_row(1, 1, 0.9), _row(2, 2, 0.1)])
        with p[0], p[1], p[2], p[3], p[4] as m_user, p[5] as m_create, p[6] as m_revalidate:
            counts = mod.run(commit=False, delete_source=False, limit=None, backup_file="unused.csv")
        assert counts == {"rows": 2, "to_create": 1, "skipped_tagged": 0, "skipped_no_score": 0,
                          "skipped_low_score": 1}
        m_user.assert_not_called()
        m_create.assert_not_called()
        m_revalidate.assert_not_called()

    def test_commit_creates_and_revalidates_each_reference(self):
        p = self._patches([_row(1, 1, 0.9), _row(2, 2, 0.7)])
        with p[0], p[1], p[2], p[3], p[4], p[5] as m_create, p[6] as m_revalidate:
            counts = mod.run(commit=True, delete_source=False, limit=None, backup_file="unused.csv")
        assert counts["created"] == 2
        assert [c.kwargs["validate_on_insert"] for c in m_create.call_args_list] == [False, False]
        assert [c.kwargs["curie_or_reference_id"] for c in m_revalidate.call_args_list] == ["1", "2"]

    def test_a_failed_tag_does_not_stop_the_rest(self):
        p = self._patches([_row(1, 1, 0.9), _row(2, 2, 0.7)])
        with p[0], p[1], p[2], p[3], p[4], p[5] as m_create, p[6]:
            m_create.side_effect = [Exception("boom"), (5, False)]
            counts = mod.run(commit=True, delete_source=False, limit=None, backup_file="unused.csv")
        assert counts["created"] == 1
        assert counts["failed"] == 1

    def test_delete_source_backs_up_every_row_first(self, tmp_path):
        backup = tmp_path / "rows.csv"
        p = self._patches([_row(1, 1, 0.9), _row(2, 2, 0.1)], tagged={1, 2})
        with p[0], p[1], p[2], p[3], p[4], p[5] as m_create, p[6], \
                patch.object(mod, "delete_source_rows", return_value=2) as m_delete:
            counts = mod.run(commit=False, delete_source=True, limit=None, backup_file=str(backup))
        m_create.assert_not_called()
        assert backup.read_text().count("\n") == 3  # header + 2 rows
        assert m_delete.called
        assert counts["deleted"] == 2
        assert counts["left_in_table"] == 0


class TestBackfillAgainstTheDatabase:
    """End to end on the test database: the tag is really created through create_tag with the
    row's date and the classifier's shape, and --delete-source removes the old row."""

    def test_row_becomes_a_tag_and_is_deleted(self, db, auth_headers, test_reference,  # noqa
                                              test_ml_model, tmp_path):  # noqa
        from starlette.testclient import TestClient
        from agr_literature_service.api.main import app
        from agr_literature_service.api.models import (ManualIndexingTagModel, ModModel,
                                                       ReferenceModel, TopicEntityTagModel)

        with TestClient(app) as client:
            client.post(url="/mod/", json={"abbreviation": "FB", "short_name": "FB",
                                           "full_name": "FlyBase"}, headers=auth_headers)
            source = client.post(url="/tag_source", headers=auth_headers, json={
                "source_evidence_assertion": "ECO:0008004", "source_method": "abc_document_classifier",
                "validation_type": None, "description": "classifier", "data_provider": "FB",
                "secondary_data_provider_abbreviation": "FB"})
            assert source.status_code == 201, source.json()
        fb = db.query(ModModel).filter_by(abbreviation="FB").one()
        reference_id = db.query(ReferenceModel).filter_by(curie=test_reference.new_ref_curie).one().reference_id
        created = datetime(2026, 9, 3, 4, 0)
        db.add(ManualIndexingTagModel(reference_id=reference_id, mod_id=fb.mod_id,
                                      curation_tag="ATP:0000207", confidence_score=0.91,
                                      date_created=created, date_updated=created))
        db.commit()

        with patch.object(mod, "create_postgres_session", return_value=db), \
                patch.object(mod, "ML_MODEL_V3_ID", test_ml_model["ml_model_id"]), \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.atp_return_invalid_ids",
                      return_value=[]):
            counts = mod.run(commit=True, delete_source=True, limit=None,
                             backup_file=str(tmp_path / "rows.csv"))

        assert counts["created"] == 1
        assert counts["deleted"] == 1
        tag = db.query(TopicEntityTagModel).filter_by(reference_id=reference_id, topic="ATP:0000402").one()
        assert tag.negated is True
        assert tag.confidence_level == "HIGH"
        assert tag.confidence_score == 0.91
        assert tag.date_created == created
        assert tag.data_context == "ATP:0000405"
        assert db.query(ManualIndexingTagModel).filter_by(reference_id=reference_id).count() == 0

    def test_low_score_row_gets_no_tag_and_is_deleted(self, db, auth_headers, test_reference,  # noqa
                                                      test_ml_model, tmp_path):  # noqa
        from starlette.testclient import TestClient
        from agr_literature_service.api.main import app
        from agr_literature_service.api.models import (ManualIndexingTagModel, ModModel,
                                                       ReferenceModel, TopicEntityTagModel)

        with TestClient(app) as client:
            client.post(url="/mod/", json={"abbreviation": "FB", "short_name": "FB",
                                           "full_name": "FlyBase"}, headers=auth_headers)
            client.post(url="/tag_source", headers=auth_headers, json={
                "source_evidence_assertion": "ECO:0008004", "source_method": "abc_document_classifier",
                "validation_type": None, "description": "classifier", "data_provider": "FB",
                "secondary_data_provider_abbreviation": "FB"})
        fb = db.query(ModModel).filter_by(abbreviation="FB").one()
        reference_id = db.query(ReferenceModel).filter_by(curie=test_reference.new_ref_curie).one().reference_id
        db.add(ManualIndexingTagModel(reference_id=reference_id, mod_id=fb.mod_id,
                                      curation_tag="ATP:0000207", confidence_score=0.02))
        db.commit()
        backup = tmp_path / "rows.csv"

        with patch.object(mod, "create_postgres_session", return_value=db), \
                patch.object(mod, "ML_MODEL_V3_ID", test_ml_model["ml_model_id"]), \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.atp_return_invalid_ids",
                      return_value=[]):
            counts = mod.run(commit=True, delete_source=True, limit=None, backup_file=str(backup))

        assert counts["to_create"] == 0
        assert counts["skipped_low_score"] == 1
        assert counts["deleted"] == 1
        assert db.query(TopicEntityTagModel).filter_by(reference_id=reference_id).count() == 0
        assert db.query(ManualIndexingTagModel).filter_by(reference_id=reference_id).count() == 0
        assert "0.02" in backup.read_text()
