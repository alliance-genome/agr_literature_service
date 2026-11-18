"""SCRUM-6845: "email extraction needed" back-fill for corpus entries since 2024 (DB-free)."""
from unittest.mock import MagicMock

from agr_literature_service.lit_processing.oneoff_scripts import backfill_email_extraction_needed_scrum6845 as script


def _db(pairs):
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = pairs
    return db


def test_dry_run_counts_per_mod_and_rolls_back():
    db = _db([(1, 10, "FB"), (2, 10, "FB"), (3, 20, "ZFIN")])
    assert script.backfill(db, commit=False) == {"FB": 2, "ZFIN": 1}
    db.rollback.assert_called_once()
    db.commit.assert_not_called()
    params = db.execute.call_args.args[1]
    assert params["since"] == "2024-01-01"
    assert set(params["mods"]) == {"FB", "SGD", "WB", "XB", "ZFIN"}
    assert "ATP:0000358" in params["states"] and "ATP:0000355" in params["states"]


def test_commit_adds_needed_tags():
    db = _db([(1, 10, "WB")])
    script.backfill(db, commit=True)
    db.commit.assert_called_once()
    tag = db.add.call_args.args[0]
    assert (tag.reference_id, tag.mod_id, tag.workflow_tag_id) == (1, 10, "ATP:0000358")
