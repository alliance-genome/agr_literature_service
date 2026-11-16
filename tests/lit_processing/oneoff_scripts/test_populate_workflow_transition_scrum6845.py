"""SCRUM-6845: the transition rows the code needs (DB-free)."""
from unittest.mock import MagicMock

from agr_literature_service.lit_processing.oneoff_scripts import populate_workflow_transition_scrum6845 as script

MOD_IDS = {"AGR": 1, "FB": 2, "MGI": 3, "RGD": 4, "SGD": 5, "WB": 6, "XB": 7, "ZFIN": 8}


def _transition(mod_id, transition_from, transition_to, transition_type="any"):
    return MagicMock(mod_id=mod_id, transition_from=transition_from, transition_to=transition_to,
                     transition_type=transition_type)


def _db(existing):
    db = MagicMock()

    def query(*entities):
        result = MagicMock()
        if len(entities) == 2:  # ModModel.abbreviation, ModModel.mod_id
            result.all.return_value = list(MOD_IDS.items())
        else:
            result.all.return_value = existing
        return result

    db.query.side_effect = query
    return db


def _added(db):
    return {(row.mod_id, row.transition_from, row.transition_to, row.transition_type, row.condition)
            for row in (call.args[0] for call in db.add.call_args_list)}


def test_adds_the_rows_and_commits_only_with_apply():
    db = _db([])
    counts = script.populate(db, apply=False)
    db.commit.assert_not_called()
    db.rollback.assert_called_once()
    added = _added(db)
    assert counts["added"] == len(added) > 0
    # file upload first states for every MOD, AGR included
    assert (MOD_IDS["AGR"], "ATP:0000140", "ATP:0000141", "any", None) in added
    # manual indexing: from and back to the process term, for every MOD
    for mod_id in (MOD_IDS["FB"], MOD_IDS["MGI"]):
        assert (mod_id, "ATP:0000273", "ATP:0000274", "any", None) in added
        assert (mod_id, "ATP:0000343", "ATP:0000273", "any", None) in added
    # the email job connector is an action row, for the email MODs only
    assert (MOD_IDS["WB"], "ATP:0000354", "ATP:0000358", "action", "email_extraction_job") in added
    assert not any(row[0] == MOD_IDS["MGI"] and row[1] == "ATP:0000354" for row in added)
    # WB-only rows
    assert (MOD_IDS["WB"], "ATP:0000375", "ATP:0000378", "any", None) in added
    assert (MOD_IDS["WB"], "ATP:0000388", "ATP:0000389", "any", None) in added
    assert not any(row[1] == "ATP:0000375" and row[0] != MOD_IDS["WB"] for row in added)
    # the WF editor's clear: state -> process term, manual only, for the MODs that show the process
    assert (MOD_IDS["FB"], "ATP:0000331", "ATP:0000329", "manual_only", None) in added
    assert (MOD_IDS["WB"], "ATP:0000390", "ATP:0000388", "manual_only", None) in added
    assert (MOD_IDS["WB"], "ATP:0000231", "ATP:0000235", "manual_only", None) in added
    assert not any(row[2] == "ATP:0000235" and row[0] == MOD_IDS["SGD"] for row in added)

    db = _db([])
    script.populate(db, apply=True)
    db.commit.assert_called_once()


def test_existing_rows_are_kept_and_listed_types_change():
    review = _transition(MOD_IDS["WB"], "ATP:0000389", "ATP:0000392", "manual_only")
    zfin_job = _transition(MOD_IDS["ZFIN"], "ATP:0000177", "ATP:0000306")
    first_state = _transition(MOD_IDS["FB"], "ATP:0000140", "ATP:0000141", "manual_only")
    db = _db([review, zfin_job, first_state])
    counts = script.populate(db, apply=True)
    assert review.transition_type == "any"
    assert zfin_job.transition_type == "action"
    # a row that exists is never re-added nor retyped unless listed
    assert first_state.transition_type == "manual_only"
    assert (MOD_IDS["FB"], "ATP:0000140", "ATP:0000141", "any", None) not in _added(db)
    assert counts["type_changed"] == 2


def test_is_idempotent():
    db = _db([])
    script.populate(db, apply=False)
    existing = [_transition(row[0], row[1], row[2], row[3]) for row in _added(db)]
    existing += [_transition(MOD_IDS["WB"], "ATP:0000389", "ATP:0000392"),
                 _transition(MOD_IDS["WB"], "ATP:0000392", "ATP:0000389"),
                 _transition(MOD_IDS["ZFIN"], "ATP:0000177", "ATP:0000306", "action"),
                 _transition(MOD_IDS["ZFIN"], "ATP:0000177", "ATP:0000380", "action")]
    db = _db(existing)
    counts = script.populate(db, apply=False)
    assert counts["added"] == 0
    assert counts["type_changed"] == 0
