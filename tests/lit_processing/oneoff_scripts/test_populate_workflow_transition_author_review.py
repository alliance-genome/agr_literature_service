"""SCRUM-6448: WB author review manual transitions (DB-free)."""
from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.oneoff_scripts import \
    populate_workflow_transition_author_review as script

STATES = ["ATP:0000389", "ATP:0000390", "ATP:0000391", "ATP:0000392"]


def _run(existing):
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = existing
    mod = MagicMock(mod_id=7, abbreviation="WB")
    with patch.object(script, "get_atp_id_by_name", side_effect=lambda name, fallback: fallback):
        added = script.populate_manual_transitions(db, mod)
    rows = [c.args[0] for c in db.add.call_args_list]
    return added, rows


def test_adds_all_to_all_manual_transitions_for_wb():
    added, rows = _run([])
    assert added == 12
    pairs = {(r.transition_from, r.transition_to) for r in rows}
    assert pairs == {(a, b) for a in STATES for b in STATES if a != b}
    assert {(r.mod_id, r.transition_type) for r in rows} == {(7, "manual_only")}


def test_is_idempotent():
    existing = [(a, b) for a in STATES for b in STATES if a != b]
    added, rows = _run(existing)
    assert (added, rows) == (0, [])
