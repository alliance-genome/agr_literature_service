"""
Populate the workflow_transition table with the FlyBase "first pass curation"
transitions (SCRUM-5478).

first pass curation (ATP:0000329)
    first pass curation needed (ATP:0000331)
    first pass curation in progress (ATP:0000332)
    first pass curation blocked (ATP:0000333)
    first pass curation TBD (ATP:0000371)
    first pass curation finished (ATP:0000330)

First pass curation is a manually applied workflow tag, so a curator can move a paper
between any of its status states. This inserts the all-to-all manual_only transitions
among the five first pass curation status states, for the FB mod only. It is idempotent:
transitions already present are left untouched.

    python3 populate_workflow_transition_first_pass_curation.py

Note: the automated "first pass curation TBD" (ATP:0000371) tag is NOT wired via a
transition row. Entity extraction (the last pre-curation step) reaches its "complete"
state through the subtask roll-up in sub_task_complete, which sets the main tag directly
rather than via a transition -- so a transition action would never fire. The roll-up
instead calls set_first_pass_curation_tbd directly (see subtask_process.py).
"""
import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModModel, WorkflowTransitionModel
from agr_literature_service.api.crud.workflow_tag_crud import get_atp_id_by_name

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

FIRST_PASS_CURATION_MOD = 'FB'

# The five applyable first pass curation status states (FlyBase only), resolved from the
# A-Team ontology by name via get_atp_id_by_name with the ATP ids kept only as fallbacks.
FIRST_PASS_CURATION_STATE_NAME_TO_FALLBACK = {
    'first pass curation needed': 'ATP:0000331',
    'first pass curation in progress': 'ATP:0000332',
    'first pass curation blocked': 'ATP:0000333',
    'first pass curation TBD': 'ATP:0000371',
    'first pass curation finished': 'ATP:0000330',
}


def populate_manual_transitions(db_session, mod):
    """All-to-all manual_only transitions among the five first pass curation states."""
    state_atps = [
        get_atp_id_by_name(name, fallback=fallback)
        for name, fallback in FIRST_PASS_CURATION_STATE_NAME_TO_FALLBACK.items()
    ]
    existing = set(
        db_session.query(
            WorkflowTransitionModel.transition_from,
            WorkflowTransitionModel.transition_to
        ).filter(WorkflowTransitionModel.mod_id == mod.mod_id).all()
    )
    added = 0
    for from_term in state_atps:
        for to_term in state_atps:
            if from_term == to_term:
                continue
            if (from_term, to_term) in existing:
                log.info(f"({mod.abbreviation}, {from_term}, {to_term}) already in db")
                continue
            db_session.add(WorkflowTransitionModel(
                mod_id=mod.mod_id,
                transition_from=from_term,
                transition_to=to_term,
                transition_type='manual_only',
                actions=[],
            ))
            added += 1
            log.info(f"adding manual transition ({mod.abbreviation}, {from_term}, {to_term})")
    log.info(f"Added {added} manual first pass curation transitions for {mod.abbreviation}.")


def populate_data():
    db_session = create_postgres_session(False)
    try:
        mod = db_session.query(ModModel).filter(
            ModModel.abbreviation == FIRST_PASS_CURATION_MOD
        ).one_or_none()
        if mod is None:
            log.error(f"Mod {FIRST_PASS_CURATION_MOD} not found; nothing to do.")
            return
        populate_manual_transitions(db_session, mod)
        db_session.commit()
        log.info("Done.")
    except Exception as e:
        db_session.rollback()
        log.error(f"Error occurred: {e}")
    finally:
        db_session.close()


if __name__ == "__main__":
    populate_data()
