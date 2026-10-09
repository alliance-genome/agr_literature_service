"""
Populate the workflow_transition table with the WormBase "author-person
curation" transitions (SCRUM-6487).

author-person curation (ATP:0000375)
    author-person curation needed (ATP:0000109)
    author-person curation in progress (ATP:0000377)
    author-person curation blocked (ATP:0000376)
    author-person curation complete (ATP:0000378)

"needed" is granted when a WB curator sorts a reference inside the corpus with
the "Author-Person curation" checkbox checked (api.crud.utils.corpus_entry_tags);
from there a curator moves the paper by hand, so this inserts the all-to-all manual_only transitions among the four
states, for the WB mod only. It is idempotent: transitions already present are
left untouched.

    python3 populate_workflow_transition_author_person_curation.py
"""
import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModModel, WorkflowTransitionModel
from agr_literature_service.api.crud.workflow_tag_crud import get_atp_id_by_name

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

AUTHOR_PERSON_CURATION_MOD = 'WB'

# The four author-person curation states, resolved from the A-Team ontology by
# name via get_atp_id_by_name with the ATP ids kept only as fallbacks.
AUTHOR_PERSON_CURATION_STATE_NAME_TO_FALLBACK = {
    'author-person curation needed': 'ATP:0000109',
    'author-person curation in progress': 'ATP:0000377',
    'author-person curation blocked': 'ATP:0000376',
    'author-person curation complete': 'ATP:0000378',
}


def populate_manual_transitions(db_session, mod) -> int:
    """All-to-all manual_only transitions among the four author-person curation
    states. Returns the number of transitions added."""
    state_atps = [
        get_atp_id_by_name(name, fallback=fallback)
        for name, fallback in AUTHOR_PERSON_CURATION_STATE_NAME_TO_FALLBACK.items()
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
    log.info(f"Added {added} manual author-person curation transitions for {mod.abbreviation}.")
    return added


def populate_data():  # pragma: no cover
    db_session = create_postgres_session(False)
    try:
        mod = db_session.query(ModModel).filter(
            ModModel.abbreviation == AUTHOR_PERSON_CURATION_MOD
        ).one_or_none()
        if mod is None:
            log.error(f"Mod {AUTHOR_PERSON_CURATION_MOD} not found; nothing to do.")
            return
        populate_manual_transitions(db_session, mod)
        db_session.commit()
        log.info("Done.")
    except Exception as e:
        db_session.rollback()
        log.error(f"Error occurred: {e}")
    finally:
        db_session.close()


if __name__ == "__main__":  # pragma: no cover
    populate_data()
