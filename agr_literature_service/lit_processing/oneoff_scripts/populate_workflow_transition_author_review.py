"""
Populate the workflow_transition table with the "author review" transitions
(SCRUM-6448).

author review (ATP:0000388)
    author review needed (ATP:0000389)
    author review in progress (ATP:0000390)
    author review blocked (ATP:0000391)
    author review complete (ATP:0000392)

The PubMed update sets "needed" and "complete" itself (api.crud.utils.author_review);
curators move papers between the states by hand, so this inserts the all-to-all
manual_only transitions among the four states, for each MOD in AUTHOR_REVIEW_MODS
(WB). It is idempotent: transitions already present are left untouched.

    python3 populate_workflow_transition_author_review.py
"""
import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModModel, WorkflowTransitionModel
from agr_literature_service.api.crud.workflow_tag_crud import get_atp_id_by_name
from agr_literature_service.api.crud.utils.author_review import AUTHOR_REVIEW_MODS, \
    AUTHOR_REVIEW_STATE_NAME_TO_ATP

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


def populate_manual_transitions(db_session, mod) -> int:
    """All-to-all manual_only transitions among the four author review states.
    Returns the number of transitions added."""
    state_atps = [
        get_atp_id_by_name(name, fallback=fallback)
        for name, fallback in AUTHOR_REVIEW_STATE_NAME_TO_ATP.items()
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
    log.info(f"Added {added} manual author review transitions for {mod.abbreviation}.")
    return added


def populate_data():  # pragma: no cover
    db_session = create_postgres_session(False)
    try:
        for mod_abbreviation in AUTHOR_REVIEW_MODS:
            mod = db_session.query(ModModel).filter(
                ModModel.abbreviation == mod_abbreviation
            ).one_or_none()
            if mod is None:
                log.error(f"Mod {mod_abbreviation} not found; skipping.")
                continue
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
