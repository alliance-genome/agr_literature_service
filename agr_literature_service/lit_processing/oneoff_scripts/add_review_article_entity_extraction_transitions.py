"""
Run entity extraction on review articles for FB and WB (SCRUM-6611).

Entity extraction is seeded when a paper's text conversion completes
(text conversion -> file converted to text, ATP:0000163), by a
``proceed_on_value`` action that only fires for research articles (FB) or
Experimental papers (WB). Text conversion itself is seeded on file upload
(-> files uploaded, ATP:0000134) under the same condition, so a review article
never reaches the entity extraction step unless both are extended.

For FB and WB this appends a ``proceed_on_value::category::review_article::...``
action next to the existing research_article / Experimental one on:

    file upload transitions      (-> ATP:0000134)  seed text conversion (ATP:0000162)
    text conversion transitions  (-> ATP:0000163)  seed entity extraction

A WB paper that is both Experimental and a review article is seeded once:
proceed_on_value skips tags an earlier action of the same transition added.

Rows are matched by mod, transition_from and transition_to (ids differ per
environment) and must already carry the existing action, so an unexpected row
is reported rather than changed. Idempotent; run once per environment.
Only transitions are changed: references already past these steps are not
backfilled.

    python3 add_review_article_entity_extraction_transitions.py --dry-run
    python3 add_review_article_entity_extraction_transitions.py
"""
import argparse
import logging

from agr_literature_service.api.models import ModModel, WorkflowTransitionModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

FILES_UPLOADED = 'ATP:0000134'
TEXT_CONVERSION_NEEDED = 'ATP:0000162'
FILE_CONVERTED_TO_TEXT = 'ATP:0000163'

FILE_UPLOAD_FROM = ['ATP:0000135', 'ATP:0000139', 'ATP:0000141']
TEXT_CONVERSION_FROM = [TEXT_CONVERSION_NEEDED, 'ATP:0000198']

REVIEW_TEXT_CONVERSION = f"proceed_on_value::category::review_article::{TEXT_CONVERSION_NEEDED}"
REVIEW_ENTITY_EXTRACTION = "proceed_on_value::category::review_article::entity extraction"

# (mod, transition_from list, transition_to, action to add, existing actions; the row needs one of them)
CHANGES = [
    ('FB', FILE_UPLOAD_FROM, FILES_UPLOADED, REVIEW_TEXT_CONVERSION,
     [f"proceed_on_value::category::research_article::{TEXT_CONVERSION_NEEDED}"]),
    ('FB', TEXT_CONVERSION_FROM, FILE_CONVERTED_TO_TEXT, REVIEW_ENTITY_EXTRACTION,
     ["proceed_on_value::category::research_article::entity extraction"]),
    # WB checks its own reference type, except on the upload-in-progress row (ATP:0000135),
    # which carries the research_article category check.
    ('WB', FILE_UPLOAD_FROM, FILES_UPLOADED, REVIEW_TEXT_CONVERSION,
     [f"proceed_on_value::reference_type::Experimental::{TEXT_CONVERSION_NEEDED}",
      f"proceed_on_value::category::research_article::{TEXT_CONVERSION_NEEDED}"]),
    ('WB', TEXT_CONVERSION_FROM, FILE_CONVERTED_TO_TEXT, REVIEW_ENTITY_EXTRACTION,
     ["proceed_on_value::reference_type::Experimental::entity extraction"]),
]


def add_review_actions(db_session, mod, transition_froms, transition_to, new_action, existing_actions) -> int:
    """Append new_action to the mod's transitions into transition_to. Returns the number changed."""
    changed = 0
    for transition_from in transition_froms:
        label = f"({mod.abbreviation}, {transition_from} -> {transition_to})"
        rows = db_session.query(WorkflowTransitionModel).filter(
            WorkflowTransitionModel.mod_id == mod.mod_id,
            WorkflowTransitionModel.transition_from == transition_from,
            WorkflowTransitionModel.transition_to == transition_to
        ).all()
        if not rows:
            log.warning(f"{label} not in db; skipping")
            continue
        for row in rows:
            actions = list(row.actions or [])
            if new_action in actions:
                log.info(f"{label} id {row.workflow_transition_id} already has {new_action}")
                continue
            if not any(action in actions for action in existing_actions):
                log.warning(f"{label} id {row.workflow_transition_id} has unexpected actions {actions}; skipping")
                continue
            row.actions = actions + [new_action]
            changed += 1
            log.info(f"{label} id {row.workflow_transition_id}: adding {new_action}")
    return changed


def populate_data(dry_run):  # pragma: no cover
    db_session = create_postgres_session(False)
    try:
        changed = 0
        for mod_abbreviation, transition_froms, transition_to, new_action, existing_actions in CHANGES:
            mod = db_session.query(ModModel).filter(
                ModModel.abbreviation == mod_abbreviation
            ).one_or_none()
            if mod is None:
                log.error(f"Mod {mod_abbreviation} not found; skipping.")
                continue
            changed += add_review_actions(db_session, mod, transition_froms, transition_to,
                                          new_action, existing_actions)
        if dry_run:
            db_session.rollback()
            log.info(f"Dry run: {changed} transitions would be changed; nothing written.")
        else:
            db_session.commit()
            log.info(f"Done: {changed} transitions changed.")
    except Exception as e:
        db_session.rollback()
        log.error(f"Error occurred: {e}")
    finally:
        db_session.close()


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', action='store_true', help='report the changes without writing them')
    populate_data(parser.parse_args().dry_run)
