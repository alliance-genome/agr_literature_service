"""
Add the missing FlyBase (FB) entity/gene extraction ``failed -> needed`` retry
transitions.

Background
----------
The weekly cron ``check_wft_in_progress.py`` (extraction phase) resets stuck
entity/gene extraction tags back to "needed" by calling
``job_change_atp_code(db, reference_workflow_tag_id, 'on_retry')``. That looks up
the matching ``WorkflowTransitionModel`` row for the tag with
``condition contains 'on_retry'`` and applies its ``transition_to`` + ``actions``.

FB has ``on_retry`` rows for the *in progress* extraction tags but **not** for the
*failed* ones:

    entity extraction in progress (ATP:0000190) -> needed (ATP:0000173)   present
    gene   extraction in progress (ATP:0000218) -> needed (ATP:0000220)   present
    entity extraction failed      (ATP:0000187) -> needed (ATP:0000173)   MISSING
    gene   extraction failed      (ATP:0000216) -> needed (ATP:0000220)   MISSING

So when a gene/entity extraction has actually *failed*, the cron's failed pass
calls ``on_retry`` on the failed tag, finds no matching row, and silently does
nothing (while the Slack report still claims it was set back to needed). The
paper stays stuck in the failed state. Classification, by contrast, has both
``failed -> needed`` and ``in progress -> needed`` retry rows -- confirming both
directions are intended.

This one-off adds the two missing ``failed -> needed`` ``on_retry`` rows for FB,
mirroring the existing in-progress rows' action (``sub_task_retry::entity
extraction``). It is idempotent (skips rows that already exist) and supports a
``--debug`` dry-run that also dumps every FB transition in the entity-extraction
tree for verification.
"""
import argparse
import logging

from agr_literature_service.api.crud.ateam_db_helpers import atp_get_all_descendants
from agr_literature_service.api.models import ModModel, WorkflowTransitionModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO)
logger = logging.getLogger()

# entity extraction ontology root (ATP:0000172 family), used for the debug dump.
ENTITY_EXTRACTION_PROCESS = 'ATP:0000172'

RETRY_CONDITION = 'on_retry'
RETRY_ACTIONS = ['sub_task_retry::entity extraction']

# Missing failed -> needed retry rows to create: (transition_from, transition_to, label)
FAILED_RETRY_ROWS = [
    ('ATP:0000187', 'ATP:0000173', 'entity extraction failed -> needed'),
    ('ATP:0000216', 'ATP:0000220', 'gene extraction failed -> needed'),
]


def get_fb_mod_id(db_session):
    mod = db_session.query(ModModel).filter(ModModel.abbreviation == 'FB').one_or_none()
    if mod is None:
        raise RuntimeError("Could not find mod with abbreviation 'FB'")
    return mod.mod_id


def dump_extraction_tree(db_session, fb_mod_id):
    """Log every FB transition touching the entity-extraction tree, for verification."""
    tree = set(atp_get_all_descendants(ENTITY_EXTRACTION_PROCESS, include_self=True))
    rows = db_session.query(WorkflowTransitionModel).filter(
        WorkflowTransitionModel.mod_id == fb_mod_id,
        (WorkflowTransitionModel.transition_from.in_(tree)
         | WorkflowTransitionModel.transition_to.in_(tree))).all()
    logger.info(f"All {len(rows)} FB transition(s) in the entity-extraction tree:")
    for row in sorted(rows, key=lambda r: (str(r.condition), r.transition_from)):
        logger.info(
            f"  {row.transition_from} -> {row.transition_to} "
            f"(condition={row.condition!r}, actions={row.actions!r})")


def add_fb_entity_extraction_failed_retry(db_session, debug=True):
    fb_mod_id = get_fb_mod_id(db_session)

    if debug:
        dump_extraction_tree(db_session, fb_mod_id)

    added = 0
    for transition_from, transition_to, label in FAILED_RETRY_ROWS:
        existing = db_session.query(WorkflowTransitionModel).filter(
            WorkflowTransitionModel.mod_id == fb_mod_id,
            WorkflowTransitionModel.condition.contains(RETRY_CONDITION),
            WorkflowTransitionModel.transition_from == transition_from,
            WorkflowTransitionModel.transition_to == transition_to).one_or_none()
        if existing is not None:
            logger.info(f"Already present, skipping: {label} ({transition_from} -> {transition_to})")
            continue

        logger.info(
            f"{'[debug] would add' if debug else 'Adding'}: {label} "
            f"({transition_from} -> {transition_to}, condition={RETRY_CONDITION}, actions={RETRY_ACTIONS})")
        if not debug:
            db_session.add(WorkflowTransitionModel(
                mod_id=fb_mod_id,
                condition=RETRY_CONDITION,
                actions=RETRY_ACTIONS,
                transition_from=transition_from,
                transition_to=transition_to,
                transition_type='any'))
            added += 1

    if debug:
        logger.info("Debug mode: no changes committed")
    else:
        db_session.commit()
        logger.info(f"Added {added} row(s)")


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-d', '--debug', help='run in debug mode, just print', action='store_true')
    args = parser.parse_args()
    db_session = create_postgres_session(False)
    add_fb_entity_extraction_failed_retry(db_session, debug=args.debug)


if __name__ == "__main__":
    main()
