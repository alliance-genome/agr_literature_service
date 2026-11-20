"""SCRUM-6754: replace the START_STATE placeholder of workflow_transition.transition_from with ATP:0000177.

Two ZFIN rows (812 -> ATP:0000306 indexing_priority_job, 824 -> ATP:0000380 abstract_classification_job)
use the made-up value START_STATE as transition_from. They are not real transitions: the corpus code sets
both tags directly, and the rows only exist so that get_jobs, which finds a job through a transition whose
transition_to is the paper's current tag, can see these two states. START_STATE is not an ontology term, so
it blocks the transition_from -> ontologyterm foreign key of the A-team merge (SCRUM-6720).

ATP:0000177 (workflow process, the root of every workflow process) takes its place: no paper ever holds it
and transitions match transition_from exactly, so the rows still never fire.

The rows are updated through the ORM, so Continuum records the change in workflow_transition_version with
this script as the author; earlier history rows keep START_STATE, as the record of what was there.

Usage (idempotent; run on each database, e.g. dev then prod):
    python3 SCRUM-6754_replace_start_state.py            # dry run: lists the rows
    python3 SCRUM-6754_replace_start_state.py --apply    # update them
"""
import argparse
import logging
from os import environ, path

from agr_literature_service.api.models import WorkflowTransitionModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

START_STATE = "START_STATE"
WORKFLOW_PROCESS = "ATP:0000177"


def main():
    parser = argparse.ArgumentParser(description="SCRUM-6754: START_STATE -> ATP:0000177 in workflow_transition")
    parser.add_argument("--apply", action="store_true", help="commit the change; without it nothing is written")
    args = parser.parse_args()

    db = create_postgres_session(False)
    set_global_user_id(db, path.basename(__file__).replace(".py", ""))
    logger.info(f"database {environ.get('PSQL_HOST', 'localhost')}/{environ.get('PSQL_DATABASE', '')}")

    rows = db.query(WorkflowTransitionModel).filter(WorkflowTransitionModel.transition_from == START_STATE).all()
    for row in rows:
        logger.info(f"  transition {row.workflow_transition_id}: {row.transition_from} -> {row.transition_to} "
                    f"(mod_id {row.mod_id}, condition {row.condition})")
    if not rows:
        logger.info("no START_STATE rows: nothing to do")
        return
    if not args.apply:
        logger.info(f"dry run: {len(rows)} rows would get transition_from {WORKFLOW_PROCESS}; pass --apply")
        return
    for row in rows:
        row.transition_from = WORKFLOW_PROCESS
    db.commit()
    logger.info(f"updated {len(rows)} rows to transition_from {WORKFLOW_PROCESS}")


if __name__ == "__main__":
    main()
