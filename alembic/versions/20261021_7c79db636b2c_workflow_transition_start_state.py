"""workflow_transition: START_STATE -> ATP:0000177

Revision ID: 7c79db636b2c
Revises: 80595e1e0d4d
Create Date: 2026-10-21 16:30:00.000000

SCRUM-6754: two ZFIN rows use the made-up value START_STATE as transition_from (812 -> ATP:0000306
indexing_priority_job, 824 -> ATP:0000380 abstract_classification_job). They are not real transitions:
the corpus code sets both tags directly, and the rows only exist so that get_jobs, which finds a job
through a transition whose transition_to is the paper's current tag, can see these two states.

START_STATE is not an ontology term, so it blocks the transition_from -> ontologyterm foreign key of
the A-team merge (SCRUM-6720). ATP:0000177 (workflow process, the root of every workflow process) takes
its place: no paper ever holds it and transitions match transition_from exactly, so the rows still never
fire. The history table is updated too, so every stored transition_from is an ATP id.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7c79db636b2c'
down_revision = '80595e1e0d4d'
branch_labels = None
depends_on = None

START_STATE = "START_STATE"
WORKFLOW_PROCESS = "ATP:0000177"


def _replace(old: str, new: str):
    conn = op.get_bind()
    for table in ("workflow_transition", "workflow_transition_version"):
        conn.execute(sa.text(f"UPDATE {table} SET transition_from = :new WHERE transition_from = :old"),  # noqa: S608
                     {"old": old, "new": new})


def upgrade():
    _replace(START_STATE, WORKFLOW_PROCESS)


def downgrade():
    # no transition used ATP:0000177 before this revision (checked in production on 2026-10-21)
    _replace(WORKFLOW_PROCESS, START_STATE)
