"""ml_model_decision_threshold

Revision ID: 2b4c3070f84a
Revises: 9923dad57b42
Create Date: 2026-11-11

Adds two ml_model columns (SCRUM-6482):

- decision_threshold: the positive-class score from which the document classifier
  calls a paper positive. NULL means the default 0.5.
- rescale_confidence: whether the classifier rescales the tag's confidence score so
  that decision_threshold maps to 0.5 (default), or keeps the model's own score.

Sets the ZFIN molecular probe models (ATP:0000370) to 0.87, the cut-off the
classifier used to hard-code for them (SCRUM-5764), with the model's own score kept
on the tags, as ZFIN curators see it today.
"""
from alembic import op
import sqlalchemy as sa


revision = "2b4c3070f84a"
down_revision = "9923dad57b42"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('ml_model', sa.Column('decision_threshold', sa.Float(), nullable=True))
    op.add_column('ml_model', sa.Column('rescale_confidence', sa.Boolean(), nullable=False,
                                        server_default=sa.text('true')))
    op.execute("""
        UPDATE ml_model
           SET decision_threshold = 0.87,
               rescale_confidence = false
          FROM mod
         WHERE mod.id = ml_model.mod_id
           AND mod.abbreviation = 'ZFIN'
           AND ml_model.topic = 'ATP:0000370'
    """)


def downgrade():
    op.drop_column('ml_model', 'rescale_confidence')
    op.drop_column('ml_model', 'decision_threshold')
