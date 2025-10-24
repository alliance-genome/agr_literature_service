"""rename_novel_topic_qualifier2data_novelty

Revision ID: 5122e153906e
Revises: 19a726040ce5
Create Date: 2025-10-22 23:28:02.654584
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '5122e153906e'
down_revision = '19a726040ce5'
branch_labels = None
depends_on = None


def upgrade():
    # Rename column without losing data
    op.alter_column(
        'ml_model',
        'novel_topic_qualifier',
        new_column_name='data_novelty'
    )


def downgrade():
    # Revert column name back
    op.alter_column(
        'ml_model',
        'data_novelty',
        new_column_name='novel_topic_qualifier'
    )
