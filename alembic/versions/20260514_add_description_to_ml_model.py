"""add description column to ml_model

Revision ID: 9f8e7d6c5b4a
Revises: 0a43280cf638
Create Date: 2026-05-14

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9f8e7d6c5b4a'
down_revision = '0a43280cf638'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('ml_model', sa.Column('description', sa.String(), nullable=True))


def downgrade():
    op.drop_column('ml_model', 'description')
