"""add file_classes to models

Revision ID: e7695936840b
Revises: 78e9d4f87ed5
Create Date: 2026-04-10 17:58:44.493581

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e7695936840b'
down_revision = '78e9d4f87ed5'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('ml_model', sa.Column('file_classes', sa.ARRAY(sa.String()), nullable=True))


def downgrade():
    op.drop_column('ml_model', 'file_classes')
