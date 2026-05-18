"""add permission_doc_url column to image_permission

Revision ID: a1b2c3d4e5f6
Revises: 9f8e7d6c5b4a
Create Date: 2026-05-18

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = '9f8e7d6c5b4a'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('image_permission', sa.Column('permission_doc_url', sa.String(), nullable=True))


def downgrade():
    op.drop_column('image_permission', 'permission_doc_url')
