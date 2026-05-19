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
    op.add_column('image_permission_version', sa.Column('permission_doc_url', sa.String(), autoincrement=False, nullable=True))
    op.add_column('image_permission_version', sa.Column('permission_doc_url_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))


def downgrade():
    op.drop_column('image_permission_version', 'permission_doc_url_mod')
    op.drop_column('image_permission_version', 'permission_doc_url')
    op.drop_column('image_permission', 'permission_doc_url')
