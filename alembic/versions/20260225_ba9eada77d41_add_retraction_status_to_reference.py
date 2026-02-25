"""add_retraction_status_to_reference

Revision ID: ba9eada77d41
Revises: 283e37c0f96d
Create Date: 2026-02-25 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ba9eada77d41'
down_revision = '283e37c0f96d'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('reference', sa.Column('retraction_status', sa.String(), nullable=True))
    op.add_column('reference_version', sa.Column('retraction_status', sa.String(), autoincrement=False, nullable=True))
    op.add_column('reference_version', sa.Column('retraction_status_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))


def downgrade():
    op.drop_column('reference_version', 'retraction_status_mod')
    op.drop_column('reference_version', 'retraction_status')
    op.drop_column('reference', 'retraction_status')
