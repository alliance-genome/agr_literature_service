"""embedding_profile

Revision ID: 32089edb2830
Revises: c8d1f2a3b4e5
Create Date: 2026-07-17 17:01:46.892112

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '32089edb2830'
down_revision = 'c8d1f2a3b4e5'
branch_labels = None
depends_on = None


def upgrade():
    # SCRUM-5781: ABC-embedding recipe columns on ml_model (NULL for legacy models).
    op.add_column('ml_model', sa.Column('embedding_profile', sa.String(), nullable=True))
    op.add_column('ml_model', sa.Column('embedding_version', sa.Integer(), nullable=True))
    op.create_index(op.f('ix_ml_model_embedding_profile'), 'ml_model', ['embedding_profile'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_ml_model_embedding_profile'), table_name='ml_model')
    op.drop_column('ml_model', 'embedding_version')
    op.drop_column('ml_model', 'embedding_profile')
