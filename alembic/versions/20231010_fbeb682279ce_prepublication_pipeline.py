"""prepublication_pipeline

Revision ID: fbeb682279ce
Revises: 3be7348a9e68
Create Date: 2023-10-10 16:55:10.043710

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fbeb682279ce'
down_revision = '3be7348a9e68'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('reference', sa.Column('prepublication_pipeline', sa.Boolean(), server_default=sa.text('false'), nullable=False))
    op.add_column('reference_version', sa.Column('prepublication_pipeline', sa.Boolean(), autoincrement=False, nullable=True))
    op.add_column('reference_version', sa.Column('prepublication_pipeline_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))
    op.execute("ALTER TYPE modcorpussortsourcetype ADD VALUE IF NOT EXISTS 'Prepublication_pipeline' AFTER 'Assigned_for_review'")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('reference_version', 'prepublication_pipeline_mod')
    op.drop_column('reference_version', 'prepublication_pipeline')
    op.drop_column('reference', 'prepublication_pipeline')
    # ### end Alembic commands ###