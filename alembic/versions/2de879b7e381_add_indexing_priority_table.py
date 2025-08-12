"""add_indexing_priority_table

Revision ID: 2de879b7e381
Revises: d1f0367048d4
Create Date: 2025-08-08 16:25:11.371292
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2de879b7e381'
down_revision = 'd1f0367048d4'
branch_labels = None
depends_on = None


def upgrade():
    # ### Create indexing_priority table ###
    op.create_table(
        'indexing_priority',
        sa.Column('indexing_priority_id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('indexing_priority', sa.String(), nullable=False),
        sa.Column('reference_id', sa.Integer(), nullable=False),
        sa.Column('mod_id', sa.Integer(), nullable=False),
        sa.Column('confidence_score', sa.Float(), nullable=True),
        sa.Column('source_id', sa.Integer(), nullable=False),
        sa.Column('validation_by_biocurator', sa.String(), nullable=True),

        sa.ForeignKeyConstraint(
            ['reference_id'],
            ['reference.reference_id'],
            ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['mod_id'],
            ['mod.mod_id'],
            ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['source_id'],
            ['topic_entity_tag_source.topic_entity_tag_source_id'],
            ondelete='CASCADE'
        ),

        sa.UniqueConstraint(
            'mod_id',
            'reference_id',
            'indexing_priority',
            name='uq_indexing_priority_mod_ref_tag'
        ),
        sa.CheckConstraint(
            "indexing_priority LIKE 'ATP:%'",
            name='ck_indexing_priority_prefix'
        )
    )

    # ### create indexes ###
    op.create_index(
        'ix_indexing_priority_indexing_priority',
        'indexing_priority',
        ['indexing_priority']
    )
    op.create_index(
        'ix_indexing_priority_reference_id',
        'indexing_priority',
        ['reference_id']
    )
    op.create_index(
        'ix_indexing_priority_mod_id',
        'indexing_priority',
        ['mod_id']
    )
    op.create_index(
        'ix_indexing_priority_source_id',
        'indexing_priority',
        ['source_id']
    )


def downgrade():
    # ### drop table and indexes ###
    op.drop_index('ix_indexing_priority_source_id', table_name='indexing_priority')
    op.drop_index('ix_indexing_priority_mod_id', table_name='indexing_priority')
    op.drop_index('ix_indexing_priority_reference_id', table_name='indexing_priority')
    op.drop_index('ix_indexing_priority_indexing_priority', table_name='indexing_priority')

    op.drop_table('indexing_priority')
