"""drop_novel_topic_data

Revision ID: fff02f4044e1
Revises: a3e751dff3e2
Create Date: 2025-10-21 20:40:30.732637
"""
from alembic import op
import sqlalchemy as sa


revision = 'fff02f4044e1'
down_revision = 'a3e751dff3e2'
branch_labels = None
depends_on = None


def upgrade():
    # --- MLModel: drop novel_topic_data ---
    with op.batch_alter_table('ml_model') as batch_op:
        batch_op.drop_column('novel_topic_data')

    # --- TopicEntityTag: ensure data_novelty non-null, drop novel_topic_data, (re)index ---
    with op.batch_alter_table('topic_entity_tag') as batch_op:
        batch_op.alter_column('data_novelty',
                              existing_type=sa.String(),
                              nullable=False)

        # Create (or ensure) standard indexes
        batch_op.create_index(op.f('ix_topic_entity_tag_data_novelty'), ['data_novelty'], unique=False)
        batch_op.create_index(op.f('ix_topic_entity_tag_entity'), ['entity'], unique=False)
        batch_op.create_index(op.f('ix_topic_entity_tag_entity_type'), ['entity_type'], unique=False)
        batch_op.create_index(op.f('ix_topic_entity_tag_negated'), ['negated'], unique=False)
        batch_op.create_index(op.f('ix_topic_entity_tag_species'), ['species'], unique=False)
        batch_op.create_index(op.f('ix_topic_entity_tag_topic'), ['topic'], unique=False)

        # Drop the deprecated boolean
        batch_op.drop_column('novel_topic_data')

    # --- Version table: drop deprecated booleans; add useful indexes; keep data_novelty nullable ---
    with op.batch_alter_table('topic_entity_tag_version') as batch_op:
        batch_op.create_index(op.f('ix_topic_entity_tag_version_data_novelty'), ['data_novelty'], unique=False)
        batch_op.create_index(op.f('ix_topic_entity_tag_version_entity'), ['entity'], unique=False)
        batch_op.create_index(op.f('ix_topic_entity_tag_version_entity_type'), ['entity_type'], unique=False)
        batch_op.create_index(op.f('ix_topic_entity_tag_version_negated'), ['negated'], unique=False)
        batch_op.create_index(op.f('ix_topic_entity_tag_version_species'), ['species'], unique=False)
        batch_op.create_index(op.f('ix_topic_entity_tag_version_topic'), ['topic'], unique=False)

        # Drop deprecated columns present in history
        batch_op.drop_column('novel_topic_data')
        batch_op.drop_column('novel_topic_data_mod')


def downgrade():
    # --- Version table: restore dropped columns; drop added indexes ---
    with op.batch_alter_table('topic_entity_tag_version') as batch_op:
        batch_op.add_column(sa.Column('novel_topic_data_mod', sa.Boolean(), nullable=False, server_default=sa.text('false')))
        batch_op.add_column(sa.Column('novel_topic_data', sa.Boolean(), nullable=True, server_default=sa.text('false')))
        batch_op.drop_index(op.f('ix_topic_entity_tag_version_topic'))
        batch_op.drop_index(op.f('ix_topic_entity_tag_version_species'))
        batch_op.drop_index(op.f('ix_topic_entity_tag_version_negated'))
        batch_op.drop_index(op.f('ix_topic_entity_tag_version_entity_type'))
        batch_op.drop_index(op.f('ix_topic_entity_tag_version_entity'))
        batch_op.drop_index(op.f('ix_topic_entity_tag_version_data_novelty'))

    # --- TopicEntityTag: restore novel_topic_data; relax NOT NULL; drop indexes we added ---
    with op.batch_alter_table('topic_entity_tag') as batch_op:
        batch_op.add_column(sa.Column('novel_topic_data', sa.Boolean(), nullable=False, server_default=sa.text('false')))
        batch_op.drop_index(op.f('ix_topic_entity_tag_topic'))
        batch_op.drop_index(op.f('ix_topic_entity_tag_species'))
        batch_op.drop_index(op.f('ix_topic_entity_tag_negated'))
        batch_op.drop_index(op.f('ix_topic_entity_tag_entity_type'))
        batch_op.drop_index(op.f('ix_topic_entity_tag_entity'))
        batch_op.drop_index(op.f('ix_topic_entity_tag_data_novelty'))
        batch_op.alter_column('data_novelty',
                              existing_type=sa.String(),
                              nullable=True)

    # --- MLModel: restore novel_topic_data ---
    with op.batch_alter_table('ml_model') as batch_op:
        batch_op.add_column(sa.Column('novel_topic_data', sa.Boolean(), nullable=True))
