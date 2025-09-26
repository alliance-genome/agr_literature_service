"""add_manual_indexing_tag_tbl_etc

Revision ID: b7bf14b5c68d
Revises: abc123def456
Create Date: 2025-09-25 03:12:43.045937
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'b7bf14b5c68d'
down_revision = '5ed317af426f'
branch_labels = None
depends_on = None


def upgrade():
    # --- NEW: manual_indexing_tag table ---
    op.create_table(
        'manual_indexing_tag',
        sa.Column('manual_indexing_tag_id', sa.Integer(), primary_key=True, autoincrement=True),

        sa.Column('reference_id', sa.Integer(), sa.ForeignKey('reference.reference_id', ondelete='CASCADE'), nullable=False),
        sa.Column('mod_id', sa.Integer(), sa.ForeignKey('mod.mod_id', ondelete='CASCADE'), nullable=False),

        sa.Column('curation_tag', sa.String(length=64), nullable=False),
        sa.Column('confidence_score', sa.Float(), nullable=True),
        sa.Column('validation_by_biocurator', sa.String(length=32), nullable=True),
        sa.Column('note', sa.String(), nullable=True),

        # audited columns (UTC)
        sa.Column('date_created', sa.DateTime(), nullable=False),
        sa.Column('date_updated', sa.DateTime(), nullable=True),
        sa.Column('created_by', sa.String(), sa.ForeignKey('users.id'), nullable=True),
        sa.Column('updated_by', sa.String(), sa.ForeignKey('users.id'), nullable=True),

        sa.UniqueConstraint('mod_id', 'reference_id', 'curation_tag', name='uq_mod_ref_tag'),
        sa.CheckConstraint(
            "curation_tag LIKE 'ATP:%'",
            name='ck_confval_curation_tag_prefix'
        ),
        sa.CheckConstraint(
            "(confidence_score IS NULL) OR (confidence_score >= 0.0 AND confidence_score <= 1.0)",
            name='ck_confval_confidence_range'
        ),
        sa.CheckConstraint(
            "(validation_by_biocurator IS NULL OR validation_by_biocurator IN ('right','wrong'))",
            name='ck_manual_indexing_tag_validation'
        ),
    )
    op.create_index(op.f('ix_manual_indexing_tag_reference_id'), 'manual_indexing_tag', ['reference_id'], unique=False)
    op.create_index(op.f('ix_manual_indexing_tag_mod_id'), 'manual_indexing_tag', ['mod_id'], unique=False)
    op.create_index(op.f('ix_manual_indexing_tag_curation_tag'), 'manual_indexing_tag', ['curation_tag'], unique=False)
    op.create_index(op.f('ix_manual_indexing_tag_date_created'), 'manual_indexing_tag', ['date_created'], unique=False)
    op.create_index(op.f('ix_manual_indexing_tag_date_updated'), 'manual_indexing_tag', ['date_updated'], unique=False)

    # Drop old indexing_priority_version (as before)
    op.drop_index('ix_indexing_priority_version_end_transaction_id', table_name='indexing_priority_version')
    op.drop_index('ix_indexing_priority_version_operation_type', table_name='indexing_priority_version')
    op.drop_index('ix_indexing_priority_version_transaction_id', table_name='indexing_priority_version')
    op.drop_table('indexing_priority_version')

    # Rename column to keep data
    op.alter_column(
        'curation_status',
        'controlled_note',
        new_column_name='curation_tag',
        existing_type=sa.String(),
    )

    op.create_index(op.f('ix_indexing_priority_date_created'), 'indexing_priority', ['date_created'], unique=False)
    op.create_index(op.f('ix_indexing_priority_date_updated'), 'indexing_priority', ['date_updated'], unique=False)

    op.add_column('workflow_tag', sa.Column('curation_tag', sa.String(), nullable=True))
    op.add_column('workflow_tag', sa.Column('note', sa.String(), nullable=True))
    op.add_column('workflow_tag_version', sa.Column('curation_tag', sa.String(), autoincrement=False, nullable=True))
    op.add_column('workflow_tag_version', sa.Column('note', sa.String(), autoincrement=False, nullable=True))
    op.add_column('workflow_tag_version', sa.Column('curation_tag_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))
    op.add_column('workflow_tag_version', sa.Column('note_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))

    # add ml_model_id to topic_entity_tag_version
    op.add_column('topic_entity_tag_version', sa.Column('ml_model_id', sa.Integer(), nullable=True))
    op.create_index('ix_topic_entity_tag_version_ml_model_id', 'topic_entity_tag_version', ['ml_model_id'], unique=False)
    op.create_foreign_key(
        'fk_topic_entity_tag_version_ml_model',
        'topic_entity_tag_version',
        'ml_model',
        ['ml_model_id'],
        ['ml_model_id'],
        ondelete='SET NULL'
    )


def downgrade():
    # Revert workflow_tag changes
    op.drop_column('workflow_tag_version', 'note_mod')
    op.drop_column('workflow_tag_version', 'curation_tag_mod')
    op.drop_column('workflow_tag_version', 'note')
    op.drop_column('workflow_tag_version', 'curation_tag')
    op.drop_column('workflow_tag', 'note')
    op.drop_column('workflow_tag', 'curation_tag')

    # Revert curation_status rename (keep data)
    op.alter_column(
        'curation_status',
        'curation_tag',
        new_column_name='controlled_note',
        existing_type=sa.String(),
    )

    # Drop indexes created on indexing_priority
    op.drop_index(op.f('ix_indexing_priority_date_updated'), table_name='indexing_priority')
    op.drop_index(op.f('ix_indexing_priority_date_created'), table_name='indexing_priority')

    op.drop_index(op.f('ix_manual_indexing_tag_date_updated'), table_name='manual_indexing_tag')
    op.drop_index(op.f('ix_manual_indexing_tag_date_created'), table_name='manual_indexing_tag')
    op.drop_index(op.f('ix_manual_indexing_tag_curation_tag'), table_name='manual_indexing_tag')
    op.drop_index(op.f('ix_manual_indexing_tag_mod_id'), table_name='manual_indexing_tag')
    op.drop_index(op.f('ix_manual_indexing_tag_reference_id'), table_name='manual_indexing_tag')
    op.drop_table('manual_indexing_tag')
