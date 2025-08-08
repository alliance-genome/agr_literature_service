"""add_audit_and_version_to_indexing_priority

Revision ID: abc123def456
Revises: 2de879b7e381
Create Date: 2025-08-08 17:30:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'abc123def456'
down_revision = '2de879b7e381'
branch_labels = None
depends_on = None


def upgrade():
    # 1) Add audit columns to indexing_priority
    op.add_column('indexing_priority', sa.Column('date_created', sa.DateTime(), nullable=False))
    op.add_column('indexing_priority', sa.Column('date_updated', sa.DateTime(), nullable=True))
    op.add_column('indexing_priority', sa.Column('created_by', sa.String(), nullable=True))
    op.add_column('indexing_priority', sa.Column('updated_by', sa.String(), nullable=True))

    # 2) Create the version table
    op.create_table(
        'indexing_priority_version',
        sa.Column('indexing_priority_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('indexing_priority', sa.String(), autoincrement=False, nullable=False),
        sa.Column('reference_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('mod_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('confidence_score', sa.Float(), autoincrement=False, nullable=True),
        sa.Column('source_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('validation_by_biocurator', sa.String(), autoincrement=False, nullable=True),
        sa.Column('date_created', sa.DateTime(), autoincrement=False, nullable=False),
        sa.Column('date_updated', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('created_by', sa.String(), autoincrement=False, nullable=True),
        sa.Column('updated_by', sa.String(), autoincrement=False, nullable=True),

        sa.Column('transaction_id', sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column('end_transaction_id', sa.BigInteger(), nullable=True),
        sa.Column('operation_type', sa.SmallInteger(), nullable=False),

        # one boolean “_mod” flag per auditable column
        sa.Column('indexing_priority_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('reference_id_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('mod_id_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('confidence_score_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('source_id_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('validation_by_biocurator_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('date_created_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('date_updated_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('created_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('updated_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),

        sa.PrimaryKeyConstraint('indexing_priority_id', 'transaction_id')
    )

    # 3) Indexes on the version table
    op.create_index(
        'ix_indexing_priority_version_transaction_id',
        'indexing_priority_version',
        ['transaction_id'],
        unique=False
    )
    op.create_index(
        'ix_indexing_priority_version_end_transaction_id',
        'indexing_priority_version',
        ['end_transaction_id'],
        unique=False
    )
    op.create_index(
        'ix_indexing_priority_version_operation_type',
        'indexing_priority_version',
        ['operation_type'],
        unique=False
    )


def downgrade():
    # Reverse the version-table creation
    op.drop_index('ix_indexing_priority_version_operation_type', table_name='indexing_priority_version')
    op.drop_index('ix_indexing_priority_version_end_transaction_id', table_name='indexing_priority_version')
    op.drop_index('ix_indexing_priority_version_transaction_id', table_name='indexing_priority_version')
    op.drop_table('indexing_priority_version')

    # Drop the audit columns
    op.drop_column('indexing_priority', 'updated_by')
    op.drop_column('indexing_priority', 'created_by')
    op.drop_column('indexing_priority', 'date_updated')
    op.drop_column('indexing_priority', 'date_created')
