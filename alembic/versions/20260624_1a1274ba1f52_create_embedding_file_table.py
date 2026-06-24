"""create embedding_file table

Revision ID: 1a1274ba1f52
Revises: c4d5e6f7a8b9
Create Date: 2026-06-24 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1a1274ba1f52'
down_revision = 'c4d5e6f7a8b9'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'embedding_file',
        sa.Column('embedding_file_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('reference_id', sa.Integer(), nullable=False),
        sa.Column('profile_name', sa.String(), nullable=False),
        sa.Column('version', sa.Integer(), nullable=False),
        sa.Column('model_name', sa.String(), nullable=True),
        sa.Column('source_referencefile_id', sa.Integer(), nullable=True),
        sa.Column('parquet_referencefile_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['reference_id'], ['reference.reference_id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['source_referencefile_id'], ['referencefile.referencefile_id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['parquet_referencefile_id'], ['referencefile.referencefile_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('embedding_file_id')
    )
    op.create_index(op.f('ix_embedding_file_reference_id'), 'embedding_file', ['reference_id'], unique=False)
    op.create_index(op.f('ix_embedding_file_profile_name'), 'embedding_file', ['profile_name'], unique=False)
    op.create_index(op.f('ix_embedding_file_model_name'), 'embedding_file', ['model_name'], unique=False)
    op.create_index(op.f('ix_embedding_file_source_referencefile_id'), 'embedding_file',
                    ['source_referencefile_id'], unique=False)
    op.create_index(op.f('ix_embedding_file_parquet_referencefile_id'), 'embedding_file',
                    ['parquet_referencefile_id'], unique=False)
    # PG13 has no NULLS NOT DISTINCT: enforce the unique key with two partial
    # unique indexes (the referencefile_mod pattern).
    op.create_index('uq_embedding_file_with_source', 'embedding_file',
                    ['reference_id', 'profile_name', 'version', 'source_referencefile_id'],
                    unique=True, postgresql_where=sa.text('source_referencefile_id IS NOT NULL'))
    op.create_index('uq_embedding_file_abstract', 'embedding_file',
                    ['reference_id', 'profile_name', 'version'],
                    unique=True, postgresql_where=sa.text('source_referencefile_id IS NULL'))


def downgrade():
    op.drop_index('uq_embedding_file_abstract', table_name='embedding_file')
    op.drop_index('uq_embedding_file_with_source', table_name='embedding_file')
    op.drop_index(op.f('ix_embedding_file_parquet_referencefile_id'), table_name='embedding_file')
    op.drop_index(op.f('ix_embedding_file_source_referencefile_id'), table_name='embedding_file')
    op.drop_index(op.f('ix_embedding_file_model_name'), table_name='embedding_file')
    op.drop_index(op.f('ix_embedding_file_profile_name'), table_name='embedding_file')
    op.drop_index(op.f('ix_embedding_file_reference_id'), table_name='embedding_file')
    op.drop_table('embedding_file')
