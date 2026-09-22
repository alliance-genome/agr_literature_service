"""create vocabulary_abc tables

Revision ID: 91c50e342859
Revises: 87f14b1e221d
Create Date: 2026-07-23 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "91c50e342859"
down_revision = "87f14b1e221d"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands hand-written to mirror the dataset versioned-table template ###
    op.create_table('vocabulary_abc_version',
    sa.Column('vocabulary_abc_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('vocabulary', sa.String(), autoincrement=False, nullable=True),
    sa.Column('date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('date_updated', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('created_by', sa.String(), autoincrement=False, nullable=True),
    sa.Column('updated_by', sa.String(), autoincrement=False, nullable=True),
    sa.Column('transaction_id', sa.BigInteger(), autoincrement=False, nullable=False),
    sa.Column('end_transaction_id', sa.BigInteger(), nullable=True),
    sa.Column('operation_type', sa.SmallInteger(), nullable=False),
    sa.Column('vocabulary_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('date_created_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('date_updated_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('created_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('updated_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.PrimaryKeyConstraint('vocabulary_abc_id', 'transaction_id')
    )
    op.create_index(op.f('ix_vocabulary_abc_version_date_created'), 'vocabulary_abc_version',
                    ['date_created'], unique=False)
    op.create_index(op.f('ix_vocabulary_abc_version_date_updated'), 'vocabulary_abc_version',
                    ['date_updated'], unique=False)
    op.create_index(op.f('ix_vocabulary_abc_version_end_transaction_id'), 'vocabulary_abc_version',
                    ['end_transaction_id'], unique=False)
    op.create_index(op.f('ix_vocabulary_abc_version_operation_type'), 'vocabulary_abc_version',
                    ['operation_type'], unique=False)
    op.create_index(op.f('ix_vocabulary_abc_version_transaction_id'), 'vocabulary_abc_version',
                    ['transaction_id'], unique=False)
    op.create_table('vocabulary_abc',
    sa.Column('vocabulary_abc_id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('vocabulary', sa.String(), nullable=False),
    sa.Column('date_created', sa.DateTime(), nullable=False),
    sa.Column('date_updated', sa.DateTime(), nullable=True),
    sa.Column('created_by', sa.String(), nullable=True),
    sa.Column('updated_by', sa.String(), nullable=True),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['updated_by'], ['users.id'], ),
    sa.PrimaryKeyConstraint('vocabulary_abc_id'),
    sa.UniqueConstraint('vocabulary', name='uq_vocabulary_abc_vocabulary')
    )
    op.create_index(op.f('ix_vocabulary_abc_date_created'), 'vocabulary_abc', ['date_created'], unique=False)
    op.create_index(op.f('ix_vocabulary_abc_date_updated'), 'vocabulary_abc', ['date_updated'], unique=False)
    op.create_table('vocabulary_term_abc_version',
    sa.Column('vocabulary_term_abc_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('vocabulary_abc_id', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('name', sa.String(), autoincrement=False, nullable=True),
    sa.Column('is_obsolete', sa.Boolean(), autoincrement=False, nullable=True),
    sa.Column('date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('date_updated', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('created_by', sa.String(), autoincrement=False, nullable=True),
    sa.Column('updated_by', sa.String(), autoincrement=False, nullable=True),
    sa.Column('transaction_id', sa.BigInteger(), autoincrement=False, nullable=False),
    sa.Column('end_transaction_id', sa.BigInteger(), nullable=True),
    sa.Column('operation_type', sa.SmallInteger(), nullable=False),
    sa.Column('vocabulary_abc_id_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('name_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('is_obsolete_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('date_created_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('date_updated_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('created_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('updated_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.PrimaryKeyConstraint('vocabulary_term_abc_id', 'transaction_id')
    )
    op.create_index(op.f('ix_vocabulary_term_abc_version_date_created'), 'vocabulary_term_abc_version',
                    ['date_created'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_abc_version_date_updated'), 'vocabulary_term_abc_version',
                    ['date_updated'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_abc_version_end_transaction_id'), 'vocabulary_term_abc_version',
                    ['end_transaction_id'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_abc_version_operation_type'), 'vocabulary_term_abc_version',
                    ['operation_type'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_abc_version_transaction_id'), 'vocabulary_term_abc_version',
                    ['transaction_id'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_abc_version_vocabulary_abc_id'), 'vocabulary_term_abc_version',
                    ['vocabulary_abc_id'], unique=False)
    op.create_table('vocabulary_term_abc',
    sa.Column('vocabulary_term_abc_id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('vocabulary_abc_id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('is_obsolete', sa.Boolean(), nullable=False),
    sa.Column('date_created', sa.DateTime(), nullable=False),
    sa.Column('date_updated', sa.DateTime(), nullable=True),
    sa.Column('created_by', sa.String(), nullable=True),
    sa.Column('updated_by', sa.String(), nullable=True),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['updated_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['vocabulary_abc_id'], ['vocabulary_abc.vocabulary_abc_id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('vocabulary_term_abc_id'),
    sa.UniqueConstraint('vocabulary_abc_id', 'name', name='uq_vocabulary_term_abc_vocab_name')
    )
    op.create_index(op.f('ix_vocabulary_term_abc_date_created'), 'vocabulary_term_abc', ['date_created'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_abc_date_updated'), 'vocabulary_term_abc', ['date_updated'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_abc_vocabulary_abc_id'), 'vocabulary_term_abc',
                    ['vocabulary_abc_id'], unique=False)
    op.create_table('vocabulary_term_synonym_abc_version',
    sa.Column('vocabulary_term_synonym_abc_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('vocabulary_term_abc_id', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('synonym_name', sa.String(), autoincrement=False, nullable=True),
    sa.Column('date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('date_updated', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('created_by', sa.String(), autoincrement=False, nullable=True),
    sa.Column('updated_by', sa.String(), autoincrement=False, nullable=True),
    sa.Column('transaction_id', sa.BigInteger(), autoincrement=False, nullable=False),
    sa.Column('end_transaction_id', sa.BigInteger(), nullable=True),
    sa.Column('operation_type', sa.SmallInteger(), nullable=False),
    sa.Column('vocabulary_term_abc_id_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('synonym_name_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('date_created_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('date_updated_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('created_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('updated_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.PrimaryKeyConstraint('vocabulary_term_synonym_abc_id', 'transaction_id')
    )
    op.create_index(op.f('ix_vocabulary_term_synonym_abc_version_date_created'),
                    'vocabulary_term_synonym_abc_version', ['date_created'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_synonym_abc_version_date_updated'),
                    'vocabulary_term_synonym_abc_version', ['date_updated'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_synonym_abc_version_end_transaction_id'),
                    'vocabulary_term_synonym_abc_version', ['end_transaction_id'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_synonym_abc_version_operation_type'),
                    'vocabulary_term_synonym_abc_version', ['operation_type'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_synonym_abc_version_transaction_id'),
                    'vocabulary_term_synonym_abc_version', ['transaction_id'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_synonym_abc_version_vocabulary_term_abc_id'),
                    'vocabulary_term_synonym_abc_version', ['vocabulary_term_abc_id'], unique=False)
    op.create_table('vocabulary_term_synonym_abc',
    sa.Column('vocabulary_term_synonym_abc_id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('vocabulary_term_abc_id', sa.Integer(), nullable=False),
    sa.Column('synonym_name', sa.String(), nullable=False),
    sa.Column('date_created', sa.DateTime(), nullable=False),
    sa.Column('date_updated', sa.DateTime(), nullable=True),
    sa.Column('created_by', sa.String(), nullable=True),
    sa.Column('updated_by', sa.String(), nullable=True),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['updated_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['vocabulary_term_abc_id'], ['vocabulary_term_abc.vocabulary_term_abc_id'],
                            ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('vocabulary_term_synonym_abc_id'),
    sa.UniqueConstraint('vocabulary_term_abc_id', 'synonym_name',
                        name='uq_vocabulary_term_synonym_abc_term_synonym')
    )
    op.create_index(op.f('ix_vocabulary_term_synonym_abc_date_created'), 'vocabulary_term_synonym_abc',
                    ['date_created'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_synonym_abc_date_updated'), 'vocabulary_term_synonym_abc',
                    ['date_updated'], unique=False)
    op.create_index(op.f('ix_vocabulary_term_synonym_abc_vocabulary_term_abc_id'), 'vocabulary_term_synonym_abc',
                    ['vocabulary_term_abc_id'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands hand-written to mirror the dataset versioned-table template ###
    op.drop_index(op.f('ix_vocabulary_term_synonym_abc_vocabulary_term_abc_id'),
                  table_name='vocabulary_term_synonym_abc')
    op.drop_index(op.f('ix_vocabulary_term_synonym_abc_date_updated'), table_name='vocabulary_term_synonym_abc')
    op.drop_index(op.f('ix_vocabulary_term_synonym_abc_date_created'), table_name='vocabulary_term_synonym_abc')
    op.drop_table('vocabulary_term_synonym_abc')
    op.drop_index(op.f('ix_vocabulary_term_synonym_abc_version_vocabulary_term_abc_id'),
                  table_name='vocabulary_term_synonym_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_synonym_abc_version_transaction_id'),
                  table_name='vocabulary_term_synonym_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_synonym_abc_version_operation_type'),
                  table_name='vocabulary_term_synonym_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_synonym_abc_version_end_transaction_id'),
                  table_name='vocabulary_term_synonym_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_synonym_abc_version_date_updated'),
                  table_name='vocabulary_term_synonym_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_synonym_abc_version_date_created'),
                  table_name='vocabulary_term_synonym_abc_version')
    op.drop_table('vocabulary_term_synonym_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_abc_vocabulary_abc_id'), table_name='vocabulary_term_abc')
    op.drop_index(op.f('ix_vocabulary_term_abc_date_updated'), table_name='vocabulary_term_abc')
    op.drop_index(op.f('ix_vocabulary_term_abc_date_created'), table_name='vocabulary_term_abc')
    op.drop_table('vocabulary_term_abc')
    op.drop_index(op.f('ix_vocabulary_term_abc_version_vocabulary_abc_id'), table_name='vocabulary_term_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_abc_version_transaction_id'), table_name='vocabulary_term_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_abc_version_operation_type'), table_name='vocabulary_term_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_abc_version_end_transaction_id'), table_name='vocabulary_term_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_abc_version_date_updated'), table_name='vocabulary_term_abc_version')
    op.drop_index(op.f('ix_vocabulary_term_abc_version_date_created'), table_name='vocabulary_term_abc_version')
    op.drop_table('vocabulary_term_abc_version')
    op.drop_index(op.f('ix_vocabulary_abc_date_updated'), table_name='vocabulary_abc')
    op.drop_index(op.f('ix_vocabulary_abc_date_created'), table_name='vocabulary_abc')
    op.drop_table('vocabulary_abc')
    op.drop_index(op.f('ix_vocabulary_abc_version_transaction_id'), table_name='vocabulary_abc_version')
    op.drop_index(op.f('ix_vocabulary_abc_version_operation_type'), table_name='vocabulary_abc_version')
    op.drop_index(op.f('ix_vocabulary_abc_version_end_transaction_id'), table_name='vocabulary_abc_version')
    op.drop_index(op.f('ix_vocabulary_abc_version_date_updated'), table_name='vocabulary_abc_version')
    op.drop_index(op.f('ix_vocabulary_abc_version_date_created'), table_name='vocabulary_abc_version')
    op.drop_table('vocabulary_abc_version')
    # ### end Alembic commands ###
