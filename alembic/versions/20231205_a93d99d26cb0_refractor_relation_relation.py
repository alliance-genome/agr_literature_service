"""refactor reference_relation

Revision ID: a93d99d26cb0
Revises: 7f029313b097
Create Date: 2023-12-05 22:51:45.038363

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a93d99d26cb0'
down_revision = '7f029313b097'
branch_labels = None
depends_on = None

def upgrade():

    # rename the sequence
    op.execute("ALTER SEQUENCE reference_comments_and_correc_reference_comment_and_correct_seq RENAME TO reference_relation_id_seq")

    # rename the table
    op.rename_table('reference_comments_and_corrections', 'reference_relation')

    # drop the old indexes
    op.drop_index('ix_reference_comments_and_corrections_reference_id_from', table_name='reference_relation')
    op.drop_index('ix_reference_comments_and_corrections_reference_id_to', table_name='reference_relation')

    # create the new indexes with the updated names
    op.create_index('ix_ref_relation_reference_id_from', 'reference_relation', ['reference_id_from'], unique=False)
    op.create_index('ix_ref_relation_reference_id_to', 'reference_relation', ['reference_id_to'], unique=False)

    # update the version table and indexes
    op.rename_table('reference_comments_and_corrections_version', 'reference_relation_version')

    # drop the old version table indexes
    """
    op.drop_index('ix_reference_comments_and_corrections_version_end_transaction_id', table_name='reference_relation_version')
    this doesn't work since 'ix_reference_comments_and_corrections_version_end_transaction_id'
    is too long so SQLAlchemy will complain
    So use raw SQL to drop the index with a long name 
    """
    op.execute('DROP INDEX IF EXISTS ix_reference_comments_and_corrections_version_end_transaction_id')
    op.drop_index('ix_reference_comments_and_corrections_version_operation_type', table_name='reference_relation_version')
    op.drop_index('ix_reference_comments_and_corrections_version_reference_id_from', table_name='reference_relation_version')
    op.drop_index('ix_reference_comments_and_corrections_version_reference_id_to', table_name='reference_relation_version')
    op.drop_index('ix_reference_comments_and_corrections_version_transaction_id', table_name='reference_relation_version')

    # create new version table indexes
    op.create_index('ix_ref_rel_ver_end_trans_id', 'reference_relation_version', ['end_transaction_id'], unique=False)
    op.create_index('ix_ref_rel_ver_operation_type', 'reference_relation_version', ['operation_type'], unique=False)
    op.create_index('ix_ref_rel_ver_reference_id_from', 'reference_relation_version', ['reference_id_from'], unique=False)
    op.create_index('ix_ref_rel_ver_reference_id_to', 'reference_relation_version', ['reference_id_to'], unique=False)
    op.create_index('ix_ref_rel_ver_transaction_id', 'reference_relation_version', ['transaction_id'], unique=False)

    # rename the column in the main table
    op.alter_column('reference_relation', 'reference_comment_and_correction_id', new_column_name='reference_relation_id')
    op.alter_column('reference_relation', 'reference_comment_and_correction_type', new_column_name='reference_relation_type')

    # rename the column in the version table
    op.alter_column('reference_relation_version', 'reference_comment_and_correction_id', new_column_name='reference_relation_id')
    op.alter_column('reference_relation_version', 'reference_comment_and_correction_type', new_column_name='reference_relation_type')

    # drop the old unique constraint
    op.drop_constraint('rccm_uniq', 'reference_relation', type_='unique')

    # create the new unique constraint with the updated column name
    op.create_unique_constraint('rc_uniq', 'reference_relation', ['reference_id_from', 'reference_id_to', 'reference_relation_type'])


def downgrade():

    # revert the table name change
    op.rename_table('reference_relation', 'reference_comments_and_corrections')

    # drop the new indexes
    op.drop_index('ix_reference_relation_reference_id_from', table_name='reference_comments_and_corrections')
    op.drop_index('ix_reference_relation_reference_id_to', table_name='reference_comments_and_corrections')

    # recreate the original indexes with the old names
    op.create_index('ix_reference_comments_and_corrections_reference_id_from', 'reference_comments_and_corrections', ['reference_id_from'], unique=False)
    op.create_index('ix_reference_comments_and_corrections_reference_id_to', 'reference_comments_and_corrections', ['reference_id_to'], unique=False)

    # revert the version table and indexes
    op.rename_table('reference_relation_version', 'reference_comments_and_corrections_version')

    # drop the new version table indexes
    op.drop_index('ix_ref_rel_ver_end_trans_id', table_name='reference_comments_and_corrections_version')
    op.drop_index('ix_ref_rel_ver_operation_type', table_name='reference_comments_and_corrections_version')
    op.drop_index('ix_ref_rel_ver_reference_id_from', table_name='reference_comments_and_corrections_version')
    op.drop_index('ix_ref_rel_ver_reference_id_to', table_name='reference_comments_and_corrections_version')
    op.drop_index('ix_ref_rel_ver_transaction_id', table_name='reference_comments_and_corrections_version')

    # recreate the original version table indexes
    # ix_reference_comments_and_corrections_version_end_transaction_id is too long
    op.create_index('ix_ref_comments_and_corrections_version_end_transaction_id', 'reference_comments_and_corrections_version', ['end_transaction_id'], unique=False)
    op.create_index('ix_reference_comments_and_corrections_version_operation_type', 'reference_comments_and_corrections_version', ['operation_type'], unique=False)
    op.create_index('ix_reference_comments_and_corrections_version_reference_id_from', 'reference_comments_and_corrections_version', ['reference_id_from'], unique=False)
    op.create_index('ix_reference_comments_and_corrections_version_reference_id_to', 'reference_comments_and_corrections_version', ['reference_id_to'], unique=False)
    op.create_index('ix_reference_comments_and_corrections_version_transaction_id', 'reference_comments_and_corrections_version', ['transaction_id'], unique=False)

    # revert the column name change in the main table
    op.alter_column('reference_comments_and_corrections', 'reference_relation_id', new_column_name='reference_comment_and_correction_id')
    op.alter_column('reference_comments_and_corrections', 'reference_relation_type', new_column_name='reference_comment_and_correction_type')

    # revert the sequence name change
    op.execute("ALTER SEQUENCE reference_relation_id_seq RENAME TO reference_comment_correction_id_seq")

    # revert the column name change in the version table
    op.alter_column('reference_comments_and_corrections_version', 'reference_relation_id', new_column_name='reference_comment_and_correction_id')
    op.alter_column('reference_comments_and_corrections_version', 'reference_relation_type', new_column_name='reference_comment_and_correction_type')

    # drop the new unique constraint
    op.drop_constraint('rc_uniq', 'reference_comments_and_corrections', type_='unique')

    # recreate the original unique constraint with the old column name
    op.create_unique_constraint('rccm_uniq', 'reference_comments_and_corrections', ['reference_id_from', 'reference_id_to', 'reference_comment_and_correction_type'])
