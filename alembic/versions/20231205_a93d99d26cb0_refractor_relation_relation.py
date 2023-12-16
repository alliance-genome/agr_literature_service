"""refractor relation_relation

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
    op.execute("ALTER SEQUENCE reference_comments_and_correc_reference_comment_and_correct_seq RENAME TO reference_relation_reference_relation_id_seq")

    # rename the table
    op.rename_table('reference_comments_and_corrections', 'reference_relation')

    # drop the old indexes
    op.drop_index('ix_reference_comments_and_corrections_reference_id_from', table_name='reference_relation')
    op.drop_index('ix_reference_comments_and_corrections_reference_id_to', table_name='reference_relation')

    # create the new indexes with the updated names
    op.create_index('ix_reference_relation_reference_id_from', 'reference_relation', ['reference_id_from'], unique=False)
    op.create_index('ix_reference_relation_reference_id_to', 'reference_relation', ['reference_id_to'], unique=False)

    # update the version table and indexes
    op.rename_table('reference_comments_and_corrections_version', 'reference_relation_version')

    # drop the old version table indexes
    """
    op.drop_index('ix_reference_comments_and_corrections_version_end_transaction_id', table_name='reference_relation_version')
    this doesn't work since 'ix_reference_comments_and_corrections_version_end_transaction_id'
    is too long so SQLAlchemy will complain
    So use raw SQL to drop the index with a long name 
    """
    # op.execute('DROP INDEX IF EXISTS ix_reference_comments_and_corrections_version_end_trans_c6e0')
    op.drop_index('ix_reference_comments_and_corrections_version_end_trans_c6e0',
                  table_name='reference_relation_version')
    op.drop_index('ix_reference_comments_and_corrections_version_operation_type', table_name='reference_relation_version')
    op.drop_index('ix_reference_comments_and_corrections_version_reference_id_from', table_name='reference_relation_version')
    op.drop_index('ix_reference_comments_and_corrections_version_reference_id_to', table_name='reference_relation_version')
    op.drop_index('ix_reference_comments_and_corrections_version_transaction_id', table_name='reference_relation_version')

    # create new version table indexes
    op.create_index('ix_reference_relation_version_end_transaction_id', 'reference_relation_version', ['end_transaction_id'], unique=False)
    op.create_index('ix_reference_relation_version_operation_type', 'reference_relation_version', ['operation_type'], unique=False)
    op.create_index('ix_reference_relation_version_reference_id_from', 'reference_relation_version', ['reference_id_from'], unique=False)
    op.create_index('ix_reference_relation_version_reference_id_to', 'reference_relation_version', ['reference_id_to'], unique=False)
    op.create_index('ix_reference_relation_version_transaction_id', 'reference_relation_version', ['transaction_id'], unique=False)

    # rename the column in the main table
    op.alter_column('reference_relation', 'reference_comment_and_correction_id', new_column_name='reference_relation_id')
    op.alter_column('reference_relation', 'reference_comment_and_correction_type', new_column_name='reference_relation_type')

    # rename the column in the version table
    op.alter_column('reference_relation_version', 'reference_comment_and_correction_id', new_column_name='reference_relation_id')
    op.alter_column('reference_relation_version', 'reference_comment_and_correction_type', new_column_name='reference_relation_type')
    op.alter_column('reference_relation_version', 'reference_comment_and_correction_type_mod',
                    new_column_name='reference_relation_type_mod')

    # drop the old unique constraint
    op.drop_constraint('rccm_uniq', 'reference_relation', type_='unique')

    # create the new unique constraint with the updated column name
    op.create_unique_constraint('rc_uniq', 'reference_relation', ['reference_id_from', 'reference_id_to', 'reference_relation_type'])

    # drop and recreate primary key constraint
    op.execute('ALTER TABLE reference_relation DROP CONSTRAINT reference_comments_and_corrections_pkey CASCADE')
    op.create_primary_key('reference_relation_pkey', 'reference_relation', ['reference_relation_id'])

    # drop and recreate primary key constraint of reference_comments_and_corrections_version
    op.execute('ALTER TABLE reference_relation_version DROP CONSTRAINT reference_comments_and_corrections_version_pkey CASCADE')
    op.create_primary_key('reference_relation_version_pkey', 'reference_relation_version', ['reference_relation_id','transaction_id'])

    # re-create two foreign keys of reference_relation table
    op.drop_constraint(u'reference_comments_and_corrections_reference_id_from_fkey', 'reference_relation',
                       type_='foreignkey')
    op.drop_constraint(u'reference_comments_and_corrections_reference_id_to_fkey', 'reference_relation',
                       type_='foreignkey')
    op.create_foreign_key(
        None,
        "reference_relation",
        "reference",
        ["reference_id_from"],
        ["reference_id"],
    )

    op.create_foreign_key(
        None,
        "reference_relation",
        "reference",
        ["reference_id_to"],
        ["reference_id"],
    )

    # recreate reference_relation_type with additional value: 'ChapterIn'
    op.execute(
        '''
        ALTER TABLE reference_relation ALTER COLUMN reference_relation_type TYPE VARCHAR(255);
        ALTER TABLE reference_relation_version ALTER COLUMN reference_relation_type TYPE VARCHAR(255);
        DROP TYPE IF EXISTS referencecommentandcorrectiontype;
        CREATE TYPE referencerelationtype AS ENUM 
            ('RetractionOf', 'CommentOn', 'ErratumFor', 'ReprintOf', 'UpdateOf', 'ExpressionOfConcernFor', 'RepublishedFrom', 'ChapterIn');
        ALTER TABLE reference_relation ALTER COLUMN reference_relation_type TYPE referencerelationtype 
            USING (reference_relation_type::referencerelationtype);
        ALTER TABLE reference_relation_version ALTER COLUMN reference_relation_type TYPE referencerelationtype 
            USING (reference_relation_type::referencerelationtype);
        '''
    )
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
    op.drop_index('ix_reference_relation_version_end_transaction_id', table_name='reference_comments_and_corrections_version')
    op.drop_index('ix_reference_relation_version_operation_type', table_name='reference_comments_and_corrections_version')
    op.drop_index('ix_reference_relation_version_reference_id_from', table_name='reference_comments_and_corrections_version')
    op.drop_index('ix_reference_relation_version_reference_id_to', table_name='reference_comments_and_corrections_version')
    op.drop_index('ix_reference_relation_version_transaction_id', table_name='reference_comments_and_corrections_version')

    # recreate the original version table indexes
    # ix_reference_comments_and_corrections_version_end_transaction_id is too long
    op.create_index('ix_ref_comments_and_corrections_version_end_trans_c6e0', 'reference_comments_and_corrections_version', ['end_transaction_id'], unique=False)
    op.create_index('ix_reference_comments_and_corrections_version_operation_type', 'reference_comments_and_corrections_version', ['operation_type'], unique=False)
    op.create_index('ix_reference_comments_and_corrections_version_reference_id_from', 'reference_comments_and_corrections_version', ['reference_id_from'], unique=False)
    op.create_index('ix_reference_comments_and_corrections_version_reference_id_to', 'reference_comments_and_corrections_version', ['reference_id_to'], unique=False)
    op.create_index('ix_reference_comments_and_corrections_version_transaction_id', 'reference_comments_and_corrections_version', ['transaction_id'], unique=False)

    # revert the column name change in the main table
    op.alter_column('reference_comments_and_corrections', 'reference_relation_id', new_column_name='reference_comment_and_correction_id')
    op.alter_column('reference_comments_and_corrections', 'reference_relation_type', new_column_name='reference_comment_and_correction_type')

    # revert the sequence name change
    op.execute("ALTER SEQUENCE reference_relation_reference_relation_id_seq RENAME TO reference_comments_and_correc_reference_comment_and_correct_seq")

    # revert the column name change in the version table
    op.alter_column('reference_comments_and_corrections_version', 'reference_relation_id', new_column_name='reference_comment_and_correction_id')
    op.alter_column('reference_comments_and_corrections_version', 'reference_relation_type', new_column_name='reference_comment_and_correction_type')
    op.alter_column('reference_comments_and_corrections_version', 'reference_relation_type_mod',
                    new_column_name='reference_comment_and_correction_type_mod')

    # drop the new unique constraint
    op.drop_constraint('rc_uniq', 'reference_comments_and_corrections', type_='unique')

    # recreate the original unique constraint with the old column name
    op.create_unique_constraint('rccm_uniq', 'reference_comments_and_corrections', ['reference_id_from', 'reference_id_to', 'reference_comment_and_correction_type'])
    # ### end Alembic commands ###

    # recreate the reference_comment_and_correction_type with 7 values.
    op.execute(
        '''
        ALTER TABLE reference_comments_and_corrections ALTER COLUMN reference_comment_and_correction_type TYPE VARCHAR(255);
        ALTER TABLE reference_comments_and_corrections_version ALTER COLUMN reference_comment_and_correction_type TYPE VARCHAR(255);
        DROP TYPE IF EXISTS   referencerelationtype;
        CREATE TYPE referencecommentandcorrectiontype AS ENUM 
            ('RetractionOf', 'CommentOn', 'ErratumFor', 'ReprintOf', 'UpdateOf', 'ExpressionOfConcernFor', 'RepublishedFrom','ChapterIn');
        ALTER TABLE reference_comments_and_corrections ALTER COLUMN reference_comment_and_correction_type TYPE referencecommentandcorrectiontype
            USING (reference_comment_and_correction_type::referencecommentandcorrectiontype);
        ALTER TABLE reference_comments_and_corrections_version ALTER COLUMN reference_comment_and_correction_type TYPE referencecommentandcorrectiontype
            USING (reference_comment_and_correction_type::referencecommentandcorrectiontype);
        '''
    )


    # drop and recreate primary key constraint
    op.execute('ALTER TABLE reference_comments_and_corrections DROP CONSTRAINT reference_relation_pkey CASCADE')
    op.create_primary_key('reference_comments_and_corrections_pkey', 'reference_comments_and_corrections', ['reference_comment_and_correction_id'])

    # drop and recreate primary key constraint of reference_comments_and_corrections_version
    op.execute('ALTER TABLE reference_comments_and_corrections_version DROP CONSTRAINT reference_relation_version_pkey CASCADE')
    op.create_primary_key('reference_comments_and_corrections_version_pkey', 'reference_comments_and_corrections_version', ['reference_comment_and_correction_id', 'transaction_id'])

    # recreate two foreign keys of
    op.drop_constraint(u'reference_relation_reference_id_from_fkey', 'reference_comments_and_corrections',
                       type_='foreignkey')
    op.drop_constraint(u'reference_relation_reference_id_to_fkey', 'reference_comments_and_corrections',
                       type_='foreignkey')

    op.create_foreign_key(
        None,
        "reference_comments_and_corrections",
        "reference",
        ["reference_id_from"],
        ["reference_id"],
    )
    op.create_foreign_key(
        None,
        "reference_comments_and_corrections",
        "reference",
        ["reference_id_to"],
        ["reference_id"],
    )