"""new_mod_ref_type_structure

Revision ID: fbd067b9e403
Revises: 92d11d28775c
Create Date: 2022-10-10 23:06:21.844569

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fbd067b9e403'
down_revision = '92d11d28775c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('mod_referencetype_version',
    sa.Column('mod_referencetype_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('mod_id', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('referencetype_id', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('display_order', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('transaction_id', sa.BigInteger(), autoincrement=False, nullable=False),
    sa.Column('end_transaction_id', sa.BigInteger(), nullable=True),
    sa.Column('operation_type', sa.SmallInteger(), nullable=False),
    sa.Column('mod_id_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('referencetype_id_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('display_order_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.PrimaryKeyConstraint('mod_referencetype_id', 'transaction_id')
    )
    op.create_index(op.f('ix_mod_referencetype_version_end_transaction_id'), 'mod_referencetype_version', ['end_transaction_id'], unique=False)
    op.create_index(op.f('ix_mod_referencetype_version_operation_type'), 'mod_referencetype_version', ['operation_type'], unique=False)
    op.create_index(op.f('ix_mod_referencetype_version_transaction_id'), 'mod_referencetype_version', ['transaction_id'], unique=False)
    op.create_table('reference_mod_referencetype_version',
    sa.Column('date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('date_updated', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('reference_mod_referencetype_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('reference_id', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('mod_referencetype_id', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('created_by', sa.String(), autoincrement=False, nullable=True),
    sa.Column('updated_by', sa.String(), autoincrement=False, nullable=True),
    sa.Column('transaction_id', sa.BigInteger(), autoincrement=False, nullable=False),
    sa.Column('end_transaction_id', sa.BigInteger(), nullable=True),
    sa.Column('operation_type', sa.SmallInteger(), nullable=False),
    sa.Column('date_created_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('date_updated_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('reference_id_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('mod_referencetype_id_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('created_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('updated_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.PrimaryKeyConstraint('reference_mod_referencetype_id', 'transaction_id')
    )
    op.create_index(op.f('ix_reference_mod_referencetype_version_end_transaction_id'), 'reference_mod_referencetype_version', ['end_transaction_id'], unique=False)
    op.create_index(op.f('ix_reference_mod_referencetype_version_operation_type'), 'reference_mod_referencetype_version', ['operation_type'], unique=False)
    op.create_index(op.f('ix_reference_mod_referencetype_version_transaction_id'), 'reference_mod_referencetype_version', ['transaction_id'], unique=False)
    op.create_table('referencetype',
    sa.Column('referencetype_id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('label', sa.String(), nullable=False),
    sa.PrimaryKeyConstraint('referencetype_id'),
    sa.UniqueConstraint('label')
    )
    op.create_table('referencetype_version',
    sa.Column('referencetype_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('label', sa.String(), autoincrement=False, nullable=True),
    sa.Column('transaction_id', sa.BigInteger(), autoincrement=False, nullable=False),
    sa.Column('end_transaction_id', sa.BigInteger(), nullable=True),
    sa.Column('operation_type', sa.SmallInteger(), nullable=False),
    sa.Column('label_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.PrimaryKeyConstraint('referencetype_id', 'transaction_id')
    )
    op.create_index(op.f('ix_referencetype_version_end_transaction_id'), 'referencetype_version', ['end_transaction_id'], unique=False)
    op.create_index(op.f('ix_referencetype_version_operation_type'), 'referencetype_version', ['operation_type'], unique=False)
    op.create_index(op.f('ix_referencetype_version_transaction_id'), 'referencetype_version', ['transaction_id'], unique=False)
    op.create_table('mod_referencetype',
    sa.Column('mod_referencetype_id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('mod_id', sa.Integer(), nullable=False),
    sa.Column('referencetype_id', sa.Integer(), nullable=False),
    sa.Column('display_order', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['mod_id'], ['mod.mod_id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['referencetype_id'], ['referencetype.referencetype_id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('mod_referencetype_id')
    )
    op.create_table('reference_mod_referencetype',
    sa.Column('date_created', sa.DateTime(), nullable=False),
    sa.Column('date_updated', sa.DateTime(), nullable=True),
    sa.Column('reference_mod_referencetype_id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('reference_id', sa.Integer(), nullable=True),
    sa.Column('mod_referencetype_id', sa.Integer(), nullable=True),
    sa.Column('created_by', sa.String(), nullable=True),
    sa.Column('updated_by', sa.String(), nullable=True),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['mod_referencetype_id'], ['mod_referencetype.mod_referencetype_id'], ),
    sa.ForeignKeyConstraint(['reference_id'], ['reference.reference_id'], ),
    sa.ForeignKeyConstraint(['updated_by'], ['users.id'], ),
    sa.PrimaryKeyConstraint('reference_mod_referencetype_id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('reference_mod_referencetype')
    op.drop_table('mod_referencetype')
    op.drop_index(op.f('ix_referencetype_version_transaction_id'), table_name='referencetype_version')
    op.drop_index(op.f('ix_referencetype_version_operation_type'), table_name='referencetype_version')
    op.drop_index(op.f('ix_referencetype_version_end_transaction_id'), table_name='referencetype_version')
    op.drop_table('referencetype_version')
    op.drop_table('referencetype')
    op.drop_index(op.f('ix_reference_mod_referencetype_version_transaction_id'), table_name='reference_mod_referencetype_version')
    op.drop_index(op.f('ix_reference_mod_referencetype_version_operation_type'), table_name='reference_mod_referencetype_version')
    op.drop_index(op.f('ix_reference_mod_referencetype_version_end_transaction_id'), table_name='reference_mod_referencetype_version')
    op.drop_table('reference_mod_referencetype_version')
    op.drop_index(op.f('ix_mod_referencetype_version_transaction_id'), table_name='mod_referencetype_version')
    op.drop_index(op.f('ix_mod_referencetype_version_operation_type'), table_name='mod_referencetype_version')
    op.drop_index(op.f('ix_mod_referencetype_version_end_transaction_id'), table_name='mod_referencetype_version')
    op.drop_table('mod_referencetype_version')
    # ### end Alembic commands ###
