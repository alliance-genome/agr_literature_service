"""added dataset tables

Revision ID: 7a72ee65a911
Revises: a89117394b2b
Create Date: 2024-11-15 18:09:15.960835

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7a72ee65a911'
down_revision = 'a89117394b2b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('dataset_version',
    sa.Column('dataset_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('title', sa.String(), autoincrement=False, nullable=True),
    sa.Column('mod_id', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('data_type', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dataset_type', sa.String(), autoincrement=False, nullable=True),
    sa.Column('version', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('description', sa.String(), autoincrement=False, nullable=True),
    sa.Column('frozen', sa.Boolean(), autoincrement=False, nullable=True),
    sa.Column('production', sa.Boolean(), autoincrement=False, nullable=True),
    sa.Column('date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('date_updated', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('created_by', sa.String(), autoincrement=False, nullable=True),
    sa.Column('updated_by', sa.String(), autoincrement=False, nullable=True),
    sa.Column('transaction_id', sa.BigInteger(), autoincrement=False, nullable=False),
    sa.Column('end_transaction_id', sa.BigInteger(), nullable=True),
    sa.Column('operation_type', sa.SmallInteger(), nullable=False),
    sa.Column('title_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('mod_id_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('data_type_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('dataset_type_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('version_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('description_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('frozen_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('production_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('date_created_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('date_updated_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('created_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('updated_by_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.PrimaryKeyConstraint('dataset_id', 'transaction_id')
    )
    op.create_index(op.f('ix_dataset_version_date_created'), 'dataset_version', ['date_created'], unique=False)
    op.create_index(op.f('ix_dataset_version_date_updated'), 'dataset_version', ['date_updated'], unique=False)
    op.create_index(op.f('ix_dataset_version_end_transaction_id'), 'dataset_version', ['end_transaction_id'], unique=False)
    op.create_index(op.f('ix_dataset_version_mod_id'), 'dataset_version', ['mod_id'], unique=False)
    op.create_index(op.f('ix_dataset_version_operation_type'), 'dataset_version', ['operation_type'], unique=False)
    op.create_index(op.f('ix_dataset_version_transaction_id'), 'dataset_version', ['transaction_id'], unique=False)
    op.create_table('dataset',
    sa.Column('dataset_id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('title', sa.String(), nullable=False),
    sa.Column('mod_id', sa.Integer(), nullable=False),
    sa.Column('data_type', sa.String(), nullable=False),
    sa.Column('dataset_type', sa.String(), nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('description', sa.String(), nullable=False),
    sa.Column('frozen', sa.Boolean(), nullable=False),
    sa.Column('production', sa.Boolean(), nullable=False),
    sa.Column('date_created', sa.DateTime(), nullable=False),
    sa.Column('date_updated', sa.DateTime(), nullable=True),
    sa.Column('created_by', sa.String(), nullable=True),
    sa.Column('updated_by', sa.String(), nullable=True),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['mod_id'], ['mod.mod_id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['updated_by'], ['users.id'], ),
    sa.PrimaryKeyConstraint('dataset_id'),
    sa.UniqueConstraint('mod_id', 'data_type', 'dataset_type', 'version', name='unique_dataset')
    )
    op.create_index(op.f('ix_dataset_date_created'), 'dataset', ['date_created'], unique=False)
    op.create_index(op.f('ix_dataset_date_updated'), 'dataset', ['date_updated'], unique=False)
    op.create_index(op.f('ix_dataset_mod_id'), 'dataset', ['mod_id'], unique=False)
    op.create_table('dataset_entry',
    sa.Column('dataset_entry_id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('dataset_id', sa.Integer(), nullable=False),
    sa.Column('supporting_topic_entity_tag_id', sa.Integer(), nullable=True),
    sa.Column('supporting_workflow_tag_id', sa.Integer(), nullable=True),
    sa.Column('reference_curie', sa.String(), nullable=False),
    sa.Column('entity', sa.String(), nullable=True),
    sa.Column('entity_count', sa.Integer(), nullable=True),
    sa.Column('sentence', sa.String(), nullable=True),
    sa.Column('section', sa.String(), nullable=True),
    sa.Column('positive', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('set_type', sa.Enum('training', 'testing', name='set_type_enum'), server_default='training', nullable=False),
    sa.ForeignKeyConstraint(['dataset_id'], ['dataset.dataset_id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['supporting_topic_entity_tag_id'], ['topic_entity_tag.topic_entity_tag_id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['supporting_workflow_tag_id'], ['workflow_tag.reference_workflow_tag_id'], ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('dataset_entry_id'),
    sa.UniqueConstraint('dataset_id', 'reference_curie', 'entity', name='unique_dataset_entry')
    )
    op.create_foreign_key(None, 'author', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'author', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'cross_reference', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'cross_reference', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'editor', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'editor', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'mod', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'mod', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'mod_corpus_association', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'mod_corpus_association', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'reference', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'reference', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'reference_mod_referencetype', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'reference_mod_referencetype', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'referencefile', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'referencefile', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'referencefile_mod', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'referencefile_mod', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'resource', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'resource', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'topic_entity_tag', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'topic_entity_tag', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'topic_entity_tag_source', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'topic_entity_tag_source', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'transaction', 'users', ['user_id'], ['id'])
    op.create_foreign_key(None, 'workflow_tag', 'users', ['created_by'], ['id'])
    op.create_foreign_key(None, 'workflow_tag', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'workflow_transition', 'users', ['updated_by'], ['id'])
    op.create_foreign_key(None, 'workflow_transition', 'users', ['created_by'], ['id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'workflow_transition', type_='foreignkey')
    op.drop_constraint(None, 'workflow_transition', type_='foreignkey')
    op.drop_constraint(None, 'workflow_tag', type_='foreignkey')
    op.drop_constraint(None, 'workflow_tag', type_='foreignkey')
    op.drop_constraint(None, 'transaction', type_='foreignkey')
    op.drop_constraint(None, 'topic_entity_tag_source', type_='foreignkey')
    op.drop_constraint(None, 'topic_entity_tag_source', type_='foreignkey')
    op.drop_constraint(None, 'topic_entity_tag', type_='foreignkey')
    op.drop_constraint(None, 'topic_entity_tag', type_='foreignkey')
    op.drop_constraint(None, 'resource', type_='foreignkey')
    op.drop_constraint(None, 'resource', type_='foreignkey')
    op.drop_constraint(None, 'referencefile_mod', type_='foreignkey')
    op.drop_constraint(None, 'referencefile_mod', type_='foreignkey')
    op.drop_constraint(None, 'referencefile', type_='foreignkey')
    op.drop_constraint(None, 'referencefile', type_='foreignkey')
    op.drop_constraint(None, 'reference_mod_referencetype', type_='foreignkey')
    op.drop_constraint(None, 'reference_mod_referencetype', type_='foreignkey')
    op.drop_constraint(None, 'reference', type_='foreignkey')
    op.drop_constraint(None, 'reference', type_='foreignkey')
    op.drop_constraint(None, 'mod_corpus_association', type_='foreignkey')
    op.drop_constraint(None, 'mod_corpus_association', type_='foreignkey')
    op.drop_constraint(None, 'mod', type_='foreignkey')
    op.drop_constraint(None, 'mod', type_='foreignkey')
    op.drop_constraint(None, 'editor', type_='foreignkey')
    op.drop_constraint(None, 'editor', type_='foreignkey')
    op.drop_constraint(None, 'cross_reference', type_='foreignkey')
    op.drop_constraint(None, 'cross_reference', type_='foreignkey')
    op.drop_constraint(None, 'author', type_='foreignkey')
    op.drop_constraint(None, 'author', type_='foreignkey')
    op.drop_table('dataset_entry')
    op.drop_index(op.f('ix_dataset_mod_id'), table_name='dataset')
    op.drop_index(op.f('ix_dataset_date_updated'), table_name='dataset')
    op.drop_index(op.f('ix_dataset_date_created'), table_name='dataset')
    op.drop_table('dataset')
    op.drop_index(op.f('ix_dataset_version_transaction_id'), table_name='dataset_version')
    op.drop_index(op.f('ix_dataset_version_operation_type'), table_name='dataset_version')
    op.drop_index(op.f('ix_dataset_version_mod_id'), table_name='dataset_version')
    op.drop_index(op.f('ix_dataset_version_end_transaction_id'), table_name='dataset_version')
    op.drop_index(op.f('ix_dataset_version_date_updated'), table_name='dataset_version')
    op.drop_index(op.f('ix_dataset_version_date_created'), table_name='dataset_version')
    op.drop_table('dataset_version')
    # ### end Alembic commands ###