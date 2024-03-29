"""resources table renamed to resource

Revision ID: 951d7c11dbcd
Revises: 2bbc6033143f
Create Date: 2022-05-04 11:41:05.920958

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '951d7c11dbcd'
down_revision = '2bbc6033143f'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    # The table itself
    op.rename_table('resources', 'resource')
    # its sequence
    op.execute('ALTER SEQUENCE resources_resource_id_seq RENAME TO resource_resource_id_seq')
    # Its Indexes
    op.execute('ALTER INDEX resources_pkey RENAME TO resource_pkey')
    op.execute('ALTER INDEX ix_resources_curie RENAME TO ix_resource_curie')

    # version table
    op.rename_table('resources_version', 'resource_version')
    op.execute('ALTER INDEX resources_version_pkey RENAME TO resource_version_pkey')
    op.execute('ALTER INDEX ix_resources_version_curie RENAME TO ix_resource_version_curie')
    op.execute('ALTER INDEX ix_resources_version_end_transaction_id RENAME TO ix_resource_version_end_transaction_id') 
    op.execute('ALTER INDEX ix_resources_version_operation_type RENAME TO ix_resource_version_operation_type')
    op.execute('ALTER INDEX ix_resources_version_transaction_id RENAME TO ix_resource_version_transaction_id')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    # The table itself
    op.rename_table('resource', 'resources')
    # its sequence
    op.execute('ALTER SEQUENCE resource_resource_id_seq RENAME TO resources_resource_id_seq')
    # Its Indexes
    op.execute('ALTER INDEX resource_pkey RENAME TO resources_pkey')
    op.execute('ALTER INDEX ix_resource_curie RENAME TO ix_resources_curie')

    # version table
    op.rename_table('resource_version', 'resources_version')
    op.execute('ALTER INDEX resource_version_pkey RENAME TO resources_version_pkey')
    op.execute('ALTER INDEX ix_resource_version_curie RENAME TO ix_resources_version_curie')
    op.execute('ALTER INDEX ix_resource_version_end_transaction_id RENAME TO ix_resources_version_end_transaction_id') 
    op.execute('ALTER INDEX ix_resource_version_operation_type RENAME TO ix_resources_version_operation_type')
    op.execute('ALTER INDEX ix_resource_version_transaction_id RENAME TO ix_resources_version_transaction_id')
    # ### end Alembic commands ###
