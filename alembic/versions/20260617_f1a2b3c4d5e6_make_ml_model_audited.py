"""make ml_model audited

Adds the four audit columns (date_created, date_updated, created_by, updated_by)
to the existing ml_model table so MLModel becomes an AuditedModel. Existing rows
have their date_created backfilled from the linked dataset's date_created, falling
back to now() for rows without a dataset; date_updated mirrors date_created.
created_by / updated_by are left NULL for pre-existing rows (no reliable historical
user). No versioning (*_version table) is added.

Revision ID: f1a2b3c4d5e6
Revises: d4e9c2b7a8f1
Create Date: 2026-06-17

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f1a2b3c4d5e6'
down_revision = 'd4e9c2b7a8f1'
branch_labels = None
depends_on = None


def upgrade():
    # Add audit columns (date_created nullable for now so the backfill can run).
    op.add_column('ml_model', sa.Column('date_created', sa.DateTime(), nullable=True))
    op.add_column('ml_model', sa.Column('date_updated', sa.DateTime(), nullable=True))
    op.add_column('ml_model', sa.Column('created_by', sa.String(), nullable=True))
    op.add_column('ml_model', sa.Column('updated_by', sa.String(), nullable=True))

    op.create_foreign_key(
        'ml_model_created_by_fkey', 'ml_model', 'users', ['created_by'], ['id']
    )
    op.create_foreign_key(
        'ml_model_updated_by_fkey', 'ml_model', 'users', ['updated_by'], ['id']
    )

    op.create_index(op.f('ix_ml_model_date_created'), 'ml_model', ['date_created'], unique=False)
    op.create_index(op.f('ix_ml_model_date_updated'), 'ml_model', ['date_updated'], unique=False)

    # Backfill date_created from the linked dataset's creation date, falling back
    # to now() for rows without a dataset; date_updated mirrors date_created.
    op.execute(
        """
        UPDATE ml_model m
        SET date_created = COALESCE(
            (SELECT d.date_created FROM dataset d WHERE d.dataset_id = m.dataset_id),
            now()
        )
        WHERE m.date_created IS NULL
        """
    )
    op.execute("UPDATE ml_model SET date_updated = date_created WHERE date_updated IS NULL")

    # date_created is NOT NULL on AuditedModel.
    op.alter_column('ml_model', 'date_created', existing_type=sa.DateTime(), nullable=False)


def downgrade():
    op.drop_index(op.f('ix_ml_model_date_updated'), table_name='ml_model')
    op.drop_index(op.f('ix_ml_model_date_created'), table_name='ml_model')
    op.drop_constraint('ml_model_updated_by_fkey', 'ml_model', type_='foreignkey')
    op.drop_constraint('ml_model_created_by_fkey', 'ml_model', type_='foreignkey')
    op.drop_column('ml_model', 'updated_by')
    op.drop_column('ml_model', 'created_by')
    op.drop_column('ml_model', 'date_updated')
    op.drop_column('ml_model', 'date_created')
