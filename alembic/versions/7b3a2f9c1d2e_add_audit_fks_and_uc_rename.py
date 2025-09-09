"""link audit columns to users + (optional) UC rename
Revision ID: 7b3a2f9c1d2e
Revises: abc123def456
Create Date: 2025-09-04 10:15:00.000000
"""
from alembic import op
import sqlalchemy as sa
# revision identifiers, used by Alembic.
revision = "7b3a2f9c1d2e"
down_revision = "abc123def456"
branch_labels = None
depends_on = None
# --- Configuration ---
TABLE = "indexing_priority"
USERS_TABLE = "users"
CREATED_BY_COL = "created_by"
UPDATED_BY_COL = "updated_by"
# Name the constraints explicitly so downgrade can drop them.
FK_CREATED_BY = "fk_ip_created_by_user"
FK_UPDATED_BY = "fk_ip_updated_by_user"
# Old/new UC names (rename is optional; see below).
UC_OLD = "uq_indexing_priority_mod_ref_tag"
UC_NEW = "uq_indexing_priority_mod_ref_priority"
def upgrade():
    # 0) (Optional but recommended) make sure column types are compatible.
    #    If you know they are already VARCHAR/TEXT, you can skip these.
    # op.alter_column(TABLE, CREATED_BY_COL, type_=sa.String(), existing_nullable=True)
    # op.alter_column(TABLE, UPDATED_BY_COL, type_=sa.String(), existing_nullable=True)
    # 1) Add FKs to users.id with **NOT VALID**, so existing stray values (e.g. "default_user")
    #    won’t block the migration. You can validate later after data cleanup.
    #
    # Alembic’s create_foreign_key doesn’t expose NOT VALID, so we use raw SQL.
    op.execute(
        sa.text(
            f'ALTER TABLE "{TABLE}" '
            f'ADD CONSTRAINT {FK_CREATED_BY} '
            f'FOREIGN KEY ("{CREATED_BY_COL}") REFERENCES "{USERS_TABLE}" ("id") NOT VALID'
        )
    )
    op.execute(
        sa.text(
            f'ALTER TABLE "{TABLE}" '
            f'ADD CONSTRAINT {FK_UPDATED_BY} '
            f'FOREIGN KEY ("{UPDATED_BY_COL}") REFERENCES "{USERS_TABLE}" ("id") NOT VALID'
        )
    )
    # If you want to validate immediately (only do this if you’re sure all values exist in users.id):
    # op.execute(sa.text(f'ALTER TABLE "{TABLE}" VALIDATE CONSTRAINT {FK_CREATED_BY}'))
    # op.execute(sa.text(f'ALTER TABLE "{TABLE}" VALIDATE CONSTRAINT {FK_UPDATED_BY}'))
    # 2) (Optional) rename the unique constraint instead of drop/create (minimal + fast).
    #    Comment this block out if you want to keep the old name.
    op.execute(
        sa.text(
            f'ALTER TABLE "{TABLE}" RENAME CONSTRAINT {UC_OLD} TO {UC_NEW}'
        )
    )
def downgrade():
    # 1) (Optional) rename the UC back (only if you executed the rename in upgrade).
    op.execute(
        sa.text(
            f'ALTER TABLE "{TABLE}" RENAME CONSTRAINT {UC_NEW} TO {UC_OLD}'
        )
    )
    # 2) Drop the audit FKs.
    op.execute(
        sa.text(
            f'ALTER TABLE "{TABLE}" DROP CONSTRAINT IF EXISTS {FK_UPDATED_BY}'
        )
    )
    op.execute(
        sa.text(
            f'ALTER TABLE "{TABLE}" DROP CONSTRAINT IF EXISTS {FK_CREATED_BY}'
        )
    )