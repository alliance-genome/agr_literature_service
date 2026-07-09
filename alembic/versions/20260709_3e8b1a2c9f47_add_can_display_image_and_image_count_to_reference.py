"""add can_display_image and image_count to reference

Revision ID: 3e8b1a2c9f47
Revises: b7c3e9f1a2d4
Create Date: 2026-07-09

Adds two denormalized columns to reference (and reference_version):

  - image_count: number of referencefile rows with file_class like '%figure%'
  - can_display_image: effective image display permission, mirroring
    get_effective_image_permission() in reference_crud.py

Installs the SQL functions and triggers that keep the columns up to date
(on reference, referencefile, resource, copyright_license,
resource_image_permission and image_permission), then backfills all
existing reference rows.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

from agr_literature_service.api.triggers.reference_image_sql_func_triggers import (
    reference_image_sql_statements,
)

revision = '3e8b1a2c9f47'
down_revision = 'b7c3e9f1a2d4'
branch_labels = None
depends_on = None


def _column_exists(table, column):
    bind = op.get_bind()
    insp = sa_inspect(bind)
    columns = [col["name"] for col in insp.get_columns(table)]
    return column in columns


def upgrade():
    # 1. Add columns to reference
    if not _column_exists('reference', 'can_display_image'):
        op.add_column(
            'reference',
            sa.Column('can_display_image', sa.Boolean(),
                      server_default=sa.text('false'), nullable=False))
    if not _column_exists('reference', 'image_count'):
        op.add_column(
            'reference',
            sa.Column('image_count', sa.Integer(),
                      server_default=sa.text('0'), nullable=False))

    # 2. Add columns to reference_version (sqlalchemy-continuum)
    if not _column_exists('reference_version', 'can_display_image'):
        op.add_column(
            'reference_version',
            sa.Column('can_display_image', sa.Boolean(),
                      server_default=sa.text('false'),
                      autoincrement=False, nullable=True))
    if not _column_exists('reference_version', 'can_display_image_mod'):
        op.add_column(
            'reference_version',
            sa.Column('can_display_image_mod', sa.Boolean(),
                      server_default=sa.text('false'), nullable=False))
    if not _column_exists('reference_version', 'image_count'):
        op.add_column(
            'reference_version',
            sa.Column('image_count', sa.Integer(),
                      server_default=sa.text('0'),
                      autoincrement=False, nullable=True))
    if not _column_exists('reference_version', 'image_count_mod'):
        op.add_column(
            'reference_version',
            sa.Column('image_count_mod', sa.Boolean(),
                      server_default=sa.text('false'), nullable=False))

    # 3. Install the SQL functions and triggers (same statements the API
    #    installs at startup via create_all_triggers())
    for statement in reference_image_sql_statements:
        op.execute(statement)

    # 4. Backfill existing rows. This update only changes the two new
    #    columns, so none of the reference triggers re-fire (their WHEN
    #    clauses watch the license/resource/date columns only).
    op.execute("""
        UPDATE reference r
        SET image_count = compute_reference_image_count(r.reference_id),
            can_display_image = compute_reference_can_display_image(r.reference_id)
    """)


def downgrade():
    # 1. Drop triggers
    op.execute("DROP TRIGGER IF EXISTS reference_can_display_image_insert_trigger ON public.reference")
    op.execute("DROP TRIGGER IF EXISTS reference_can_display_image_update_trigger ON public.reference")
    op.execute("DROP TRIGGER IF EXISTS referencefile_image_count_insert_trigger ON public.referencefile")
    op.execute("DROP TRIGGER IF EXISTS referencefile_image_count_update_trigger ON public.referencefile")
    op.execute("DROP TRIGGER IF EXISTS referencefile_image_count_delete_trigger ON public.referencefile")
    op.execute("DROP TRIGGER IF EXISTS resource_can_display_image_update_trigger ON public.resource")
    op.execute("DROP TRIGGER IF EXISTS copyright_license_can_display_image_update_trigger ON public.copyright_license")
    op.execute("DROP TRIGGER IF EXISTS resource_image_permission_can_display_image_trigger ON public.resource_image_permission")
    op.execute("DROP TRIGGER IF EXISTS image_permission_can_display_image_update_trigger ON public.image_permission")

    # 2. Drop functions
    op.execute("DROP FUNCTION IF EXISTS reference_set_can_display_image()")
    op.execute("DROP FUNCTION IF EXISTS referencefile_update_image_count()")
    op.execute("DROP FUNCTION IF EXISTS resource_refresh_can_display_image()")
    op.execute("DROP FUNCTION IF EXISTS copyright_license_refresh_can_display_image()")
    op.execute("DROP FUNCTION IF EXISTS resource_image_permission_refresh_can_display_image()")
    op.execute("DROP FUNCTION IF EXISTS image_permission_refresh_can_display_image()")
    op.execute("DROP FUNCTION IF EXISTS refresh_can_display_image_for_resource(INTEGER)")
    op.execute("DROP FUNCTION IF EXISTS compute_reference_can_display_image(INTEGER)")
    op.execute("DROP FUNCTION IF EXISTS compute_reference_image_count(INTEGER)")
    op.execute("DROP FUNCTION IF EXISTS compute_can_display_image(INTEGER, INTEGER, INTEGER)")
    op.execute("DROP FUNCTION IF EXISTS extract_publication_year(TEXT, TEXT, TEXT)")

    # 3. Drop columns
    if _column_exists('reference_version', 'image_count_mod'):
        op.drop_column('reference_version', 'image_count_mod')
    if _column_exists('reference_version', 'image_count'):
        op.drop_column('reference_version', 'image_count')
    if _column_exists('reference_version', 'can_display_image_mod'):
        op.drop_column('reference_version', 'can_display_image_mod')
    if _column_exists('reference_version', 'can_display_image'):
        op.drop_column('reference_version', 'can_display_image')
    if _column_exists('reference', 'image_count'):
        op.drop_column('reference', 'image_count')
    if _column_exists('reference', 'can_display_image'):
        op.drop_column('reference', 'can_display_image')
