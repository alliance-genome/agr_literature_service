"""make topic_entity_tag.data_context NOT NULL

Revision ID: e4f9a2c81b57
Revises: d7b3e1c95a24
Create Date: 2026-08-31

SCRUM-5697, second half. Revision d7b3e1c95a24 added ``data_context`` nullable so
the producers could be updated one at a time; this one closes it, matching
``data_novelty``.

Run order matters. Before applying this:

  1. every TET-creating client sends a data_context (or relies on create_tag's
     default -- ``set_provider_derived_fields`` in ``topic_entity_tag_crud``
     fills one in for every provider, so the API path is already covered), and
  2. ``lit_processing/oneoff_scripts/SCRUM-5697_backfill_data_context.py`` has
     run to completion.

The pre-flight check below aborts before any DDL if NULLs remain, so a premature
run fails with a useful message instead of a bare constraint violation.

``topic_entity_tag_version.data_context`` stays nullable, exactly as
``data_novelty`` does: historical version rows predate the column and there is
nothing meaningful to backfill them with.
"""
from alembic import op
import sqlalchemy as sa


revision = 'e4f9a2c81b57'
down_revision = 'd7b3e1c95a24'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    remaining = conn.execute(sa.text(
        "SELECT count(*) FROM topic_entity_tag WHERE data_context IS NULL")).scalar() or 0
    if remaining:
        raise RuntimeError(
            f"Aborting migration e4f9a2c81b57 (data_context NOT NULL) before any DDL - "
            f"{remaining} topic_entity_tag row(s) still have a NULL data_context.\n"
            f"Run agr_literature_service/lit_processing/oneoff_scripts/"
            f"SCRUM-5697_backfill_data_context.py first, then re-run the migration.")

    op.alter_column('topic_entity_tag', 'data_context',
                    existing_type=sa.String(),
                    nullable=False)


def downgrade():
    op.alter_column('topic_entity_tag', 'data_context',
                    existing_type=sa.String(),
                    nullable=True)
