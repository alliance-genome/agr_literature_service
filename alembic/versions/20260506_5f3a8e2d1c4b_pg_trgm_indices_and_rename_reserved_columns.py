"""add pg_trgm/functional indices and rename reserved-word columns

Revision ID: 5f3a8e2d1c4b
Revises: 5653a1b2c3d4
Create Date: 2026-05-06

Two related groups of changes:

1. Index hygiene
   - Replace the misnamed plain btree on ``person.display_name`` (originally
     ``ix_person_display_name_trigram``) with a real GIN trigram.
   - Add GIN trigrams on ``person_name.{first,middle,last}_name``.
   - Add a functional ``lower(email_address)`` btree on ``email`` so that
     ``func.lower(...) == :norm`` lookups can use an index. The plain
     ``ix_email_address`` btree is left in place because raw equality
     queries on ``email_address`` still exist in other CRUDs.

2. Rename reserved-word columns
   - ``email.primary`` -> ``is_primary`` (PG-reserved word; previously
     worked around with ``Column("primary", ...)``).
   - ``person_name.primary`` -> ``is_primary`` (same).
   - ``author.order`` -> ``author_order`` (PG-reserved word).
   - ``editor.order`` -> ``editor_order`` (same).
   The ``_version`` shadow tables (sqlalchemy-continuum) and their
   ``_mod`` tracker columns are renamed to match.
"""
from alembic import op

from agr_literature_service.api.triggers.citation_sql_func_triggers import (
    citation_update,
)


# revision identifiers, used by Alembic.
revision = "5f3a8e2d1c4b"
down_revision = "5653a1b2c3d4"
branch_labels = None
depends_on = None


def upgrade():
    # --- 1. Index hygiene -------------------------------------------------
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.drop_index("ix_person_display_name_trigram", table_name="person")
    op.create_index(
        "ix_person_display_name_trgm",
        "person",
        ["display_name"],
        postgresql_using="gin",
        postgresql_ops={"display_name": "gin_trgm_ops"},
    )

    for col in ("first_name", "middle_name", "last_name"):
        op.create_index(
            f"ix_person_name_{col}_trgm",
            "person_name",
            [col],
            postgresql_using="gin",
            postgresql_ops={col: "gin_trgm_ops"},
        )

    op.execute(
        "CREATE INDEX ix_email_lower_email_address "
        "ON email (lower(email_address))"
    )

    # --- 2. Rename reserved-word columns ---------------------------------
    # Postgres ALTER TABLE ... RENAME COLUMN auto-rewrites the column
    # references inside dependent indices, partial-index WHERE clauses,
    # and CHECK constraints. The constraint/index *names* keep their
    # legacy "primary" labels - purely cosmetic, no functional impact.
    op.alter_column("email", "primary", new_column_name="is_primary")
    op.alter_column("email_version", "primary", new_column_name="is_primary")
    op.alter_column(
        "email_version", "primary_mod", new_column_name="is_primary_mod"
    )

    op.alter_column(
        "person_name", "primary", new_column_name="is_primary"
    )
    op.alter_column(
        "person_name_version", "primary", new_column_name="is_primary"
    )
    op.alter_column(
        "person_name_version",
        "primary_mod",
        new_column_name="is_primary_mod",
    )

    op.alter_column("author", "order", new_column_name="author_order")
    op.alter_column(
        "author_version", "order", new_column_name="author_order"
    )
    op.alter_column(
        "author_version", "order_mod", new_column_name="author_order_mod"
    )

    op.alter_column("editor", "order", new_column_name="editor_order")
    op.alter_column(
        "editor_version", "order", new_column_name="editor_order"
    )
    op.alter_column(
        "editor_version", "order_mod", new_column_name="editor_order_mod"
    )

    # Postgres ALTER TABLE RENAME COLUMN does NOT rewrite PL/pgSQL function
    # bodies. The update_citations procedure references author.author_order
    # in its body; reinstall it so writes to author don't fail between this
    # migration and the next API restart (which also reinstalls it via
    # add_citation_methods).
    op.execute(citation_update)


def downgrade():
    # Reverse the column renames first (call sites assume the new names).
    op.alter_column(
        "editor_version", "editor_order_mod", new_column_name="order_mod"
    )
    op.alter_column(
        "editor_version", "editor_order", new_column_name="order"
    )
    op.alter_column("editor", "editor_order", new_column_name="order")

    op.alter_column(
        "author_version", "author_order_mod", new_column_name="order_mod"
    )
    op.alter_column(
        "author_version", "author_order", new_column_name="order"
    )
    op.alter_column("author", "author_order", new_column_name="order")

    op.alter_column(
        "person_name_version",
        "is_primary_mod",
        new_column_name="primary_mod",
    )
    op.alter_column(
        "person_name_version", "is_primary", new_column_name="primary"
    )
    op.alter_column(
        "person_name", "is_primary", new_column_name="primary"
    )

    op.alter_column(
        "email_version", "is_primary_mod", new_column_name="primary_mod"
    )
    op.alter_column(
        "email_version", "is_primary", new_column_name="primary"
    )
    op.alter_column("email", "is_primary", new_column_name="primary")

    # Then reverse the index hygiene changes.
    op.execute("DROP INDEX IF EXISTS ix_email_lower_email_address")
    for col in ("last_name", "middle_name", "first_name"):
        op.drop_index(f"ix_person_name_{col}_trgm", table_name="person_name")
    op.drop_index("ix_person_display_name_trgm", table_name="person")
    op.create_index(
        "ix_person_display_name_trigram",
        "person",
        ["display_name"],
        unique=False,
    )
    # pg_trgm extension intentionally left in place: dropping it would
    # CASCADE-drop any other GIN trigram index that may exist.
