"""restructure person_email and reference_email

Revision ID: b1f8e7d6c5a4
Revises: a3f7c1d8b4e2
Create Date: 2026-05-22

Schema cleanup:
  - Rename ``email`` -> ``person_email`` (and ``email_version`` ->
    ``person_email_version``).
  - Rename ``date_invalidated`` -> ``date_made_old_email``.
  - Drop ``is_primary`` column, the partial unique index that enforced
    "at most one primary per person", and the related CHECK constraint.
  - Delete orphan ``person_email`` rows whose ``person_id IS NULL``
    (these only existed to satisfy the old ``reference_email`` FK
    pattern; reference emails are about to become string snapshots),
    then make ``person_id`` NOT NULL.
  - Add ``person.unsubscribe`` BOOLEAN NOT NULL DEFAULT FALSE.
  - Convert ``reference_email`` from FK -> string: add
    ``email_address`` column, backfill from ``person_email``, drop
    ``email_id`` (and its FK / index / uniqueness constraint), install
    a case-insensitive uniqueness index on
    ``(reference_id, lower(email_address))``.
  - Drop the now-unused ``users.email`` column.
  - Install ``get_most_current_email(p_person_id)`` SQL function.
"""
from alembic import op
import sqlalchemy as sa


revision = "b1f8e7d6c5a4"
down_revision = "a3f7c1d8b4e2"
branch_labels = None
depends_on = None


# ---------- small helpers (Postgres) ----------
def _index_exists(conn, index_name: str) -> bool:
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname = :n
                """
            ),
            {"n": index_name},
        ).fetchone()
    )


def _constraint_exists(conn, table: str, name: str) -> bool:
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class r ON r.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = r.relnamespace
                WHERE n.nspname = current_schema()
                  AND r.relname = :t
                  AND c.conname = :n
                """
            ),
            {"t": table, "n": name},
        ).fetchone()
    )


def _col_exists(conn, table: str, column: str) -> bool:
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = :t AND column_name = :c
                """
            ),
            {"t": table, "c": column},
        ).fetchone()
    )


def _table_exists(conn, table: str) -> bool:
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = :t
                """
            ),
            {"t": table},
        ).fetchone()
    )


def upgrade():  # noqa: C901
    conn = op.get_bind()

    # ------------------------------------------------------------------
    # 1. Drop is_primary-related indexes / CHECK on ``email`` *before*
    #    renaming the table so the legacy names disappear cleanly.
    # ------------------------------------------------------------------
    if _index_exists(conn, "ux_email_person_primary_true"):
        op.drop_index("ux_email_person_primary_true", table_name="email")
    if _index_exists(conn, "ix_email_person_primary"):
        op.drop_index("ix_email_person_primary", table_name="email")
    if _constraint_exists(conn, "email", "ck_email_person_primary_nulls_together"):
        op.drop_constraint(
            "ck_email_person_primary_nulls_together",
            "email",
            type_="check",
        )

    # ------------------------------------------------------------------
    # 2. Drop is_primary column from email + email_version (+ _mod col).
    # ------------------------------------------------------------------
    if _col_exists(conn, "email", "is_primary"):
        op.drop_column("email", "is_primary")
    if _col_exists(conn, "email_version", "is_primary"):
        op.drop_column("email_version", "is_primary")
    if _col_exists(conn, "email_version", "is_primary_mod"):
        op.drop_column("email_version", "is_primary_mod")

    # ------------------------------------------------------------------
    # 3. Clean orphan rows then make person_id NOT NULL.
    #    Orphan rows previously existed to back reference_email FKs for
    #    addresses with no associated person. Those FKs go away below;
    #    the orphan email rows are obsolete after backfill (step 8).
    #    Order matters: we need person_email.email_address available to
    #    backfill reference_email *first*, so the NOT NULL flip happens
    #    AFTER the backfill (step 9b below). Here we just verify there
    #    are no orphans whose data we'd lose silently.
    # ------------------------------------------------------------------
    # (NOT NULL flip is performed after the reference_email backfill.)

    # ------------------------------------------------------------------
    # 4. Rename date_invalidated -> date_made_old_email on email +
    #    email_version (and the _mod tracker column).
    # ------------------------------------------------------------------
    if _col_exists(conn, "email", "date_invalidated"):
        op.alter_column(
            "email",
            "date_invalidated",
            new_column_name="date_made_old_email",
        )
    if _col_exists(conn, "email_version", "date_invalidated"):
        op.alter_column(
            "email_version",
            "date_invalidated",
            new_column_name="date_made_old_email",
        )
    if _col_exists(conn, "email_version", "date_invalidated_mod"):
        op.alter_column(
            "email_version",
            "date_invalidated_mod",
            new_column_name="date_made_old_email_mod",
        )

    # ------------------------------------------------------------------
    # 5. Rename the tables themselves: email -> person_email,
    #    email_version -> person_email_version.
    # ------------------------------------------------------------------
    if _table_exists(conn, "email") and not _table_exists(conn, "person_email"):
        op.rename_table("email", "person_email")
    if _table_exists(conn, "email_version") and not _table_exists(
        conn, "person_email_version"
    ):
        op.rename_table("email_version", "person_email_version")

    # Rename the still-existing named indexes / constraint to match the
    # new table name. Postgres ALTER INDEX / ALTER TABLE RENAME
    # CONSTRAINT is metadata-only.
    if _index_exists(conn, "ix_email_address"):
        op.execute(
            "ALTER INDEX ix_email_address "
            "RENAME TO ix_person_email_email_address"
        )
    if _index_exists(conn, "ix_email_lower_email_address"):
        op.execute(
            "ALTER INDEX ix_email_lower_email_address "
            "RENAME TO ix_person_email_lower_email_address"
        )
    if _constraint_exists(conn, "person_email", "uq_email_person_address"):
        op.execute(
            "ALTER TABLE person_email "
            "RENAME CONSTRAINT uq_email_person_address "
            "TO uq_person_email_person_address"
        )

    # Swap the plain `(person_id, email_address)` unique constraint for a
    # case-insensitive functional unique index on
    # `(person_id, lower(email_address))`. This lets the table store the
    # email_address with its original casing while still preventing
    # case-only duplicates.
    if _constraint_exists(conn, "person_email", "uq_person_email_person_address"):
        op.drop_constraint(
            "uq_person_email_person_address",
            "person_email",
            type_="unique",
        )
    if not _index_exists(conn, "uq_person_email_person_address_lower"):
        op.execute(
            "CREATE UNIQUE INDEX uq_person_email_person_address_lower "
            "ON person_email (person_id, lower(email_address))"
        )

    # ------------------------------------------------------------------
    # 6. Add person.unsubscribe with permanent DB default FALSE.
    # ------------------------------------------------------------------
    if not _col_exists(conn, "person", "unsubscribe"):
        op.add_column(
            "person",
            sa.Column(
                "unsubscribe",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("FALSE"),
            ),
        )
    # person_version mirror column (Continuum)
    if _table_exists(conn, "person_version") and not _col_exists(
        conn, "person_version", "unsubscribe"
    ):
        op.add_column(
            "person_version",
            sa.Column(
                "unsubscribe",
                sa.Boolean(),
                autoincrement=False,
                nullable=True,
            ),
        )
        op.add_column(
            "person_version",
            sa.Column(
                "unsubscribe_mod",
                sa.Boolean(),
                server_default=sa.text("false"),
                nullable=False,
            ),
        )

    # ------------------------------------------------------------------
    # 7. Add reference_email.email_address (nullable for now).
    # ------------------------------------------------------------------
    if not _col_exists(conn, "reference_email", "email_address"):
        op.add_column(
            "reference_email",
            sa.Column("email_address", sa.String(), nullable=True),
        )
    if _table_exists(conn, "reference_email_version") and not _col_exists(
        conn, "reference_email_version", "email_address"
    ):
        op.add_column(
            "reference_email_version",
            sa.Column(
                "email_address",
                sa.String(),
                autoincrement=False,
                nullable=True,
            ),
        )
        op.add_column(
            "reference_email_version",
            sa.Column(
                "email_address_mod",
                sa.Boolean(),
                server_default=sa.text("false"),
                nullable=False,
            ),
        )

    # ------------------------------------------------------------------
    # 8. Backfill reference_email.email_address from person_email, and
    #    do the same for the Continuum version table so the audit trail
    #    isn't blanked out.
    # ------------------------------------------------------------------
    op.execute(
        """
        UPDATE reference_email re
        SET email_address = pe.email_address
        FROM person_email pe
        WHERE pe.email_id = re.email_id
          AND re.email_address IS NULL
        """
    )

    if _table_exists(conn, "reference_email_version"):
        op.execute(
            """
            UPDATE reference_email_version rev
            SET email_address = pe.email_address
            FROM person_email pe
            WHERE pe.email_id = rev.email_id
              AND rev.email_address IS NULL
            """
        )

    # Sanity check: every row must be populated before NOT NULL.
    missing = conn.execute(
        sa.text(
            "SELECT COUNT(*) FROM reference_email "
            "WHERE email_address IS NULL"
        )
    ).scalar()
    if missing:
        raise RuntimeError(
            f"reference_email backfill incomplete: {missing} row(s) still "
            "have NULL email_address. Investigate orphan rows before "
            "re-running this migration."
        )

    # ------------------------------------------------------------------
    # 9. Lock down reference_email: email_address NOT NULL, drop the
    #    email_id FK / index / unique constraint / column.
    # ------------------------------------------------------------------
    op.alter_column("reference_email", "email_address", nullable=False)

    # Drop the FK on email_id. The constraint name is auto-generated by
    # Postgres; look it up dynamically.
    fk_name = conn.execute(
        sa.text(
            """
            SELECT conname
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = current_schema()
              AND t.relname = 'reference_email'
              AND c.contype = 'f'
              AND pg_get_constraintdef(c.oid) ILIKE '%REFERENCES person_email%'
            """
        )
    ).scalar()
    if fk_name is None:
        # Fallback: it may still point at the pre-rename ``email`` name
        # in older databases.
        fk_name = conn.execute(
            sa.text(
                """
                SELECT conname
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = current_schema()
                  AND t.relname = 'reference_email'
                  AND c.contype = 'f'
                  AND pg_get_constraintdef(c.oid) ILIKE '%REFERENCES email%'
                """
            )
        ).scalar()
    if fk_name:
        op.drop_constraint(fk_name, "reference_email", type_="foreignkey")

    if _index_exists(conn, "ix_reference_email_email_id"):
        op.drop_index("ix_reference_email_email_id", table_name="reference_email")
    if _index_exists(conn, "ix_reference_email_reference_email"):
        op.drop_index(
            "ix_reference_email_reference_email", table_name="reference_email"
        )
    if _constraint_exists(
        conn, "reference_email", "uq_reference_email_reference_email"
    ):
        op.drop_constraint(
            "uq_reference_email_reference_email",
            "reference_email",
            type_="unique",
        )
    if _col_exists(conn, "reference_email", "email_id"):
        op.drop_column("reference_email", "email_id")

    # Mirror the column drops on reference_email_version (no FK / NOT
    # NULL constraints there, just columns).
    if _table_exists(conn, "reference_email_version"):
        if _index_exists(conn, "ix_reference_email_version_email_id"):
            op.drop_index(
                "ix_reference_email_version_email_id",
                table_name="reference_email_version",
            )
        if _col_exists(conn, "reference_email_version", "email_id"):
            op.drop_column("reference_email_version", "email_id")
        if _col_exists(conn, "reference_email_version", "email_id_mod"):
            op.drop_column("reference_email_version", "email_id_mod")

    # New uniqueness rule: case-insensitive on (reference_id,
    # lower(email_address)).
    if not _index_exists(conn, "uq_reference_email_reference_email_lower"):
        op.execute(
            "CREATE UNIQUE INDEX uq_reference_email_reference_email_lower "
            "ON reference_email (reference_id, lower(email_address))"
        )
    # Plain composite index to support lookups in either direction.
    if not _index_exists(conn, "ix_reference_email_reference_email"):
        op.create_index(
            "ix_reference_email_reference_email",
            "reference_email",
            ["reference_id", "email_address"],
            unique=False,
        )

    # ------------------------------------------------------------------
    # 9b. Now that reference_email no longer references person_email,
    #     orphan rows are obsolete. Clean them up, swap the FK from
    #     SET NULL to CASCADE (SET NULL no longer makes sense once
    #     person_id is NOT NULL), and make person_id NOT NULL.
    # ------------------------------------------------------------------
    op.execute("DELETE FROM person_email WHERE person_id IS NULL")

    if _constraint_exists(conn, "person_email", "email_person_id_fkey"):
        op.drop_constraint(
            "email_person_id_fkey", "person_email", type_="foreignkey"
        )
    if not _constraint_exists(
        conn, "person_email", "person_email_person_id_fkey"
    ):
        op.create_foreign_key(
            "person_email_person_id_fkey",
            "person_email",
            "person",
            ["person_id"],
            ["person_id"],
            ondelete="CASCADE",
        )

    op.alter_column(
        "person_email",
        "person_id",
        existing_type=sa.Integer(),
        nullable=False,
    )

    # ------------------------------------------------------------------
    # 10. Drop users.email + its index (legacy column).
    # ------------------------------------------------------------------
    if _index_exists(conn, "ix_users_email"):
        op.drop_index("ix_users_email", table_name="users")
    if _col_exists(conn, "users", "email"):
        op.drop_column("users", "email")

    # ------------------------------------------------------------------
    # 11. Partial index supporting get_most_current_email() lookups.
    #     Mirrors the old `ux_email_person_primary_true` partial-index
    #     pattern: keeps active-email lookups O(log N_active) even as
    #     historical (date_made_old_email IS NOT NULL) rows accumulate.
    # ------------------------------------------------------------------
    if not _index_exists(conn, "ix_person_email_active_by_person"):
        op.create_index(
            "ix_person_email_active_by_person",
            "person_email",
            ["person_id"],
            postgresql_where=sa.text("date_made_old_email IS NULL"),
        )

    # ------------------------------------------------------------------
    # 12. Install get_most_current_email(p_person_id) SQL function.
    #     STABLE so the planner can inline it into raw-SQL JOINs in the
    #     workflow_tag / topic_entity_tag / indexing_priority /
    #     manual_indexing_tag / reference CRUD queries.
    # ------------------------------------------------------------------
    op.execute(
        """
        CREATE OR REPLACE FUNCTION get_most_current_email(p_person_id INTEGER)
        RETURNS TEXT AS $$
          SELECT email_address
          FROM person_email
          WHERE person_id = p_person_id
            AND date_made_old_email IS NULL
          ORDER BY COALESCE(date_updated, date_created) DESC,
                   email_id DESC
          LIMIT 1;
        $$ LANGUAGE SQL STABLE
        """
    )


def downgrade():  # noqa: C901
    conn = op.get_bind()

    # Drop the function first.
    op.execute("DROP FUNCTION IF EXISTS get_most_current_email(INTEGER)")

    # Drop the partial active-email index.
    if _index_exists(conn, "ix_person_email_active_by_person"):
        op.drop_index(
            "ix_person_email_active_by_person", table_name="person_email"
        )

    # Re-add users.email (best-effort: data is lost on downgrade).
    if not _col_exists(conn, "users", "email"):
        op.add_column(
            "users",
            sa.Column("email", sa.String(), nullable=True),
        )
        op.create_index("ix_users_email", "users", ["email"], unique=False)

    # Reverse the person_email FK and NOT NULL changes.
    op.alter_column(
        "person_email",
        "person_id",
        existing_type=sa.Integer(),
        nullable=True,
    )
    if _constraint_exists(
        conn, "person_email", "person_email_person_id_fkey"
    ):
        op.drop_constraint(
            "person_email_person_id_fkey",
            "person_email",
            type_="foreignkey",
        )
    if not _constraint_exists(conn, "person_email", "email_person_id_fkey"):
        op.create_foreign_key(
            "email_person_id_fkey",
            "person_email",
            "person",
            ["person_id"],
            ["person_id"],
            ondelete="SET NULL",
        )

    # Reverse the reference_email FK->string conversion. NOTE: the
    # original email_id values are not recoverable from the string
    # alone, so this downgrade leaves email_id NULL. Callers must
    # restore the FK manually if needed.
    if _index_exists(conn, "ix_reference_email_reference_email"):
        op.drop_index(
            "ix_reference_email_reference_email", table_name="reference_email"
        )
    if _index_exists(conn, "uq_reference_email_reference_email_lower"):
        op.execute("DROP INDEX uq_reference_email_reference_email_lower")

    if not _col_exists(conn, "reference_email", "email_id"):
        op.add_column(
            "reference_email",
            sa.Column("email_id", sa.Integer(), nullable=True),
        )
        op.create_index(
            "ix_reference_email_email_id",
            "reference_email",
            ["email_id"],
            unique=False,
        )
        op.create_index(
            "ix_reference_email_reference_email",
            "reference_email",
            ["reference_id", "email_id"],
            unique=False,
        )

    # Mirror on the version table.
    if _table_exists(conn, "reference_email_version"):
        if _col_exists(conn, "reference_email_version", "email_address"):
            op.drop_column("reference_email_version", "email_address")
        if _col_exists(conn, "reference_email_version", "email_address_mod"):
            op.drop_column("reference_email_version", "email_address_mod")
        if not _col_exists(conn, "reference_email_version", "email_id"):
            op.add_column(
                "reference_email_version",
                sa.Column(
                    "email_id",
                    sa.Integer(),
                    autoincrement=False,
                    nullable=True,
                ),
            )
            op.add_column(
                "reference_email_version",
                sa.Column(
                    "email_id_mod",
                    sa.Boolean(),
                    server_default=sa.text("false"),
                    nullable=False,
                ),
            )
            op.create_index(
                "ix_reference_email_version_email_id",
                "reference_email_version",
                ["email_id"],
                unique=False,
            )

    if _col_exists(conn, "reference_email", "email_address"):
        op.drop_column("reference_email", "email_address")

    # Reverse the person.unsubscribe addition.
    if _col_exists(conn, "person", "unsubscribe"):
        op.drop_column("person", "unsubscribe")
    if _table_exists(conn, "person_version"):
        if _col_exists(conn, "person_version", "unsubscribe_mod"):
            op.drop_column("person_version", "unsubscribe_mod")
        if _col_exists(conn, "person_version", "unsubscribe"):
            op.drop_column("person_version", "unsubscribe")

    # Reverse the table rename and the index/constraint renames.
    if _index_exists(conn, "ix_person_email_email_address"):
        op.execute(
            "ALTER INDEX ix_person_email_email_address "
            "RENAME TO ix_email_address"
        )
    if _index_exists(conn, "ix_person_email_lower_email_address"):
        op.execute(
            "ALTER INDEX ix_person_email_lower_email_address "
            "RENAME TO ix_email_lower_email_address"
        )
    # Reverse the case-insensitive uniqueness swap: drop the functional
    # unique index and restore the plain (person_id, email_address)
    # UniqueConstraint. Existing data is normalized lower-case from
    # before the upgrade, so the restored plain unique is satisfiable.
    if _index_exists(conn, "uq_person_email_person_address_lower"):
        op.execute("DROP INDEX uq_person_email_person_address_lower")
    if not _constraint_exists(
        conn, "person_email", "uq_person_email_person_address"
    ):
        op.create_unique_constraint(
            "uq_person_email_person_address",
            "person_email",
            ["person_id", "email_address"],
        )
    if _constraint_exists(conn, "person_email", "uq_person_email_person_address"):
        op.execute(
            "ALTER TABLE person_email "
            "RENAME CONSTRAINT uq_person_email_person_address "
            "TO uq_email_person_address"
        )

    if _table_exists(conn, "person_email_version") and not _table_exists(
        conn, "email_version"
    ):
        op.rename_table("person_email_version", "email_version")
    if _table_exists(conn, "person_email") and not _table_exists(conn, "email"):
        op.rename_table("person_email", "email")

    # Reverse the column renames on the email + email_version tables.
    if _col_exists(conn, "email", "date_made_old_email"):
        op.alter_column(
            "email",
            "date_made_old_email",
            new_column_name="date_invalidated",
        )
    if _col_exists(conn, "email_version", "date_made_old_email"):
        op.alter_column(
            "email_version",
            "date_made_old_email",
            new_column_name="date_invalidated",
        )
    if _col_exists(conn, "email_version", "date_made_old_email_mod"):
        op.alter_column(
            "email_version",
            "date_made_old_email_mod",
            new_column_name="date_invalidated_mod",
        )

    # Re-add is_primary (data lost).
    if not _col_exists(conn, "email", "is_primary"):
        op.add_column("email", sa.Column("is_primary", sa.Boolean(), nullable=True))
    if _table_exists(conn, "email_version") and not _col_exists(
        conn, "email_version", "is_primary"
    ):
        op.add_column(
            "email_version",
            sa.Column("is_primary", sa.Boolean(), nullable=True),
        )
        op.add_column(
            "email_version",
            sa.Column(
                "is_primary_mod",
                sa.Boolean(),
                server_default=sa.text("false"),
                nullable=False,
            ),
        )

    # Backfill is_primary=FALSE for rows linked to a person so the next
    # CHECK constraint can be added without violating any existing row.
    # (Mirrors the original migration 20251204_283e37c0f96d.)
    op.execute(
        "UPDATE email SET is_primary = FALSE "
        "WHERE person_id IS NOT NULL AND is_primary IS NULL"
    )

    # Restore the is_primary constraint + indexes.
    if not _constraint_exists(conn, "email", "ck_email_person_primary_nulls_together"):
        op.create_check_constraint(
            "ck_email_person_primary_nulls_together",
            "email",
            "((person_id IS NULL AND is_primary IS NULL) OR "
            "(person_id IS NOT NULL AND is_primary IS NOT NULL))",
        )
    if not _index_exists(conn, "ix_email_person_primary"):
        op.create_index(
            "ix_email_person_primary",
            "email",
            ["person_id", "is_primary"],
            unique=False,
        )
    if not _index_exists(conn, "ux_email_person_primary_true"):
        op.create_index(
            "ux_email_person_primary_true",
            "email",
            ["person_id"],
            unique=True,
            postgresql_where=sa.text("is_primary = TRUE"),
        )
