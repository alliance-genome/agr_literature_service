"""author: person_id link + person/author constraints

Revision ID: 9a1c7f2e4d10
Revises: 8603439f2008
Create Date: 2026-08-07
"""
from alembic import op
import sqlalchemy as sa

revision = "9a1c7f2e4d10"
down_revision = "8603439f2008"
branch_labels = None
depends_on = None


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


def upgrade():
    # Pre-flight guard: abort BEFORE any locking DDL if legacy `author` data would
    # violate the new constraints, so a production `alembic upgrade` on dirty data
    # fails fast (nothing locked, nothing half-applied) with an actionable message
    # instead of dying part-way through. person_id is all-NULL until it is added
    # below, so only these three pre-existing conditions can break the migration.
    conn = op.get_bind()
    null_ref = conn.execute(sa.text(
        "SELECT count(*) FROM author WHERE reference_id IS NULL")).scalar() or 0
    null_order = conn.execute(sa.text(
        "SELECT count(*) FROM author WHERE author_order IS NULL")).scalar() or 0
    dup_order = conn.execute(sa.text(
        "SELECT count(*) FROM (SELECT reference_id, author_order FROM author "
        "WHERE author_order IS NOT NULL GROUP BY reference_id, author_order "
        "HAVING count(*) > 1) d")).scalar() or 0
    problems = []
    if null_ref:
        problems.append(f"{null_ref} row(s) with NULL reference_id (breaks reference_id NOT NULL)")
    if null_order:
        problems.append(f"{null_order} row(s) with NULL author_order (breaks ck_author_person_or_order)")
    if dup_order:
        problems.append(f"{dup_order} duplicate (reference_id, author_order) group(s) (breaks uq_author_ref_order)")
    if problems:
        raise RuntimeError(
            "Aborting migration 9a1c7f2e4d10 (author person_id link) before any DDL - "
            "legacy author data violates the new constraints:\n  - "
            + "\n  - ".join(problems)
            + "\nClean up these rows, then re-run the migration.")

    op.add_column("author", sa.Column("person_id", sa.Integer(), nullable=True))
    op.create_foreign_key("author_person_id_fkey", "author", "person",
                          ["person_id"], ["person_id"], ondelete="RESTRICT")
    op.create_index("ix_author_person_id", "author", ["person_id"])

    op.alter_column("author", "reference_id", existing_type=sa.Integer(), nullable=False)

    op.create_check_constraint("ck_author_person_or_order", "author",
                               "person_id IS NOT NULL OR author_order IS NOT NULL")
    op.create_check_constraint(
        "ck_person_only_link_only", "author",
        "author_order IS NOT NULL OR ("
        "name IS NULL AND first_name IS NULL AND last_name IS NULL "
        "AND first_initial IS NULL AND orcid IS NULL AND affiliations IS NULL "
        "AND COALESCE(first_author, false) = false "
        "AND COALESCE(corresponding_author, false) = false)")

    op.create_index("uq_author_ref_person", "author", ["reference_id", "person_id"], unique=True)
    # Deferrable so a single-statement renumber (POST /author/reorder) can swap
    # author_order values without tripping the per-row uniqueness check mid-UPDATE.
    op.execute("ALTER TABLE author ADD CONSTRAINT uq_author_ref_order "
               "UNIQUE (reference_id, author_order) DEFERRABLE INITIALLY IMMEDIATE")

    # author is SQLAlchemy-Continuum versioned, so the person_id column must also
    # exist on author_version - otherwise every author INSERT/UPDATE/DELETE fails
    # with `column "person_id" of relation "author_version" does not exist`.
    # create_all() (dev/test) already builds these from the model, so guard with
    # _col_exists to stay idempotent. No FK/index/NOT NULL/CHECK here: Continuum
    # delete-version rows are mostly NULL and would violate any such constraint.
    if not _col_exists(conn, "author_version", "person_id"):
        op.add_column("author_version",
                      sa.Column("person_id", sa.Integer(), autoincrement=False, nullable=True))
    if not _col_exists(conn, "author_version", "person_id_mod"):
        op.add_column("author_version",
                      sa.Column("person_id_mod", sa.Boolean(),
                                server_default=sa.text("false"), nullable=False))


def downgrade():
    op.drop_column("author_version", "person_id_mod")
    op.drop_column("author_version", "person_id")
    op.drop_constraint("uq_author_ref_order", "author", type_="unique")
    op.drop_index("uq_author_ref_person", table_name="author")
    op.drop_constraint("ck_person_only_link_only", "author", type_="check")
    op.drop_constraint("ck_author_person_or_order", "author", type_="check")
    op.alter_column("author", "reference_id", existing_type=sa.Integer(), nullable=True)
    op.drop_index("ix_author_person_id", table_name="author")
    op.drop_constraint("author_person_id_fkey", "author", type_="foreignkey")
    op.drop_column("author", "person_id")
