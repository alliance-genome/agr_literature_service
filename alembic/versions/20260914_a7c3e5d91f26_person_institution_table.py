"""move person.institution into a person_institution child table

Revision ID: a7c3e5d91f26
Revises: e4f9a2c81b57
Create Date: 2026-09-14

``person.institution`` was an ARRAY(String) with no way to record that a person
*used to be* somewhere. This replaces it with a ``person_institution`` child
table carrying ``date_made_old_institution``, mirroring how ``person_email``
records old addresses via ``date_made_old_email``.

Deliberately unlike ``person_email``:
  - No uniqueness index on (person_id, institution). People return to
    institutions (Caltech -> MIT -> Caltech), so an old row and a new active row
    must be able to hold the same string.
  - No lower() functional index and no partial active-row index: nothing looks
    an institution up by value, and there is no get_most_current_institution()
    SQL function for such an index to serve.

Data: verified on prod 2026-09-14 -- 5,634 of 12,580 persons have an
institution and every array holds exactly one element, with no blank or
whitespace-padded values. The backfill nonetheless unnests the array rather
than taking ``institution[1]`` (see the note on the INSERT), and marks every
migrated row active; the array carried no old/current signal, so nothing else
can be inferred.

The historical ``person_version.institution`` values are discarded rather than
synthesized into ``person_institution_version``: person-level version rows
cannot be mapped onto child-row versions without inventing transaction ids,
which would corrupt the continuum chain.
"""
from alembic import op
import sqlalchemy as sa


revision = "a7c3e5d91f26"
down_revision = "e4f9a2c81b57"
branch_labels = None
depends_on = None


# ---------- small helpers (Postgres) ----------
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


def _col_exists(conn, table: str, col: str) -> bool:
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = :t
                  AND column_name = :c
                """
            ),
            {"t": table, "c": col},
        ).fetchone()
    )


def _audit_columns():
    return [
        sa.Column("date_created", sa.DateTime(), nullable=False),
        sa.Column("date_updated", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.Column("updated_by", sa.String(), nullable=True),
    ]


# Audit columns tracked by continuum (each carries a *_mod column in version tables).
_AUDIT_TRACKED = [
    ("date_created", sa.DateTime()),
    ("date_updated", sa.DateTime()),
    ("created_by", sa.String()),
    ("updated_by", sa.String()),
]

_PERSON_INSTITUTION_TRACKED = [
    ("person_id", sa.Integer()),
    ("institution", sa.String()),
    ("date_made_old_institution", sa.DateTime()),
]


def _create_version_table(table: str, pk_col: str, tracked_cols, indexed_cols):
    """Create a continuum *_version table (same shape as d4e9c2b7a8f1's helper)."""
    vtable = f"{table}_version"
    all_tracked = list(tracked_cols) + _AUDIT_TRACKED

    cols = [sa.Column(pk_col, sa.Integer(), autoincrement=False, nullable=False)]
    for name, typ in all_tracked:
        cols.append(sa.Column(name, typ, autoincrement=False, nullable=True))
    cols += [
        sa.Column("transaction_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.SmallInteger(), nullable=False),
    ]
    for name, _typ in all_tracked:
        cols.append(
            sa.Column(f"{name}_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False)
        )

    op.create_table(vtable, *cols, sa.PrimaryKeyConstraint(pk_col, "transaction_id"))
    op.create_index(
        op.f(f"ix_{vtable}_transaction_id"), vtable, ["transaction_id"], unique=False
    )
    op.create_index(
        op.f(f"ix_{vtable}_end_transaction_id"), vtable, ["end_transaction_id"], unique=False
    )
    op.create_index(
        op.f(f"ix_{vtable}_operation_type"), vtable, ["operation_type"], unique=False
    )
    for name in indexed_cols:
        op.create_index(op.f(f"ix_{vtable}_{name}"), vtable, [name], unique=False)
    # AuditedModel declares date_created / date_updated with index=True, and
    # continuum mirrors that onto the version table -- so every other
    # *_version table (person_email, person_name, person_note, laboratory)
    # carries these two. Create them here or this table is the odd one out,
    # and the drift is invisible to CI because the test DB is built by
    # create_all(), which adds them.
    for name in ("date_created", "date_updated"):
        op.create_index(op.f(f"ix_{vtable}_{name}"), vtable, [name], unique=False)


def upgrade():
    conn = op.get_bind()

    # ------------------------------------------------------------------
    # 1. person_institution
    # ------------------------------------------------------------------
    if not _table_exists(conn, "person_institution"):
        op.create_table(
            "person_institution",
            sa.Column(
                "person_institution_id", sa.Integer(), autoincrement=True, nullable=False
            ),
            sa.Column("person_id", sa.Integer(), nullable=False),
            sa.Column("institution", sa.String(), nullable=False),
            sa.Column("date_made_old_institution", sa.DateTime(), nullable=True),
            *_audit_columns(),
            sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
            sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
            sa.ForeignKeyConstraint(
                ["person_id"], ["person.person_id"], ondelete="CASCADE"
            ),
            sa.PrimaryKeyConstraint("person_institution_id"),
        )
        op.create_index(
            op.f("ix_person_institution_person_id"),
            "person_institution",
            ["person_id"],
            unique=False,
        )
        op.create_index(
            "ix_person_institution_institution",
            "person_institution",
            ["institution"],
            unique=False,
        )
        op.create_index(
            op.f("ix_person_institution_date_created"),
            "person_institution",
            ["date_created"],
            unique=False,
        )
        op.create_index(
            op.f("ix_person_institution_date_updated"),
            "person_institution",
            ["date_updated"],
            unique=False,
        )

    # ------------------------------------------------------------------
    # 2. person_institution_version (continuum)
    # ------------------------------------------------------------------
    if not _table_exists(conn, "person_institution_version"):
        _create_version_table(
            "person_institution",
            "person_institution_id",
            _PERSON_INSTITUTION_TRACKED,
            ["person_id"],
        )

    # ------------------------------------------------------------------
    # 3. Backfill from person.institution.
    #    Every populated array on prod holds exactly one element, but the API
    #    accepts multi-element arrays right up until this migration deploys, so
    #    the backfill unnests rather than taking institution[1] -- otherwise a
    #    row that gained a second element in the meantime would lose it
    #    silently. WITH ORDINALITY keeps array order, so person_institution_id
    #    ascends in the order the array held. Blank and NULL elements are
    #    dropped and the rest are trimmed (prod has none of either).
    #    All rows land active: the array carried no old/current signal.
    #
    #    The NOT EXISTS guard matters because person_institution can already
    #    hold rows before this migration runs: the API's startup create_all()
    #    builds any table missing from the DB, so restarting the API on the new
    #    code materializes this table while alembic is still behind. In that
    #    window a curator sees an empty Institutions section (the array is not
    #    migrated yet) and may retype an institution by hand -- observed on the
    #    4002 dev server. Without the guard the backfill then inserts the array
    #    copy on top, leaving two identical active rows for that person. The
    #    guard skips only an exact (person, trimmed value) match, so array
    #    values the curator did NOT retype are still restored. It also makes the
    #    backfill safe to re-run.
    #
    #    date_updated is set equal to date_created, matching what
    #    AuditedModel.before_insert does for every row created through the ORM
    #    ("if date_created is not None and date_updated is None: date_updated =
    #    date_created"). Leaving it NULL would make these the only rows in any
    #    person child table without a date_updated -- person_email, person_name
    #    and person_note have none -- which shows up two ways: list_for_person
    #    orders by date_updated DESC NULLS LAST, so backfilled rows would always
    #    sort below hand-entered ones, and the editor renders no timestamp label
    #    at all for a row whose date_updated is null.
    # ------------------------------------------------------------------
    if _col_exists(conn, "person", "institution"):
        op.execute(
            """
            INSERT INTO person_institution
                (person_id, institution, date_made_old_institution,
                 date_created, date_updated, created_by, updated_by)
            SELECT p.person_id,
                   btrim(t.elem),
                   NULL,
                   COALESCE(p.date_created, now()),
                   COALESCE(p.date_created, now()),
                   p.created_by,
                   p.updated_by
            FROM person p
            CROSS JOIN LATERAL unnest(p.institution) WITH ORDINALITY AS t(elem, ord)
            WHERE p.institution IS NOT NULL
              AND btrim(COALESCE(t.elem, '')) <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM person_institution pi
                  WHERE pi.person_id = p.person_id
                    AND btrim(pi.institution) = btrim(t.elem)
              )
            ORDER BY p.person_id, t.ord
            """
        )

    # ------------------------------------------------------------------
    # 4. Drop the old column, on person AND on person_version.
    #    The version-table drops are easy to forget: the test database is built
    #    by create_all() from the models, where person_version is generated by
    #    continuum and simply won't have these columns, so omitting them here
    #    passes CI and fails on a real migrated database.
    # ------------------------------------------------------------------
    if _col_exists(conn, "person", "institution"):
        op.drop_column("person", "institution")
    if _col_exists(conn, "person_version", "institution_mod"):
        op.drop_column("person_version", "institution_mod")
    if _col_exists(conn, "person_version", "institution"):
        op.drop_column("person_version", "institution")


def downgrade():
    conn = op.get_bind()

    # 1. Re-add the array column on person and person_version.
    if not _col_exists(conn, "person", "institution"):
        op.add_column(
            "person", sa.Column("institution", sa.ARRAY(sa.String()), nullable=True)
        )
    if not _col_exists(conn, "person_version", "institution"):
        op.add_column(
            "person_version",
            sa.Column(
                "institution", sa.ARRAY(sa.String()), autoincrement=False, nullable=True
            ),
        )
    if not _col_exists(conn, "person_version", "institution_mod"):
        op.add_column(
            "person_version",
            sa.Column(
                "institution_mod",
                sa.Boolean(),
                server_default=sa.text("false"),
                nullable=False,
            ),
        )

    # 2. Fold the ACTIVE child rows back into the array. Rows already marked old
    #    are dropped: the array has nowhere to record that they are historical.
    if _table_exists(conn, "person_institution"):
        op.execute(
            """
            UPDATE person p
            SET institution = sub.arr
            FROM (
                SELECT person_id,
                       array_agg(institution ORDER BY person_institution_id) AS arr
                FROM person_institution
                WHERE date_made_old_institution IS NULL
                GROUP BY person_id
            ) sub
            WHERE p.person_id = sub.person_id
            """
        )

    # 3. Drop the child tables.
    if _table_exists(conn, "person_institution_version"):
        op.drop_table("person_institution_version")
    if _table_exists(conn, "person_institution"):
        op.drop_table("person_institution")
