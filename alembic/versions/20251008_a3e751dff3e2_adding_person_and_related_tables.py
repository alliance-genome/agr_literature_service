"""adding_person_and_related_tables

Revision ID: a3e751dff3e2
Revises: b7bf14b5c68d
Create Date: 2025-10-08 03:26:45.060721
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "a3e751dff3e2"
down_revision = "b7bf14b5c68d"
branch_labels = None
depends_on = None


# ---------- small helpers (Postgres) ----------
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


def _create_index_if_missing(table: str, cols, name: str, unique: bool = False):
    conn = op.get_bind()
    if not _index_exists(conn, name):
        op.create_index(name, table, cols, unique=unique)


def upgrade():  # noqa: C901
    # ---------------------------------------------------------
    # Clean up any leftover *users_version* objects (idempotent)
    # ---------------------------------------------------------
    op.execute("DROP INDEX IF EXISTS ix_users_version_person_id")
    op.execute("DROP INDEX IF EXISTS ix_users_version_email")
    op.execute("DROP INDEX IF EXISTS ix_users_version_automation_username")
    op.execute("DROP TABLE IF EXISTS users_version CASCADE")

    # ----------------------------
    # Version tables (email/person/xref)
    # ----------------------------
    op.create_table(
        "email_version",
        sa.Column("email_id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("person_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("email_address", sa.String(), autoincrement=False, nullable=True),
        sa.Column(
            "date_invalidated",
            sa.DateTime(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("date_created", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("date_updated", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("created_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("updated_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("transaction_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.SmallInteger(), nullable=False),
        sa.Column(
            "person_id_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "email_address_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "date_invalidated_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "date_created_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "date_updated_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "created_by_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "updated_by_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("email_id", "transaction_id"),
    )
    op.create_index(
        op.f("ix_email_version_date_created"),
        "email_version",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_email_version_date_updated"),
        "email_version",
        ["date_updated"],
        unique=False,
    )
    op.create_index(
        op.f("ix_email_version_end_transaction_id"),
        "email_version",
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_email_version_operation_type"),
        "email_version",
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_email_version_person_id"),
        "email_version",
        ["person_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_email_version_transaction_id"),
        "email_version",
        ["transaction_id"],
        unique=False,
    )

    op.create_table(
        "person",
        sa.Column("person_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("display_name", sa.String(), nullable=False),
        sa.Column("curie", sa.String(), nullable=True),
        sa.Column("okta_id", sa.String(), nullable=True),
        sa.Column("mod_roles", sa.ARRAY(sa.String()), nullable=True),
        sa.Column("date_created", sa.DateTime(), nullable=False),
        sa.Column("date_updated", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.Column("updated_by", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("person_id"),
        sa.UniqueConstraint("okta_id", name="uq_person_okta_id"),
    )
    op.create_index(op.f("ix_person_curie"), "person", ["curie"], unique=False)
    op.create_index(
        op.f("ix_person_date_created"), "person", ["date_created"], unique=False
    )
    op.create_index(
        op.f("ix_person_date_updated"), "person", ["date_updated"], unique=False
    )
    op.create_index(
        "ix_person_display_name_trigram", "person", ["display_name"], unique=False
    )
    op.create_index(op.f("ix_person_okta_id"), "person", ["okta_id"], unique=False)

    op.create_table(
        "person_cross_reference_version",
        sa.Column(
            "person_cross_reference_id",
            sa.Integer(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("curie", sa.String(), autoincrement=False, nullable=True),
        sa.Column("curie_prefix", sa.String(), autoincrement=False, nullable=True),
        sa.Column("person_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("pages", sa.ARRAY(sa.String()), autoincrement=False, nullable=True),
        sa.Column(
            "is_obsolete",
            sa.Boolean(),
            server_default=sa.text("false"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("date_created", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("date_updated", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("created_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("updated_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("transaction_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.SmallInteger(), nullable=False),
        sa.Column("curie_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column(
            "curie_prefix_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "person_id_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column("pages_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column(
            "is_obsolete_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "date_created_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "date_updated_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "created_by_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "updated_by_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("person_cross_reference_id", "transaction_id"),
    )
    op.create_index(
        op.f("ix_person_cross_reference_version_curie"),
        "person_cross_reference_version",
        ["curie"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_cross_reference_version_curie_prefix"),
        "person_cross_reference_version",
        ["curie_prefix"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_cross_reference_version_date_created"),
        "person_cross_reference_version",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_cross_reference_version_date_updated"),
        "person_cross_reference_version",
        ["date_updated"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_cross_reference_version_end_transaction_id"),
        "person_cross_reference_version",
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_cross_reference_version_operation_type"),
        "person_cross_reference_version",
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_cross_reference_version_person_id"),
        "person_cross_reference_version",
        ["person_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_cross_reference_version_transaction_id"),
        "person_cross_reference_version",
        ["transaction_id"],
        unique=False,
    )

    op.create_table(
        "person_version",
        sa.Column("person_id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("display_name", sa.String(), autoincrement=False, nullable=True),
        sa.Column("curie", sa.String(), autoincrement=False, nullable=True),
        sa.Column("okta_id", sa.String(), autoincrement=False, nullable=True),
        sa.Column("mod_roles", sa.ARRAY(sa.String()), autoincrement=False, nullable=True),
        sa.Column("date_created", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("date_updated", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("created_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("updated_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("transaction_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.SmallInteger(), nullable=False),
        sa.Column(
            "display_name_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("curie_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("okta_id_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column(
            "mod_roles_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "date_created_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "date_updated_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "created_by_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "updated_by_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("person_id", "transaction_id"),
    )
    op.create_index(op.f("ix_person_version_curie"), "person_version", ["curie"], unique=False)
    op.create_index(
        op.f("ix_person_version_date_created"),
        "person_version",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_version_date_updated"),
        "person_version",
        ["date_updated"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_version_end_transaction_id"),
        "person_version",
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_version_okta_id"), "person_version", ["okta_id"], unique=False
    )
    op.create_index(
        op.f("ix_person_version_operation_type"),
        "person_version",
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_version_transaction_id"),
        "person_version",
        ["transaction_id"],
        unique=False,
    )

    op.create_table(
        "email",
        sa.Column("email_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("person_id", sa.Integer(), nullable=False),
        sa.Column("email_address", sa.String(), nullable=False),
        sa.Column("date_invalidated", sa.DateTime(), nullable=True),
        sa.Column("date_created", sa.DateTime(), nullable=False),
        sa.Column("date_updated", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.Column("updated_by", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["person_id"], ["person.person_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("email_id"),
        sa.UniqueConstraint("person_id", "email_address", name="uq_email_person_address"),
    )
    op.create_index("ix_email_address", "email", ["email_address"], unique=False)
    op.create_index(op.f("ix_email_date_created"), "email", ["date_created"], unique=False)
    op.create_index(op.f("ix_email_date_updated"), "email", ["date_updated"], unique=False)
    op.create_index(op.f("ix_email_person_id"), "email", ["person_id"], unique=False)

    op.create_table(
        "person_cross_reference",
        sa.Column(
            "person_cross_reference_id", sa.Integer(), autoincrement=True, nullable=False
        ),
        sa.Column("curie", sa.String(), nullable=False),
        sa.Column("curie_prefix", sa.String(), nullable=False),
        sa.Column("person_id", sa.Integer(), nullable=True),
        sa.Column("pages", sa.ARRAY(sa.String()), nullable=True),
        sa.Column("is_obsolete", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("date_created", sa.DateTime(), nullable=False),
        sa.Column("date_updated", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.Column("updated_by", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["person_id"], ["person.person_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("person_cross_reference_id"),
        sa.UniqueConstraint("curie", name="uq_person_xref_curie"),
        sa.UniqueConstraint("person_id", "curie_prefix", name="uq_person_xref_person_prefix"),
    )
    op.create_index(
        op.f("ix_person_cross_reference_curie"), "person_cross_reference", ["curie"], unique=False
    )
    op.create_index(
        op.f("ix_person_cross_reference_curie_prefix"),
        "person_cross_reference",
        ["curie_prefix"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_cross_reference_date_created"),
        "person_cross_reference",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_cross_reference_date_updated"),
        "person_cross_reference",
        ["date_updated"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_cross_reference_person_id"),
        "person_cross_reference",
        ["person_id"],
        unique=False,
    )
    op.create_index("ix_person_xref_person_id", "person_cross_reference", ["person_id"], unique=False)
    op.create_index(
        "ix_person_xref_prefix_curie",
        "person_cross_reference",
        ["curie_prefix", "curie"],
        unique=False,
    )

    # ----------------------------
    # USERS table adjustments (data-safe & idempotent)
    # ----------------------------
    conn = op.get_bind()

    # 1) Add user_id as identity (if missing)
    if not _col_exists(conn, "users", "user_id"):
        op.execute(
            'ALTER TABLE "users" ADD COLUMN "user_id" INTEGER '
            "GENERATED BY DEFAULT AS IDENTITY"
        )

    # Backfill identity for existing rows (idempotent)
    op.execute(
        """
        UPDATE "users"
        SET user_id = COALESCE(
            user_id,
            nextval(pg_get_serial_sequence('"users"', 'user_id'))
        )
        """
    )
    op.alter_column("users", "user_id", nullable=False)

    # 2) Ensure UNIQUE(user_id) for FKs (keep users.id as PK if present)
    if not _constraint_exists(conn, "users", "uq_users_user_id"):
        op.create_unique_constraint("uq_users_user_id", "users", ["user_id"])

    # 3) Add columns if missing
    if not _col_exists(conn, "users", "automation_username"):
        op.add_column("users", sa.Column("automation_username", sa.String(), nullable=True))
    if not _col_exists(conn, "users", "person_id"):
        op.add_column("users", sa.Column("person_id", sa.Integer(), nullable=True))

    # 4) Indexes on users
    _create_index_if_missing(
        "users", ["automation_username"], op.f("ix_users_automation_username")
    )
    _create_index_if_missing("users", ["email"], op.f("ix_users_email"))
    _create_index_if_missing("users", ["person_id"], op.f("ix_users_person_id"))

    # 5) FK users.person_id -> person (SET NULL)
    if _constraint_exists(conn, "users", "users_person_id_fkey"):
        op.drop_constraint("users_person_id_fkey", "users", type_="foreignkey")
    if not _constraint_exists(conn, "users", "users_person_id_fkey"):
        op.create_foreign_key(
            "users_person_id_fkey",
            "users",
            "person",
            ["person_id"],
            ["person_id"],
            ondelete="SET NULL",
        )

    # 6) CHECK constraint exactly-one-of
    violations = conn.execute(
        sa.text(
            """
            SELECT COUNT(*)::bigint
            FROM users
            WHERE (person_id IS NULL) = (automation_username IS NULL)
            """
        )
    ).scalar()
    if violations and int(violations) > 0:
        if not _constraint_exists(
            conn, "users", "ck_users_exactly_one_of_person_or_automation"
        ):
            op.execute(
                """
                ALTER TABLE users
                ADD CONSTRAINT ck_users_exactly_one_of_person_or_automation
                CHECK ( (person_id IS NULL) <> (automation_username IS NULL) )
                NOT VALID
                """
            )
    else:
        if not _constraint_exists(
            conn, "users", "ck_users_exactly_one_of_person_or_automation"
        ):
            op.create_check_constraint(
                "ck_users_exactly_one_of_person_or_automation",
                "users",
                "(person_id IS NULL) <> (automation_username IS NULL)",
            )

    # ----------------------------
    # TRANSACTION.user_id migration (VARCHAR -> INTEGER FK)
    # ----------------------------
    if not _col_exists(conn, "transaction", "user_id_new"):
        op.add_column("transaction", sa.Column("user_id_new", sa.Integer(), nullable=True))

    # Map old varchar user_id -> users.id -> users.user_id
    op.execute(
        """
        UPDATE transaction t
        SET user_id_new = u.user_id
        FROM users u
        WHERE t.user_id IS NOT NULL
          AND u.id = t.user_id
        """
    )

    if _constraint_exists(conn, "transaction", "transaction_user_id_fkey"):
        op.drop_constraint("transaction_user_id_fkey", "transaction", type_="foreignkey")

    if _col_exists(conn, "transaction", "user_id"):
        op.drop_column("transaction", "user_id")
    op.alter_column("transaction", "user_id_new", new_column_name="user_id")

    if not _constraint_exists(conn, "transaction", "transaction_user_id_fkey"):
        op.create_foreign_key(
            "transaction_user_id_fkey", "transaction", "users", ["user_id"], ["user_id"]
        )


def downgrade():
    conn = op.get_bind()

    # users: drop check, FK to person, indexes, and UNIQUE(user_id)
    if _constraint_exists(conn, "users", "ck_users_exactly_one_of_person_or_automation"):
        op.drop_constraint(
            "ck_users_exactly_one_of_person_or_automation", "users", type_="check"
        )
    if _constraint_exists(conn, "users", "users_person_id_fkey"):
        op.drop_constraint("users_person_id_fkey", "users", type_="foreignkey")

    op.drop_index(op.f("ix_users_person_id"), table_name="users")
    op.drop_index(op.f("ix_users_email"), table_name="users")
    op.drop_index(op.f("ix_users_automation_username"), table_name="users")

    if _constraint_exists(conn, "users", "uq_users_user_id"):
        op.drop_constraint("uq_users_user_id", "users", type_="unique")

    op.drop_column("users", "person_id")
    op.drop_column("users", "automation_username")
    op.drop_column("users", "user_id")

    # person_cross_reference
    op.drop_index("ix_person_xref_prefix_curie", table_name="person_cross_reference")
    op.drop_index("ix_person_xref_person_id", table_name="person_cross_reference")
    op.drop_index(
        op.f("ix_person_cross_reference_person_id"), table_name="person_cross_reference"
    )
    op.drop_index(
        op.f("ix_person_cross_reference_date_updated"),
        table_name="person_cross_reference",
    )
    op.drop_index(
        op.f("ix_person_cross_reference_date_created"),
        table_name="person_cross_reference",
    )
    op.drop_index(
        op.f("ix_person_cross_reference_curie_prefix"),
        table_name="person_cross_reference",
    )
    op.drop_index(
        op.f("ix_person_cross_reference_curie"), table_name="person_cross_reference"
    )
    op.drop_table("person_cross_reference")

    # email
    op.drop_index(op.f("ix_email_person_id"), table_name="email")
    op.drop_index(op.f("ix_email_date_updated"), table_name="email")
    op.drop_index(op.f("ix_email_date_created"), table_name="email")
    op.drop_index("ix_email_address", table_name="email")
    op.drop_table("email")

    # person_version
    op.drop_index(
        op.f("ix_person_version_transaction_id"), table_name="person_version"
    )
    op.drop_index(
        op.f("ix_person_version_operation_type"), table_name="person_version"
    )
    op.drop_index(op.f("ix_person_version_okta_id"), table_name="person_version")
    op.drop_index(
        op.f("ix_person_version_end_transaction_id"), table_name="person_version"
    )
    op.drop_index(
        op.f("ix_person_version_date_updated"), table_name="person_version"
    )
    op.drop_index(
        op.f("ix_person_version_date_created"), table_name="person_version"
    )
    op.drop_index(op.f("ix_person_version_curie"), table_name="person_version")
    op.drop_table("person_version")

    # person_cross_reference_version
    op.drop_index(
        op.f("ix_person_cross_reference_version_transaction_id"),
        table_name="person_cross_reference_version",
    )
    op.drop_index(
        op.f("ix_person_cross_reference_version_person_id"),
        table_name="person_cross_reference_version",
    )
    op.drop_index(
        op.f("ix_person_cross_reference_version_operation_type"),
        table_name="person_cross_reference_version",
    )
    op.drop_index(
        op.f("ix_person_cross_reference_version_end_transaction_id"),
        table_name="person_cross_reference_version",
    )
    op.drop_index(
        op.f("ix_person_cross_reference_version_date_updated"),
        table_name="person_cross_reference_version",
    )
    op.drop_index(
        op.f("ix_person_cross_reference_version_date_created"),
        table_name="person_cross_reference_version",
    )
    op.drop_index(
        op.f("ix_person_cross_reference_version_curie_prefix"),
        table_name="person_cross_reference_version",
    )
    op.drop_index(
        op.f("ix_person_cross_reference_version_curie"),
        table_name="person_cross_reference_version",
    )
    op.drop_table("person_cross_reference_version")

    # person
    op.drop_index(op.f("ix_person_okta_id"), table_name="person")
    op.drop_index("ix_person_display_name_trigram", table_name="person")
    op.drop_index(op.f("ix_person_date_updated"), table_name="person")
    op.drop_index(op.f("ix_person_date_created"), table_name="person")
    op.drop_index(op.f("ix_person_curie"), table_name="person")
    op.drop_table("person")

    # email_version
    op.drop_index(op.f("ix_email_version_transaction_id"), table_name="email_version")
    op.drop_index(op.f("ix_email_version_person_id"), table_name="email_version")
    op.drop_index(op.f("ix_email_version_operation_type"), table_name="email_version")
    op.drop_index(op.f("ix_email_version_end_transaction_id"), table_name="email_version")
    op.drop_index(op.f("ix_email_version_date_updated"), table_name="email_version")
    op.drop_index(op.f("ix_email_version_date_created"), table_name="email_version")
    op.drop_table("email_version")
