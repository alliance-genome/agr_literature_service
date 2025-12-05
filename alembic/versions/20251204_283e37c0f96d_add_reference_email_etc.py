"""add_reference_email_etc

Revision ID: 283e37c0f96d
Revises: 2011f74f5d11
Create Date: 2025-12-04 22:47:18.294143

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "283e37c0f96d"
down_revision = "2011f74f5d11"
branch_labels = None
depends_on = None


def upgrade():
    # --- reference_email_version -----------------------------------------------
    op.create_table(
        "reference_email_version",
        sa.Column("reference_email_id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("reference_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("email_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("date_created", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("date_updated", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("created_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("updated_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("transaction_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.SmallInteger(), nullable=False),
        sa.Column("reference_id_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("email_id_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("date_created_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("date_updated_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_by_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("updated_by_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.PrimaryKeyConstraint("reference_email_id", "transaction_id"),
    )
    op.create_index(
        op.f("ix_reference_email_version_date_created"),
        "reference_email_version",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_reference_email_version_date_updated"),
        "reference_email_version",
        ["date_updated"],
        unique=False,
    )
    op.create_index(
        op.f("ix_reference_email_version_email_id"),
        "reference_email_version",
        ["email_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_reference_email_version_end_transaction_id"),
        "reference_email_version",
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_reference_email_version_operation_type"),
        "reference_email_version",
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_reference_email_version_reference_id"),
        "reference_email_version",
        ["reference_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_reference_email_version_transaction_id"),
        "reference_email_version",
        ["transaction_id"],
        unique=False,
    )

    # --- reference_email --------------------------------------------------------
    op.create_table(
        "reference_email",
        sa.Column("reference_email_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("reference_id", sa.Integer(), nullable=False),
        sa.Column("email_id", sa.Integer(), nullable=False),
        sa.Column("date_created", sa.DateTime(), nullable=False),
        sa.Column("date_updated", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.Column("updated_by", sa.String(), nullable=True),
        # note: no FK to users.id here (users.id is not unique at this revision)
        sa.ForeignKeyConstraint(["email_id"], ["email.email_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["reference_id"], ["reference.reference_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("reference_email_id"),
        sa.UniqueConstraint("reference_id", "email_id", name="uq_reference_email_reference_email"),
    )
    op.create_index(
        op.f("ix_reference_email_date_created"),
        "reference_email",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_reference_email_date_updated"),
        "reference_email",
        ["date_updated"],
        unique=False,
    )
    op.create_index(
        op.f("ix_reference_email_email_id"),
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
    op.create_index(
        op.f("ix_reference_email_reference_id"),
        "reference_email",
        ["reference_id"],
        unique=False,
    )

    # --- email / email_version --------------------------------------------------
    # new nullable primary column
    op.add_column("email", sa.Column("primary", sa.Boolean(), nullable=True))

    # allow NULL person_id
    op.alter_column("email", "person_id", existing_type=sa.INTEGER(), nullable=True)

    # backfill existing rows so the CHECK constraint passes:
    # for existing rows with person_id NOT NULL, set primary to FALSE if still NULL
    op.execute(
        'UPDATE email SET "primary" = FALSE '
        'WHERE person_id IS NOT NULL AND "primary" IS NULL'
    )

    # constraint: person_id and primary both NULL or both non-NULL
    op.create_check_constraint(
        "ck_email_person_primary_nulls_together",
        "email",
        '((person_id IS NULL AND "primary" IS NULL) OR '
        '(person_id IS NOT NULL AND "primary" IS NOT NULL))',
    )

    # indexes for queries / uniqueness
    op.create_index(
        "ix_email_person_primary",
        "email",
        ["person_id", "primary"],
        unique=False,
    )
    op.create_index(
        "ux_email_person_primary_true",
        "email",
        ["person_id"],
        unique=True,
        postgresql_where=sa.text('"primary" = TRUE'),
    )

    # version table: add primary + primary_mod
    op.add_column("email_version", sa.Column("primary", sa.Boolean(), nullable=True))
    op.add_column(
        "email_version",
        sa.Column(
            "primary_mod",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )


def downgrade():
    # --- email / email_version rollback ----------------------------------------
    op.drop_index("ux_email_person_primary_true", table_name="email")
    op.drop_index("ix_email_person_primary", table_name="email")

    op.drop_constraint(
        "ck_email_person_primary_nulls_together",
        "email",
        type_="check",
    )

    op.alter_column("email", "person_id", existing_type=sa.INTEGER(), nullable=False)

    op.drop_column("email", "primary")

    op.drop_column("email_version", "primary_mod")
    op.drop_column("email_version", "primary")

    # --- reference_email rollback ----------------------------------------------
    op.drop_index(op.f("ix_reference_email_reference_id"), table_name="reference_email")
    op.drop_index("ix_reference_email_reference_email", table_name="reference_email")
    op.drop_index(op.f("ix_reference_email_email_id"), table_name="reference_email")
    op.drop_index(op.f("ix_reference_email_date_updated"), table_name="reference_email")
    op.drop_index(op.f("ix_reference_email_date_created"), table_name="reference_email")
    op.drop_table("reference_email")

    # --- reference_email_version rollback --------------------------------------
    op.drop_index(
        op.f("ix_reference_email_version_transaction_id"),
        table_name="reference_email_version",
    )
    op.drop_index(
        op.f("ix_reference_email_version_reference_id"),
        table_name="reference_email_version",
    )
    op.drop_index(
        op.f("ix_reference_email_version_operation_type"),
        table_name="reference_email_version",
    )
    op.drop_index(
        op.f("ix_reference_email_version_end_transaction_id"),
        table_name="reference_email_version",
    )
    op.drop_index(
        op.f("ix_reference_email_version_email_id"),
        table_name="reference_email_version",
    )
    op.drop_index(
        op.f("ix_reference_email_version_date_updated"),
        table_name="reference_email_version",
    )
    op.drop_index(
        op.f("ix_reference_email_version_date_created"),
        table_name="reference_email_version",
    )
    op.drop_table("reference_email_version")
