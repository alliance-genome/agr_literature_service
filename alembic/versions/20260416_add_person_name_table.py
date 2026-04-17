"""add_person_name_table

Revision ID: placeholder
Revises: 78e9d4f87ed5
Create Date: 2026-04-16
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "placeholder_person_name"
down_revision = "78e9d4f87ed5"
branch_labels = None
depends_on = None


def upgrade():
    # -------------------------------------------------------
    # person_name table
    # -------------------------------------------------------
    op.create_table(
        "person_name",
        sa.Column("person_name_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("person_id", sa.Integer(), nullable=False),
        sa.Column("first_name", sa.String(), nullable=True),
        sa.Column("middle_name", sa.String(), nullable=True),
        sa.Column("last_name", sa.String(), nullable=False),
        sa.Column("primary", sa.Boolean(), nullable=True),
        sa.Column("date_created", sa.DateTime(), nullable=False),
        sa.Column("date_updated", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.Column("updated_by", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["person_id"], ["person.person_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("person_name_id"),
    )
    op.create_index(
        op.f("ix_person_name_person_id"), "person_name", ["person_id"], unique=False
    )
    op.create_index(
        op.f("ix_person_name_date_created"), "person_name", ["date_created"], unique=False
    )
    op.create_index(
        op.f("ix_person_name_date_updated"), "person_name", ["date_updated"], unique=False
    )
    # At most one primary name per person
    op.create_index(
        "ux_person_name_person_primary_true",
        "person_name",
        ["person_id"],
        unique=True,
        postgresql_where=sa.text('"primary" = TRUE'),
    )
    # Composite index for (person_id, primary) lookups
    op.create_index(
        "ix_person_name_person_primary", "person_name", ["person_id", "primary"], unique=False
    )

    # -------------------------------------------------------
    # person_name_version table (for versioning/audit trail)
    # -------------------------------------------------------
    op.create_table(
        "person_name_version",
        sa.Column("person_name_id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("person_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("first_name", sa.String(), autoincrement=False, nullable=True),
        sa.Column("middle_name", sa.String(), autoincrement=False, nullable=True),
        sa.Column("last_name", sa.String(), autoincrement=False, nullable=True),
        sa.Column("primary", sa.Boolean(), autoincrement=False, nullable=True),
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
            "first_name_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "middle_name_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "last_name_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "primary_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "date_created_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "date_updated_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "created_by_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "updated_by_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.PrimaryKeyConstraint("person_name_id", "transaction_id"),
    )
    op.create_index(
        op.f("ix_person_name_version_person_id"),
        "person_name_version",
        ["person_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_name_version_date_created"),
        "person_name_version",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_name_version_date_updated"),
        "person_name_version",
        ["date_updated"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_name_version_end_transaction_id"),
        "person_name_version",
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_name_version_operation_type"),
        "person_name_version",
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_person_name_version_transaction_id"),
        "person_name_version",
        ["transaction_id"],
        unique=False,
    )

    # -------------------------------------------------------
    # New columns on person table
    # -------------------------------------------------------
    op.add_column("person", sa.Column("orcid", sa.String(), nullable=True))
    op.add_column("person", sa.Column("webpage", sa.ARRAY(sa.String()), nullable=True))
    op.add_column(
        "person",
        sa.Column(
            "active_status",
            sa.String(),
            nullable=False,
            server_default="active",
        ),
    )
    op.create_check_constraint(
        "ck_person_active_status",
        "person",
        "active_status IN ('active', 'retired', 'deceased')",
    )
    op.add_column("person", sa.Column("city", sa.String(), nullable=True))
    op.add_column("person", sa.Column("state", sa.String(), nullable=True))
    op.add_column("person", sa.Column("postal_code", sa.String(), nullable=True))
    op.add_column("person", sa.Column("country", sa.String(), nullable=True))
    op.add_column("person", sa.Column("street_address", sa.String(), nullable=True))
    op.add_column("person", sa.Column("address_last_updated", sa.DateTime(), nullable=True))
    op.create_index(op.f("ix_person_orcid"), "person", ["orcid"], unique=False)

    # -------------------------------------------------------
    # New columns on person_version table (for versioning)
    # -------------------------------------------------------
    op.add_column("person_version", sa.Column(
        "orcid", sa.String(), autoincrement=False, nullable=True))
    op.add_column("person_version", sa.Column(
        "webpage", sa.ARRAY(sa.String()), autoincrement=False, nullable=True))
    op.add_column("person_version", sa.Column(
        "active_status", sa.String(), autoincrement=False, nullable=True))
    op.add_column("person_version", sa.Column(
        "city", sa.String(), autoincrement=False, nullable=True))
    op.add_column("person_version", sa.Column(
        "state", sa.String(), autoincrement=False, nullable=True))
    op.add_column("person_version", sa.Column(
        "postal_code", sa.String(), autoincrement=False, nullable=True))
    op.add_column("person_version", sa.Column(
        "country", sa.String(), autoincrement=False, nullable=True))
    op.add_column("person_version", sa.Column(
        "street_address", sa.String(), autoincrement=False, nullable=True))
    op.add_column("person_version", sa.Column(
        "address_last_updated", sa.DateTime(), autoincrement=False, nullable=True))
    # _mod columns for versioning
    op.add_column("person_version", sa.Column(
        "orcid_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False))
    op.add_column("person_version", sa.Column(
        "webpage_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False))
    op.add_column("person_version", sa.Column(
        "active_status_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False))
    op.add_column("person_version", sa.Column(
        "city_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False))
    op.add_column("person_version", sa.Column(
        "state_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False))
    op.add_column("person_version", sa.Column(
        "postal_code_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False))
    op.add_column("person_version", sa.Column(
        "country_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False))
    op.add_column("person_version", sa.Column(
        "street_address_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False))
    op.add_column("person_version", sa.Column(
        "address_last_updated_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False))


def downgrade():
    # -------------------------------------------------------
    # Remove person columns first
    # -------------------------------------------------------
    # person_version _mod columns
    for col in ["address_last_updated_mod", "street_address_mod", "country_mod",
                "postal_code_mod", "state_mod", "city_mod", "active_status_mod",
                "webpage_mod", "orcid_mod"]:
        op.drop_column("person_version", col)
    # person_version data columns
    for col in ["address_last_updated", "street_address", "country", "postal_code",
                "state", "city", "active_status", "webpage", "orcid"]:
        op.drop_column("person_version", col)
    # person columns
    op.drop_index(op.f("ix_person_orcid"), table_name="person")
    op.drop_constraint("ck_person_active_status", "person", type_="check")
    for col in ["address_last_updated", "street_address", "country", "postal_code",
                "state", "city", "active_status", "webpage", "orcid"]:
        op.drop_column("person", col)

    # person_name_version
    op.drop_index(
        op.f("ix_person_name_version_transaction_id"), table_name="person_name_version"
    )
    op.drop_index(
        op.f("ix_person_name_version_operation_type"), table_name="person_name_version"
    )
    op.drop_index(
        op.f("ix_person_name_version_end_transaction_id"), table_name="person_name_version"
    )
    op.drop_index(
        op.f("ix_person_name_version_date_updated"), table_name="person_name_version"
    )
    op.drop_index(
        op.f("ix_person_name_version_date_created"), table_name="person_name_version"
    )
    op.drop_index(
        op.f("ix_person_name_version_person_id"), table_name="person_name_version"
    )
    op.drop_table("person_name_version")

    # person_name
    op.drop_index("ix_person_name_person_primary", table_name="person_name")
    op.drop_index("ux_person_name_person_primary_true", table_name="person_name")
    op.drop_index(op.f("ix_person_name_date_updated"), table_name="person_name")
    op.drop_index(op.f("ix_person_name_date_created"), table_name="person_name")
    op.drop_index(op.f("ix_person_name_person_id"), table_name="person_name")
    op.drop_table("person_name")
