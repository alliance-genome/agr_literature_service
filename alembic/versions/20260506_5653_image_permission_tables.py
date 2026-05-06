"""image_permission_tables

Revision ID: 5653a1b2c3d4
Revises: d9e0f1a2b3c4
Create Date: 2026-05-06
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5653a1b2c3d4"
down_revision = "d9e0f1a2b3c4"
branch_labels = None
depends_on = None


def _create_version_indexes(table_name: str, indexed_columns):
    op.create_index(
        op.f(f"ix_{table_name}_end_transaction_id"),
        table_name,
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f(f"ix_{table_name}_operation_type"),
        table_name,
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f(f"ix_{table_name}_transaction_id"),
        table_name,
        ["transaction_id"],
        unique=False,
    )
    for column_name in indexed_columns:
        op.create_index(
            op.f(f"ix_{table_name}_{column_name}"),
            table_name,
            [column_name],
            unique=False,
        )


def upgrade():
    op.create_table(
        "image_permission",
        sa.Column("image_permission_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("permission_text", sa.TEXT(), nullable=False),
        sa.Column("permission_url", sa.String(), nullable=True),
        sa.Column("can_display_images", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("date_created", sa.DateTime(), nullable=False),
        sa.Column("date_updated", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.Column("updated_by", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("image_permission_id"),
        sa.UniqueConstraint("name", name="uq_image_permission_name"),
    )
    op.create_index(op.f("ix_image_permission_name"), "image_permission", ["name"], unique=False)
    op.create_index(op.f("ix_image_permission_date_created"), "image_permission", ["date_created"], unique=False)
    op.create_index(op.f("ix_image_permission_date_updated"), "image_permission", ["date_updated"], unique=False)

    op.create_table(
        "resource_image_permission",
        sa.Column("resource_image_permission_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("resource_id", sa.Integer(), nullable=False),
        sa.Column("image_permission_id", sa.Integer(), nullable=False),
        sa.Column("start_year", sa.Integer(), nullable=True),
        sa.Column("end_year", sa.Integer(), nullable=True),
        sa.Column("notes", sa.TEXT(), nullable=True),
        sa.Column("date_created", sa.DateTime(), nullable=False),
        sa.Column("date_updated", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.Column("updated_by", sa.String(), nullable=True),
        sa.CheckConstraint(
            "end_year IS NULL OR start_year IS NULL OR end_year >= start_year",
            name="ck_resource_image_permission_year_range",
        ),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(
            ["image_permission_id"],
            ["image_permission.image_permission_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["resource_id"], ["resource.resource_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("resource_image_permission_id"),
        sa.UniqueConstraint(
            "resource_id",
            "image_permission_id",
            "start_year",
            "end_year",
            name="uq_resource_image_permission_range",
        ),
    )
    op.create_index(
        op.f("ix_resource_image_permission_date_created"),
        "resource_image_permission",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_resource_image_permission_date_updated"),
        "resource_image_permission",
        ["date_updated"],
        unique=False,
    )
    op.create_index(
        op.f("ix_resource_image_permission_image_permission_id"),
        "resource_image_permission",
        ["image_permission_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_resource_image_permission_resource_id"),
        "resource_image_permission",
        ["resource_id"],
        unique=False,
    )

    op.create_table(
        "image_permission_version",
        sa.Column("image_permission_id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("name", sa.String(), autoincrement=False, nullable=True),
        sa.Column("permission_text", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("permission_url", sa.String(), autoincrement=False, nullable=True),
        sa.Column("can_display_images", sa.Boolean(), autoincrement=False, nullable=True),
        sa.Column("date_created", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("date_updated", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("created_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("updated_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("transaction_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.SmallInteger(), nullable=False),
        sa.Column("name_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("permission_text_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("permission_url_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("can_display_images_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("date_created_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("date_updated_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_by_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("updated_by_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.PrimaryKeyConstraint("image_permission_id", "transaction_id"),
    )
    _create_version_indexes("image_permission_version", ["date_created", "date_updated"])

    op.create_table(
        "resource_image_permission_version",
        sa.Column("resource_image_permission_id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("resource_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("image_permission_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("start_year", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("end_year", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("notes", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("date_created", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("date_updated", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("created_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("updated_by", sa.String(), autoincrement=False, nullable=True),
        sa.Column("transaction_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.SmallInteger(), nullable=False),
        sa.Column("resource_id_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("image_permission_id_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("start_year_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("end_year_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("notes_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("date_created_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("date_updated_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_by_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("updated_by_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.PrimaryKeyConstraint("resource_image_permission_id", "transaction_id"),
    )
    _create_version_indexes(
        "resource_image_permission_version",
        ["resource_id", "image_permission_id", "date_created", "date_updated"],
    )


def downgrade():
    op.drop_index(op.f("ix_resource_image_permission_version_date_updated"),
                  table_name="resource_image_permission_version")
    op.drop_index(op.f("ix_resource_image_permission_version_date_created"),
                  table_name="resource_image_permission_version")
    op.drop_index(op.f("ix_resource_image_permission_version_image_permission_id"),
                  table_name="resource_image_permission_version")
    op.drop_index(op.f("ix_resource_image_permission_version_resource_id"),
                  table_name="resource_image_permission_version")
    op.drop_index(op.f("ix_resource_image_permission_version_transaction_id"),
                  table_name="resource_image_permission_version")
    op.drop_index(op.f("ix_resource_image_permission_version_operation_type"),
                  table_name="resource_image_permission_version")
    op.drop_index(op.f("ix_resource_image_permission_version_end_transaction_id"),
                  table_name="resource_image_permission_version")
    op.drop_table("resource_image_permission_version")

    op.drop_index(op.f("ix_image_permission_version_date_updated"), table_name="image_permission_version")
    op.drop_index(op.f("ix_image_permission_version_date_created"), table_name="image_permission_version")
    op.drop_index(op.f("ix_image_permission_version_transaction_id"), table_name="image_permission_version")
    op.drop_index(op.f("ix_image_permission_version_operation_type"), table_name="image_permission_version")
    op.drop_index(op.f("ix_image_permission_version_end_transaction_id"), table_name="image_permission_version")
    op.drop_table("image_permission_version")

    op.drop_index(op.f("ix_resource_image_permission_resource_id"), table_name="resource_image_permission")
    op.drop_index(op.f("ix_resource_image_permission_image_permission_id"), table_name="resource_image_permission")
    op.drop_index(op.f("ix_resource_image_permission_date_updated"), table_name="resource_image_permission")
    op.drop_index(op.f("ix_resource_image_permission_date_created"), table_name="resource_image_permission")
    op.drop_table("resource_image_permission")

    op.drop_index(op.f("ix_image_permission_date_updated"), table_name="image_permission")
    op.drop_index(op.f("ix_image_permission_date_created"), table_name="image_permission")
    op.drop_index(op.f("ix_image_permission_name"), table_name="image_permission")
    op.drop_table("image_permission")
