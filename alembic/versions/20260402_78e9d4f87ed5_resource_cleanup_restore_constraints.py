"""resource_cleanup_restore_constraints

Revision ID: 78e9d4f87ed5
Revises: b55c67873df0
Create Date: 2026-04-02

Restores idx_curie and idx_curie_res constraints on cross_reference.
Merges iso_abbreviation into medline_abbreviation and renames to
title_abbreviation. Renames abbreviation_synonyms to
title_abbreviation_synonyms. Drops: abstract, summary, open_access,
print_issn, online_issn from resource (and resource_version).
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect, text

revision = "78e9d4f87ed5"
down_revision = "b55c67873df0"
branch_labels = None
depends_on = None


def _index_exists(table, name):
    bind = op.get_bind()
    insp = sa_inspect(bind)
    for idx in insp.get_indexes(table):
        if idx["name"] == name:
            return True
    return False


def _column_exists(table, column):
    bind = op.get_bind()
    insp = sa_inspect(bind)
    columns = [col["name"] for col in insp.get_columns(table)]
    return column in columns


def upgrade():  # noqa: C901
    conn = op.get_bind()

    # ================================================================
    # 1a. Restore cross_reference constraints
    # ================================================================
    if _index_exists("cross_reference", "idx_curie"):
        op.drop_index("idx_curie", table_name="cross_reference")
    op.create_index(
        "idx_curie", "cross_reference", ["curie"],
        unique=True,
        postgresql_where=text("is_obsolete IS FALSE"))

    if _index_exists("cross_reference", "idx_curie_res"):
        op.drop_index("idx_curie_res", table_name="cross_reference")
    op.create_index(
        "idx_curie_res", "cross_reference",
        ["curie", "resource_id"],
        unique=True,
        postgresql_where=text("resource_id IS NOT NULL"))

    # ================================================================
    # 1b. Merge iso_abbreviation into medline_abbreviation,
    #     rename to title_abbreviation
    # ================================================================
    conn.execute(text(
        "UPDATE resource "
        "SET medline_abbreviation = iso_abbreviation "
        "WHERE medline_abbreviation IS NULL "
        "AND iso_abbreviation IS NOT NULL"
    ))
    conn.execute(text(
        "UPDATE resource_version "
        "SET medline_abbreviation = iso_abbreviation "
        "WHERE medline_abbreviation IS NULL "
        "AND iso_abbreviation IS NOT NULL"
    ))

    # resource table
    op.alter_column("resource", "medline_abbreviation",
                    new_column_name="title_abbreviation")
    if _column_exists("resource", "iso_abbreviation"):
        op.drop_column("resource", "iso_abbreviation")

    # resource_version table
    op.alter_column("resource_version", "medline_abbreviation",
                    new_column_name="title_abbreviation")
    if _column_exists("resource_version", "iso_abbreviation"):
        op.drop_column("resource_version", "iso_abbreviation")
    if _column_exists("resource_version", "iso_abbreviation_mod"):
        op.drop_column("resource_version", "iso_abbreviation_mod")
    if _column_exists("resource_version", "medline_abbreviation_mod"):
        op.drop_column("resource_version", "medline_abbreviation_mod")
    if not _column_exists("resource_version", "title_abbreviation_mod"):
        op.add_column(
            "resource_version",
            sa.Column("title_abbreviation_mod", sa.Boolean(),
                      server_default=sa.text("false"),
                      nullable=False))

    # ================================================================
    # 1c. Rename abbreviation_synonyms -> title_abbreviation_synonyms
    # ================================================================
    if _column_exists("resource", "abbreviation_synonyms"):
        op.alter_column("resource", "abbreviation_synonyms",
                        new_column_name="title_abbreviation_synonyms")
    if _column_exists("resource_version", "abbreviation_synonyms"):
        op.alter_column("resource_version", "abbreviation_synonyms",
                        new_column_name="title_abbreviation_synonyms")
    if _column_exists("resource_version", "abbreviation_synonyms_mod"):
        op.alter_column("resource_version", "abbreviation_synonyms_mod",
                        new_column_name="title_abbreviation_synonyms_mod")

    # ================================================================
    # 1d. Drop columns: abstract, summary, open_access
    # ================================================================
    for col in ["abstract", "summary", "open_access"]:
        if _column_exists("resource", col):
            op.drop_column("resource", col)
    for col in ["abstract", "summary", "open_access",
                "abstract_mod", "summary_mod", "open_access_mod"]:
        if _column_exists("resource_version", col):
            op.drop_column("resource_version", col)

    # ================================================================
    # 1e. Drop columns: print_issn, online_issn
    # ================================================================
    for col in ["print_issn", "online_issn"]:
        if _column_exists("resource", col):
            op.drop_column("resource", col)
    for col in ["print_issn", "online_issn",
                "print_issn_mod", "online_issn_mod"]:
        if _column_exists("resource_version", col):
            op.drop_column("resource_version", col)


def downgrade():  # noqa: C901
    # Re-add dropped columns to resource
    for col in ["print_issn", "online_issn", "abstract", "summary"]:
        if not _column_exists("resource", col):
            op.add_column(
                "resource",
                sa.Column(col, sa.String(), nullable=True))
    if not _column_exists("resource", "open_access"):
        op.add_column(
            "resource",
            sa.Column("open_access", sa.Boolean(),
                      server_default=sa.text("false"),
                      nullable=False))

    # Rename back on resource
    if _column_exists("resource", "title_abbreviation_synonyms"):
        op.alter_column("resource", "title_abbreviation_synonyms",
                        new_column_name="abbreviation_synonyms")
    if _column_exists("resource", "title_abbreviation"):
        op.alter_column("resource", "title_abbreviation",
                        new_column_name="medline_abbreviation")
    if not _column_exists("resource", "iso_abbreviation"):
        op.add_column(
            "resource",
            sa.Column("iso_abbreviation", sa.String(), nullable=True))

    # Re-add dropped columns to resource_version
    for col in ["print_issn", "online_issn", "abstract", "summary"]:
        if not _column_exists("resource_version", col):
            op.add_column(
                "resource_version",
                sa.Column(col, sa.String(),
                          autoincrement=False, nullable=True))
    for col in ["print_issn_mod", "online_issn_mod",
                "abstract_mod", "summary_mod"]:
        if not _column_exists("resource_version", col):
            op.add_column(
                "resource_version",
                sa.Column(col, sa.Boolean(),
                          server_default=sa.text("false"),
                          nullable=False))
    if not _column_exists("resource_version", "open_access"):
        op.add_column(
            "resource_version",
            sa.Column("open_access", sa.Boolean(),
                      autoincrement=False, nullable=True))
    if not _column_exists("resource_version", "open_access_mod"):
        op.add_column(
            "resource_version",
            sa.Column("open_access_mod", sa.Boolean(),
                      server_default=sa.text("false"),
                      nullable=False))

    # Rename back on resource_version
    if _column_exists("resource_version", "title_abbreviation_synonyms"):
        op.alter_column("resource_version",
                        "title_abbreviation_synonyms",
                        new_column_name="abbreviation_synonyms")
    if _column_exists("resource_version",
                      "title_abbreviation_synonyms_mod"):
        op.alter_column("resource_version",
                        "title_abbreviation_synonyms_mod",
                        new_column_name="abbreviation_synonyms_mod")
    if _column_exists("resource_version", "title_abbreviation"):
        op.alter_column("resource_version", "title_abbreviation",
                        new_column_name="medline_abbreviation")
    if _column_exists("resource_version", "title_abbreviation_mod"):
        op.drop_column("resource_version", "title_abbreviation_mod")
    if not _column_exists("resource_version", "medline_abbreviation_mod"):
        op.add_column(
            "resource_version",
            sa.Column("medline_abbreviation_mod", sa.Boolean(),
                      server_default=sa.text("false"),
                      nullable=False))
    if not _column_exists("resource_version", "iso_abbreviation"):
        op.add_column(
            "resource_version",
            sa.Column("iso_abbreviation", sa.String(),
                      autoincrement=False, nullable=True))
    if not _column_exists("resource_version", "iso_abbreviation_mod"):
        op.add_column(
            "resource_version",
            sa.Column("iso_abbreviation_mod", sa.Boolean(),
                      server_default=sa.text("false"),
                      nullable=False))

    # Restore loosened cross_reference constraints
    if _index_exists("cross_reference", "idx_curie"):
        op.drop_index("idx_curie", table_name="cross_reference")
    op.create_index(
        "idx_curie", "cross_reference", ["curie"],
        unique=True,
        postgresql_where=text(
            "is_obsolete IS FALSE AND curie_prefix != 'ISSN'"))

    if _index_exists("cross_reference", "idx_curie_res"):
        op.drop_index("idx_curie_res", table_name="cross_reference")
    op.create_index(
        "idx_curie_res", "cross_reference",
        ["curie", "resource_id"],
        unique=True,
        postgresql_where=text(
            "resource_id IS NOT NULL AND curie_prefix != 'ISSN'"))
