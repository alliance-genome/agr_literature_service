"""add_laboratory_and_person_lineage

Adds the Laboratory entity and its related tables (laboratory_cross_reference,
laboratory_allele_designation, laboratory_person), the person_lineage table, and
an institution ARRAY column on person. All new tables are audited and versioned
(sqlalchemy_continuum), so a matching *_version table is created for each.

Revision ID: d4e9c2b7a8f1
Revises: c8e5f9a2d3b1
Create Date: 2026-06-10 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "d4e9c2b7a8f1"
down_revision = "c8e5f9a2d3b1"
branch_labels = None
depends_on = None


# ---------- helpers ----------
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


def _audit_columns():
    return [
        sa.Column("date_created", sa.DateTime(), nullable=False),
        sa.Column("date_updated", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.Column("updated_by", sa.String(), nullable=True),
    ]


def _audit_fks():
    return [
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
    ]


# Audit columns that are tracked by continuum (carry a *_mod column in version tables).
_AUDIT_TRACKED = [
    ("date_created", sa.DateTime()),
    ("date_updated", sa.DateTime()),
    ("created_by", sa.String()),
    ("updated_by", sa.String()),
]
_AUDIT_INDEXED = ["date_created", "date_updated"]


def _create_version_table(table: str, pk_col: str, tracked_cols, indexed_cols):
    """Create a continuum *_version table.

    tracked_cols: list of (name, sa_type) for all non-PK columns (excluding audit).
    indexed_cols: list of column names to index (excluding the 3 transaction columns).
    """
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

    for c in list(indexed_cols) + _AUDIT_INDEXED + ["transaction_id", "end_transaction_id", "operation_type"]:
        op.create_index(op.f(f"ix_{vtable}_{c}"), vtable, [c], unique=False)


def _drop_version_table(table: str, indexed_cols):
    vtable = f"{table}_version"
    for c in list(indexed_cols) + _AUDIT_INDEXED + ["transaction_id", "end_transaction_id", "operation_type"]:
        op.drop_index(op.f(f"ix_{vtable}_{c}"), table_name=vtable)
    op.drop_table(vtable)


# ---------- per-table version specs (non-PK, non-audit tracked columns + indexed columns) ----------
_LAB_TRACKED = [
    ("curie", sa.String()),
    ("name", sa.String()),
    ("strain_designation", sa.String()),
    ("institution", sa.ARRAY(sa.String())),
    ("webpage", sa.ARRAY(sa.String())),
    ("city", sa.String()),
    ("state", sa.String()),
    ("postal_code", sa.String()),
    ("country", sa.String()),
    ("street_address", sa.String()),
    ("email", sa.ARRAY(sa.String())),
    ("email_visibility", sa.String()),
    ("lab_is_open", sa.Boolean()),
    ("status", sa.String()),
    ("research_area", sa.String()),
    ("short_research_description", sa.String()),
    ("additional_information", sa.String()),
    ("private_note", sa.String()),
]
_LAB_INDEXED = ["curie"]

_LAB_XREF_TRACKED = [
    ("curie", sa.String()),
    ("curie_prefix", sa.String()),
    ("laboratory_id", sa.Integer()),
    ("pages", sa.ARRAY(sa.String())),
    ("is_obsolete", sa.Boolean()),
]
_LAB_XREF_INDEXED = ["curie", "curie_prefix", "laboratory_id"]

_LAB_ALLELE_TRACKED = [
    ("laboratory_id", sa.Integer()),
    ("mod_id", sa.Integer()),
    ("allele_designation", sa.String()),
]
_LAB_ALLELE_INDEXED = ["laboratory_id", "mod_id"]

_LAB_PERSON_TRACKED = [
    ("laboratory_id", sa.Integer()),
    ("person_id", sa.Integer()),
    ("is_pi", sa.DateTime()),
    ("former_pi", sa.DateTime()),
    ("alum", sa.DateTime()),
    ("is_lab_contact", sa.Boolean()),
    ("can_edit_lab", sa.Boolean()),
    ("lab_position", sa.String()),
]
_LAB_PERSON_INDEXED = ["laboratory_id", "person_id"]

# Canonical validated PPR.
_PERSON_LINEAGE_TRACKED = [
    ("person_one_id", sa.Integer()),
    ("person_two_id", sa.Integer()),
    ("relationship", sa.String()),
    ("start_date", sa.DateTime()),
    ("end_date", sa.DateTime()),
]
_PERSON_LINEAGE_INDEXED = ["person_one_id", "person_two_id"]

# Submission / curation working space.
_PERSON_LINEAGE_SUBMISSION_TRACKED = [
    ("person_one_name", sa.String()),
    ("person_two_name", sa.String()),
    ("relationship", sa.String()),
    ("who_sent_this", sa.String()),
    ("person_one_id", sa.Integer()),
    ("person_two_id", sa.Integer()),
    ("start_date", sa.DateTime()),
    ("end_date", sa.DateTime()),
    ("status", sa.String()),
    ("person_lineage_id", sa.Integer()),
]
_PERSON_LINEAGE_SUBMISSION_INDEXED = ["person_one_id", "person_two_id", "person_lineage_id"]


def upgrade():  # noqa: C901
    conn = op.get_bind()

    # ------------------------------------------------------------------
    # person.institution (+ version column)
    # ------------------------------------------------------------------
    if not _col_exists(conn, "person", "institution"):
        op.add_column("person", sa.Column("institution", sa.ARRAY(sa.String()), nullable=True))
    if not _col_exists(conn, "person_version", "institution"):
        op.add_column(
            "person_version",
            sa.Column("institution", sa.ARRAY(sa.String()), autoincrement=False, nullable=True),
        )
    if not _col_exists(conn, "person_version", "institution_mod"):
        op.add_column(
            "person_version",
            sa.Column("institution_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        )

    # ------------------------------------------------------------------
    # laboratory
    # ------------------------------------------------------------------
    op.create_table(
        "laboratory",
        sa.Column("laboratory_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("curie", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("strain_designation", sa.String(), nullable=True),
        sa.Column("institution", sa.ARRAY(sa.String()), nullable=True),
        sa.Column("webpage", sa.ARRAY(sa.String()), nullable=True),
        sa.Column("city", sa.String(), nullable=True),
        sa.Column("state", sa.String(), nullable=True),
        sa.Column("postal_code", sa.String(), nullable=True),
        sa.Column("country", sa.String(), nullable=True),
        sa.Column("street_address", sa.String(), nullable=True),
        sa.Column("email", sa.ARRAY(sa.String()), nullable=True),
        sa.Column("email_visibility", sa.String(), nullable=True),
        sa.Column("lab_is_open", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("status", sa.String(), server_default="active", nullable=False),
        sa.Column("research_area", sa.String(), nullable=True),
        sa.Column("short_research_description", sa.String(), nullable=True),
        sa.Column("additional_information", sa.String(), nullable=True),
        sa.Column("private_note", sa.String(), nullable=True),
        *_audit_columns(),
        *_audit_fks(),
        sa.PrimaryKeyConstraint("laboratory_id"),
    )
    op.create_index(op.f("ix_laboratory_curie"), "laboratory", ["curie"], unique=False)
    op.create_index(op.f("ix_laboratory_date_created"), "laboratory", ["date_created"], unique=False)
    op.create_index(op.f("ix_laboratory_date_updated"), "laboratory", ["date_updated"], unique=False)
    _create_version_table("laboratory", "laboratory_id", _LAB_TRACKED, _LAB_INDEXED)

    # ------------------------------------------------------------------
    # laboratory_cross_reference
    # ------------------------------------------------------------------
    op.create_table(
        "laboratory_cross_reference",
        sa.Column("laboratory_cross_reference_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("curie", sa.String(), nullable=False),
        sa.Column("curie_prefix", sa.String(), nullable=False),
        sa.Column("laboratory_id", sa.Integer(), nullable=True),
        sa.Column("pages", sa.ARRAY(sa.String()), nullable=True),
        sa.Column("is_obsolete", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        *_audit_columns(),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["laboratory_id"], ["laboratory.laboratory_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("laboratory_cross_reference_id"),
        sa.UniqueConstraint("curie", name="uq_laboratory_xref_curie"),
        sa.UniqueConstraint("laboratory_id", "curie_prefix", name="uq_laboratory_xref_laboratory_prefix"),
    )
    op.create_index(
        op.f("ix_laboratory_cross_reference_curie"), "laboratory_cross_reference", ["curie"], unique=False
    )
    op.create_index(
        op.f("ix_laboratory_cross_reference_curie_prefix"),
        "laboratory_cross_reference",
        ["curie_prefix"],
        unique=False,
    )
    op.create_index(
        op.f("ix_laboratory_cross_reference_laboratory_id"),
        "laboratory_cross_reference",
        ["laboratory_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_laboratory_cross_reference_date_created"),
        "laboratory_cross_reference",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_laboratory_cross_reference_date_updated"),
        "laboratory_cross_reference",
        ["date_updated"],
        unique=False,
    )
    op.create_index(
        "ix_laboratory_xref_laboratory_id", "laboratory_cross_reference", ["laboratory_id"], unique=False
    )
    op.create_index(
        "ix_laboratory_xref_prefix_curie",
        "laboratory_cross_reference",
        ["curie_prefix", "curie"],
        unique=False,
    )
    _create_version_table(
        "laboratory_cross_reference", "laboratory_cross_reference_id", _LAB_XREF_TRACKED, _LAB_XREF_INDEXED
    )

    # ------------------------------------------------------------------
    # laboratory_allele_designation
    # ------------------------------------------------------------------
    op.create_table(
        "laboratory_allele_designation",
        sa.Column("laboratory_allele_designation_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("laboratory_id", sa.Integer(), nullable=False),
        sa.Column("mod_id", sa.Integer(), nullable=False),
        sa.Column("allele_designation", sa.String(), nullable=False),
        *_audit_columns(),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["laboratory_id"], ["laboratory.laboratory_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["mod_id"], ["mod.mod_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("laboratory_allele_designation_id"),
        sa.UniqueConstraint("laboratory_id", "mod_id", name="uq_laboratory_allele_designation_lab_mod"),
    )
    op.create_index(
        op.f("ix_laboratory_allele_designation_laboratory_id"),
        "laboratory_allele_designation",
        ["laboratory_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_laboratory_allele_designation_mod_id"),
        "laboratory_allele_designation",
        ["mod_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_laboratory_allele_designation_date_created"),
        "laboratory_allele_designation",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_laboratory_allele_designation_date_updated"),
        "laboratory_allele_designation",
        ["date_updated"],
        unique=False,
    )
    _create_version_table(
        "laboratory_allele_designation",
        "laboratory_allele_designation_id",
        _LAB_ALLELE_TRACKED,
        _LAB_ALLELE_INDEXED,
    )

    # ------------------------------------------------------------------
    # laboratory_person
    # ------------------------------------------------------------------
    op.create_table(
        "laboratory_person",
        sa.Column("laboratory_person_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("laboratory_id", sa.Integer(), nullable=False),
        sa.Column("person_id", sa.Integer(), nullable=False),
        sa.Column("is_pi", sa.DateTime(), nullable=True),
        sa.Column("former_pi", sa.DateTime(), nullable=True),
        sa.Column("alum", sa.DateTime(), nullable=True),
        sa.Column("is_lab_contact", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("can_edit_lab", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("lab_position", sa.String(), nullable=True),
        *_audit_columns(),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["laboratory_id"], ["laboratory.laboratory_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["person_id"], ["person.person_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("laboratory_person_id"),
    )
    op.create_index(
        op.f("ix_laboratory_person_laboratory_id"), "laboratory_person", ["laboratory_id"], unique=False
    )
    op.create_index(
        op.f("ix_laboratory_person_person_id"), "laboratory_person", ["person_id"], unique=False
    )
    op.create_index(
        op.f("ix_laboratory_person_date_created"), "laboratory_person", ["date_created"], unique=False
    )
    op.create_index(
        op.f("ix_laboratory_person_date_updated"), "laboratory_person", ["date_updated"], unique=False
    )
    op.create_index(
        "ix_laboratory_person_laboratory_person",
        "laboratory_person",
        ["laboratory_id", "person_id"],
        unique=False,
    )
    _create_version_table("laboratory_person", "laboratory_person_id", _LAB_PERSON_TRACKED, _LAB_PERSON_INDEXED)

    # ------------------------------------------------------------------
    # person_lineage (canonical validated PPR)
    # ------------------------------------------------------------------
    op.create_table(
        "person_lineage",
        sa.Column("person_lineage_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("person_one_id", sa.Integer(), nullable=False),
        sa.Column("person_two_id", sa.Integer(), nullable=False),
        sa.Column("relationship", sa.String(), nullable=False),
        sa.Column("start_date", sa.DateTime(), nullable=True),
        sa.Column("end_date", sa.DateTime(), nullable=True),
        *_audit_columns(),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["person_one_id"], ["person.person_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["person_two_id"], ["person.person_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("person_lineage_id"),
        sa.UniqueConstraint(
            "person_one_id", "person_two_id", "relationship",
            name="uq_person_lineage_person_ids_relationship",
        ),
    )
    op.create_index(op.f("ix_person_lineage_person_one_id"), "person_lineage", ["person_one_id"], unique=False)
    op.create_index(op.f("ix_person_lineage_person_two_id"), "person_lineage", ["person_two_id"], unique=False)
    op.create_index(
        op.f("ix_person_lineage_date_created"), "person_lineage", ["date_created"], unique=False
    )
    op.create_index(
        op.f("ix_person_lineage_date_updated"), "person_lineage", ["date_updated"], unique=False
    )
    _create_version_table("person_lineage", "person_lineage_id", _PERSON_LINEAGE_TRACKED, _PERSON_LINEAGE_INDEXED)

    # ------------------------------------------------------------------
    # person_lineage_submission (raw claim + curation working space)
    # ------------------------------------------------------------------
    op.create_table(
        "person_lineage_submission",
        sa.Column("person_lineage_submission_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("person_one_name", sa.String(), nullable=False),
        sa.Column("person_two_name", sa.String(), nullable=False),
        sa.Column("relationship", sa.String(), nullable=False),
        sa.Column("who_sent_this", sa.String(), nullable=False),
        sa.Column("person_one_id", sa.Integer(), nullable=True),
        sa.Column("person_two_id", sa.Integer(), nullable=True),
        sa.Column("start_date", sa.DateTime(), nullable=True),
        sa.Column("end_date", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(), server_default="pending", nullable=False),
        sa.Column("person_lineage_id", sa.Integer(), nullable=True),
        *_audit_columns(),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["person_one_id"], ["person.person_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["person_two_id"], ["person.person_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["person_lineage_id"], ["person_lineage.person_lineage_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("person_lineage_submission_id"),
    )
    op.create_index(
        op.f("ix_person_lineage_submission_person_one_id"),
        "person_lineage_submission", ["person_one_id"], unique=False,
    )
    op.create_index(
        op.f("ix_person_lineage_submission_person_two_id"),
        "person_lineage_submission", ["person_two_id"], unique=False,
    )
    op.create_index(
        op.f("ix_person_lineage_submission_person_lineage_id"),
        "person_lineage_submission", ["person_lineage_id"], unique=False,
    )
    op.create_index(
        op.f("ix_person_lineage_submission_date_created"),
        "person_lineage_submission", ["date_created"], unique=False,
    )
    op.create_index(
        op.f("ix_person_lineage_submission_date_updated"),
        "person_lineage_submission", ["date_updated"], unique=False,
    )
    _create_version_table(
        "person_lineage_submission", "person_lineage_submission_id",
        _PERSON_LINEAGE_SUBMISSION_TRACKED, _PERSON_LINEAGE_SUBMISSION_INDEXED,
    )


def downgrade():  # noqa: C901
    # person_lineage_submission (drop first — FK to person_lineage)
    _drop_version_table(
        "person_lineage_submission", _PERSON_LINEAGE_SUBMISSION_INDEXED
    )
    op.drop_index(op.f("ix_person_lineage_submission_date_updated"), table_name="person_lineage_submission")
    op.drop_index(op.f("ix_person_lineage_submission_date_created"), table_name="person_lineage_submission")
    op.drop_index(op.f("ix_person_lineage_submission_person_lineage_id"), table_name="person_lineage_submission")
    op.drop_index(op.f("ix_person_lineage_submission_person_two_id"), table_name="person_lineage_submission")
    op.drop_index(op.f("ix_person_lineage_submission_person_one_id"), table_name="person_lineage_submission")
    op.drop_table("person_lineage_submission")

    # person_lineage (canonical)
    _drop_version_table("person_lineage", _PERSON_LINEAGE_INDEXED)
    op.drop_index(op.f("ix_person_lineage_date_updated"), table_name="person_lineage")
    op.drop_index(op.f("ix_person_lineage_date_created"), table_name="person_lineage")
    op.drop_index(op.f("ix_person_lineage_person_two_id"), table_name="person_lineage")
    op.drop_index(op.f("ix_person_lineage_person_one_id"), table_name="person_lineage")
    op.drop_table("person_lineage")

    # laboratory_person
    _drop_version_table("laboratory_person", _LAB_PERSON_INDEXED)
    op.drop_index("ix_laboratory_person_laboratory_person", table_name="laboratory_person")
    op.drop_index(op.f("ix_laboratory_person_date_updated"), table_name="laboratory_person")
    op.drop_index(op.f("ix_laboratory_person_date_created"), table_name="laboratory_person")
    op.drop_index(op.f("ix_laboratory_person_person_id"), table_name="laboratory_person")
    op.drop_index(op.f("ix_laboratory_person_laboratory_id"), table_name="laboratory_person")
    op.drop_table("laboratory_person")

    # laboratory_allele_designation
    _drop_version_table("laboratory_allele_designation", _LAB_ALLELE_INDEXED)
    op.drop_index(op.f("ix_laboratory_allele_designation_date_updated"), table_name="laboratory_allele_designation")
    op.drop_index(op.f("ix_laboratory_allele_designation_date_created"), table_name="laboratory_allele_designation")
    op.drop_index(op.f("ix_laboratory_allele_designation_mod_id"), table_name="laboratory_allele_designation")
    op.drop_index(op.f("ix_laboratory_allele_designation_laboratory_id"), table_name="laboratory_allele_designation")
    op.drop_table("laboratory_allele_designation")

    # laboratory_cross_reference
    _drop_version_table("laboratory_cross_reference", _LAB_XREF_INDEXED)
    op.drop_index("ix_laboratory_xref_prefix_curie", table_name="laboratory_cross_reference")
    op.drop_index("ix_laboratory_xref_laboratory_id", table_name="laboratory_cross_reference")
    op.drop_index(op.f("ix_laboratory_cross_reference_date_updated"), table_name="laboratory_cross_reference")
    op.drop_index(op.f("ix_laboratory_cross_reference_date_created"), table_name="laboratory_cross_reference")
    op.drop_index(op.f("ix_laboratory_cross_reference_laboratory_id"), table_name="laboratory_cross_reference")
    op.drop_index(op.f("ix_laboratory_cross_reference_curie_prefix"), table_name="laboratory_cross_reference")
    op.drop_index(op.f("ix_laboratory_cross_reference_curie"), table_name="laboratory_cross_reference")
    op.drop_table("laboratory_cross_reference")

    # laboratory
    _drop_version_table("laboratory", _LAB_INDEXED)
    op.drop_index(op.f("ix_laboratory_date_updated"), table_name="laboratory")
    op.drop_index(op.f("ix_laboratory_date_created"), table_name="laboratory")
    op.drop_index(op.f("ix_laboratory_curie"), table_name="laboratory")
    op.drop_table("laboratory")

    # person.institution
    op.drop_column("person_version", "institution_mod")
    op.drop_column("person_version", "institution")
    op.drop_column("person", "institution")
