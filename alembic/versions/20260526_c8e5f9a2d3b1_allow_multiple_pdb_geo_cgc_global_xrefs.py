"""allow multiple PDB GEO CGC global xrefs and re-enforce ISSN uniqueness

Revision ID: c8e5f9a2d3b1
Revises: b1f8e7d6c5a4
Create Date: 2026-05-26

Rewrites idx_curie's predicate so the same GEO / PDB / CGC accession can
appear in multiple cross_reference rows (e.g. one GEO dataset cited by many
papers), while ISSN is no longer excepted and is globally unique again.
Keeps idx_curie in lockstep with idx_curie_prefix_ref_no_cgc, which
already excludes the same PDB/GEO/CGC prefix set per-reference.
idx_curie_res is intentionally left unchanged.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect


revision = "c8e5f9a2d3b1"
down_revision = "b1f8e7d6c5a4"
branch_labels = None
depends_on = None


def _index_exists(table, name):
    bind = op.get_bind()
    insp = sa_inspect(bind)
    for idx in insp.get_indexes(table):
        if idx["name"] == name:
            return True
    return False


def upgrade():
    if _index_exists("cross_reference", "idx_curie"):
        op.drop_index("idx_curie", table_name="cross_reference")
    op.create_index(
        "idx_curie", "cross_reference", ["curie"],
        unique=True,
        postgresql_where=sa.text(
            "is_obsolete IS FALSE "
            "AND curie_prefix NOT IN ('CGC', 'PDB', 'GEO')"
        )
    )


def downgrade():
    if _index_exists("cross_reference", "idx_curie"):
        op.drop_index("idx_curie", table_name="cross_reference")
    op.create_index(
        "idx_curie", "cross_reference", ["curie"],
        unique=True,
        postgresql_where=sa.text(
            "is_obsolete IS FALSE AND curie_prefix != 'ISSN'"
        )
    )
