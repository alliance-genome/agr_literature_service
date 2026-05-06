"""add pg_trgm and functional indices for person and email

Revision ID: 5f3a8e2d1c4b
Revises: d9e0f1a2b3c4
Create Date: 2026-05-06

Replaces the misnamed plain btree on ``person.display_name`` (originally
``ix_person_display_name_trigram``) with a real GIN trigram, adds GIN
trigrams on ``person_name.{first,middle,last}_name``, and adds a
functional ``lower(email_address)`` btree on ``email`` so that
``func.lower(...) == :norm`` lookups can use an index. The plain
``ix_email_address`` btree is left in place because raw equality
queries on ``email_address`` still exist in other CRUDs.
"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "5f3a8e2d1c4b"
down_revision = "d9e0f1a2b3c4"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.drop_index("ix_person_display_name_trigram", table_name="person")
    op.create_index(
        "ix_person_display_name_trgm",
        "person",
        ["display_name"],
        postgresql_using="gin",
        postgresql_ops={"display_name": "gin_trgm_ops"},
    )

    for col in ("first_name", "middle_name", "last_name"):
        op.create_index(
            f"ix_person_name_{col}_trgm",
            "person_name",
            [col],
            postgresql_using="gin",
            postgresql_ops={col: "gin_trgm_ops"},
        )

    op.execute(
        "CREATE INDEX ix_email_lower_email_address "
        "ON email (lower(email_address))"
    )


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_email_lower_email_address")
    for col in ("last_name", "middle_name", "first_name"):
        op.drop_index(f"ix_person_name_{col}_trgm", table_name="person_name")
    op.drop_index("ix_person_display_name_trgm", table_name="person")
    op.create_index(
        "ix_person_display_name_trigram",
        "person",
        ["display_name"],
        unique=False,
    )
    # pg_trgm extension intentionally left in place: dropping it would
    # CASCADE-drop any other GIN trigram index that may exist.
