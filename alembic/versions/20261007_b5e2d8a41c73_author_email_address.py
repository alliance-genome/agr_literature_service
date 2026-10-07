"""author email_address

Revision ID: b5e2d8a41c73
Revises: dca4009ee029
Create Date: 2026-10-07 12:00:00.000000

SCRUM-6513: PubMed attaches an author's email to that author (inside the
author's own AffiliationInfo, usually as "Electronic address: x@y.org"), so it
is stored on the author row instead of only at the reference level
(reference_email).

1. author.email_address (and its sqlalchemy-continuum version columns).
2. ck_person_only_link_only also requires email_address IS NULL on a
   person-only link row (no author_order), like every other author metadata
   field. The constraint is recreated NOT VALID and then validated, so the
   full scan of the author table runs under SHARE UPDATE EXCLUSIVE instead of
   blocking writes. No existing row can fail it: the column is new and NULL.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b5e2d8a41c73'
down_revision = 'dca4009ee029'
branch_labels = None
depends_on = None

_PERSON_ONLY_BASE = (
    "author_order IS NOT NULL OR ("
    "name IS NULL AND first_name IS NULL AND last_name IS NULL "
    "AND first_initial IS NULL AND orcid IS NULL AND affiliations IS NULL "
    "{email}"
    "AND COALESCE(first_author, false) = false "
    "AND COALESCE(corresponding_author, false) = false)"
)


def _recreate_person_only_check(include_email: bool):
    op.drop_constraint("ck_person_only_link_only", "author", type_="check")
    condition = _PERSON_ONLY_BASE.format(email="AND email_address IS NULL " if include_email else "")
    op.execute(f"ALTER TABLE author ADD CONSTRAINT ck_person_only_link_only CHECK ({condition}) NOT VALID")
    op.execute("ALTER TABLE author VALIDATE CONSTRAINT ck_person_only_link_only")


def upgrade():
    op.add_column('author', sa.Column('email_address', sa.String(), nullable=True))
    op.add_column('author_version', sa.Column('email_address', sa.String(), autoincrement=False, nullable=True))
    op.add_column('author_version', sa.Column('email_address_mod', sa.Boolean(),
                                              server_default=sa.text('false'), nullable=False))
    _recreate_person_only_check(include_email=True)


def downgrade():
    _recreate_person_only_check(include_email=False)
    op.drop_column('author_version', 'email_address_mod')
    op.drop_column('author_version', 'email_address')
    op.drop_column('author', 'email_address')
