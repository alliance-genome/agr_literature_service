"""ateam_merge: manual_indexing_tag.curation_tag -> FK ontologyterm

Revision ID: b8d2f4e6a031
Revises: c4a1e7b93d20
Create Date: 2026-10-22 12:00:00.000000

SCRUM-6720 Phase 1, first table (the pattern for the other ontology curie columns).

  manual_indexing_tag.curation_tag (varchar ATP curie)  ->  curation_tag_id (bigint, FK
  ontologyterm.id, an ATPTerm)

The model keeps curation_tag as the curie (ontology_term_model.ontology_curie), so the API is
unchanged. The upgrade stops, listing the values, if any curation_tag is not an ATP term in
ontologyterm (none on the merged test DB, 2026-10-22: 6,959 rows, 1 distinct value). The unique
key (mod_id, reference_id, curation_tag) moves to curation_tag_id; the LIKE 'ATP:%' CHECK goes,
replaced by the FK and the ATPTerm check on write. No history table.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'b8d2f4e6a031'
down_revision = 'c4a1e7b93d20'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE manual_indexing_tag ADD COLUMN curation_tag_id bigint")
    op.execute("""
        UPDATE manual_indexing_tag mit SET curation_tag_id = ot.id
        FROM ontologyterm ot
        WHERE ot.curie = mit.curation_tag AND ot.ontologytermtype = 'ATPTerm'
    """)
    unresolved = op.get_bind().execute(text(
        "SELECT curation_tag, count(*) FROM manual_indexing_tag WHERE curation_tag_id IS NULL "
        "GROUP BY 1 ORDER BY 2 DESC")).fetchall()
    if unresolved:
        raise RuntimeError("manual_indexing_tag.curation_tag values that are not ATP terms in ontologyterm: "
                           + ", ".join(f"{value} ({count})" for value, count in unresolved))
    op.execute("ALTER TABLE manual_indexing_tag ALTER COLUMN curation_tag_id SET NOT NULL")
    op.execute("ALTER TABLE manual_indexing_tag ADD CONSTRAINT manual_indexing_tag_curation_tag_id_fkey "
               "FOREIGN KEY (curation_tag_id) REFERENCES ontologyterm (id)")
    op.execute("CREATE INDEX ix_manual_indexing_tag_curation_tag_id ON manual_indexing_tag (curation_tag_id)")
    op.execute("ALTER TABLE manual_indexing_tag DROP CONSTRAINT uq_mod_ref_tag")
    op.execute("ALTER TABLE manual_indexing_tag ADD CONSTRAINT uq_mod_ref_tag "
               "UNIQUE (mod_id, reference_id, curation_tag_id)")
    op.execute("ALTER TABLE manual_indexing_tag DROP CONSTRAINT ck_confval_curation_tag_prefix")
    op.execute("DROP INDEX ix_manual_indexing_tag_curation_tag")
    op.execute("ALTER TABLE manual_indexing_tag DROP COLUMN curation_tag")


def downgrade():
    op.execute("ALTER TABLE manual_indexing_tag ADD COLUMN curation_tag varchar(64)")
    op.execute("UPDATE manual_indexing_tag mit SET curation_tag = ot.curie "
               "FROM ontologyterm ot WHERE ot.id = mit.curation_tag_id")
    op.execute("ALTER TABLE manual_indexing_tag ALTER COLUMN curation_tag SET NOT NULL")
    op.execute("CREATE INDEX ix_manual_indexing_tag_curation_tag ON manual_indexing_tag (curation_tag)")
    op.execute("ALTER TABLE manual_indexing_tag ADD CONSTRAINT ck_confval_curation_tag_prefix "
               "CHECK (curation_tag LIKE 'ATP:%')")
    op.execute("ALTER TABLE manual_indexing_tag DROP CONSTRAINT uq_mod_ref_tag")
    op.execute("ALTER TABLE manual_indexing_tag ADD CONSTRAINT uq_mod_ref_tag "
               "UNIQUE (mod_id, reference_id, curation_tag)")
    op.execute("ALTER TABLE manual_indexing_tag DROP COLUMN curation_tag_id")
