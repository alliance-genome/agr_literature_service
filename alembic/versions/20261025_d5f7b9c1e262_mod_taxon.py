"""ateam_merge: mod.taxon_ids -> mod_taxon (FK ontologyterm)

Revision ID: d5f7b9c1e262
Revises: c2e4a6b8d051
Create Date: 2026-10-25 12:00:00.000000

SCRUM-6720 Phase 2. The mod.taxon_ids array of NCBITaxon curies becomes rows of mod_taxon (mod_id,
ordinal, ontologyterm_id), in the array's order (mod_crud.taxons() takes the first as the MOD's
taxon). ModModel.taxon_ids stays the list of curies (an association proxy), so the API is unchanged.

mod_taxon is not versioned; mod_version loses taxon_ids / taxon_ids_mod, which hold nothing on the
merged test DB (2026-10-22: one mod_version row, taxon_ids NULL). There the 8 taxa of the 7 MODs with
taxa all resolve; the upgrade stops, listing them, if any is not an NCBITaxon term.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'd5f7b9c1e262'
down_revision = 'c2e4a6b8d051'
branch_labels = None
depends_on = None


def upgrade():
    unresolved = op.get_bind().execute(text(
        "SELECT m.abbreviation, t.curie FROM mod m, unnest(m.taxon_ids) AS t(curie) "
        "WHERE NOT EXISTS (SELECT 1 FROM ontologyterm ot "
        "                  WHERE ot.curie = t.curie AND ot.ontologytermtype = 'NCBITaxonTerm')")).fetchall()
    if unresolved:
        raise RuntimeError("mod.taxon_ids values that are not NCBITaxon terms in ontologyterm: "
                           + ", ".join(f"{mod} {curie}" for mod, curie in unresolved))

    op.execute("""CREATE TABLE mod_taxon (
        id bigserial PRIMARY KEY,
        mod_id bigint NOT NULL REFERENCES mod (id) ON DELETE CASCADE,
        ordinal integer NOT NULL,
        ontologyterm_id bigint NOT NULL REFERENCES ontologyterm (id))""")
    op.execute("CREATE INDEX ix_mod_taxon_mod_id ON mod_taxon (mod_id)")
    op.execute("INSERT INTO mod_taxon (mod_id, ordinal, ontologyterm_id) "
               "SELECT m.id, t.ordinality - 1, ot.id "
               "FROM mod m, unnest(m.taxon_ids) WITH ORDINALITY AS t(curie, ordinality) "
               "JOIN ontologyterm ot ON ot.curie = t.curie AND ot.ontologytermtype = 'NCBITaxonTerm' "
               "ORDER BY m.id, t.ordinality")
    op.execute("ALTER TABLE mod DROP COLUMN taxon_ids")
    op.execute("ALTER TABLE mod_version DROP COLUMN taxon_ids, DROP COLUMN taxon_ids_mod")


def downgrade():
    op.execute("ALTER TABLE mod_version ADD COLUMN taxon_ids varchar[], "
               "ADD COLUMN taxon_ids_mod BOOLEAN NOT NULL DEFAULT false")
    op.execute("ALTER TABLE mod ADD COLUMN taxon_ids varchar[]")
    op.execute("UPDATE mod m SET taxon_ids = (SELECT array_agg(ot.curie ORDER BY mt.ordinal) "
               "FROM mod_taxon mt JOIN ontologyterm ot ON ot.id = mt.ontologyterm_id WHERE mt.mod_id = m.id)")
    op.execute("DROP TABLE mod_taxon")
