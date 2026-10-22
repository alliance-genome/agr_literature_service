"""ateam_merge: topic_entity_tag.entity_id -> FK biologicalentity

Revision ID: c2e4a6b8d051
Revises: b7d9f1a3c540
Create Date: 2026-10-25 10:00:00.000000

SCRUM-6720 Phase 2. topic_entity_tag gets entity_id, the A-team biologicalentity of entity (matched on
biologicalentity.primaryexternalid), and keeps the entity string, which stays the source of truth
(biological_entity_model). Decided 2026-10-22:

- Only biologicalentity is linked. Species tags (entity_type ATP:0000123, whose entity is an
  NCBITaxon ontologyterm) keep just the string, like the entities A-team does not hold.
- ON DELETE SET NULL: an A-team delete is never blocked by a tag, which keeps its entity string.
- topic_entity_tag_version gets entity_id and entity_id_mod (DEFAULT false, so no table rewrite) but
  no backfill; the old history rows keep their entity string.

On the merged test DB (2026-10-22), of 232,154 distinct entity values (2,644,494 rows) 229,810 match a
biologicalentity. Unmatched, entity_id NULL: 1,090 species tags (133,559 rows), SGD protein complexes
and pathways (about 20,700 rows), WB transgenes (2,061), about 285 deleted genes and alleles and 29
malformed values. valid_entity_type_dependencies is unchanged: entity_id is not required, even for
entity_id_validation 'alliance', which most of the SGD complexes have.
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'c2e4a6b8d051'
down_revision = 'b7d9f1a3c540'
branch_labels = None
depends_on = None

TABLE = 'topic_entity_tag'
HISTORY = 'topic_entity_tag_version'


def upgrade():
    op.execute(f"ALTER TABLE {TABLE} ADD COLUMN entity_id bigint")
    op.execute(f"UPDATE {TABLE} t SET entity_id = be.id FROM biologicalentity be "
               "WHERE be.primaryexternalid = t.entity")
    op.execute(f"CREATE INDEX ix_{TABLE}_entity_id ON {TABLE} (entity_id)")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT {TABLE}_entity_id_fkey "
               "FOREIGN KEY (entity_id) REFERENCES biologicalentity (id) ON DELETE SET NULL")

    op.execute(f"ALTER TABLE {HISTORY} ADD COLUMN entity_id bigint, "
               "ADD COLUMN entity_id_mod BOOLEAN NOT NULL DEFAULT false")
    op.execute(f"CREATE INDEX ix_{HISTORY}_entity_id ON {HISTORY} (entity_id)")


def downgrade():
    op.execute(f"ALTER TABLE {HISTORY} DROP COLUMN entity_id, DROP COLUMN entity_id_mod")
    op.execute(f"ALTER TABLE {TABLE} DROP COLUMN entity_id")
