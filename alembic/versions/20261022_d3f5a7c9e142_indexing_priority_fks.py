"""ateam_merge: indexing_priority predicted/curator priorities -> FK ontologyterm

Revision ID: d3f5a7c9e142
Revises: b8d2f4e6a031
Create Date: 2026-10-22 14:00:00.000000

SCRUM-6720 Phase 1, the pattern of b8d2f4e6a031 on two nullable columns:

  indexing_priority.predicted_indexing_priority -> predicted_indexing_priority_id (FK, indexed)
  indexing_priority.curator_indexing_priority   -> curator_indexing_priority_id   (FK)

Both are ATP terms. The upgrade stops, listing the values, if any non-NULL value is not an ATP term
in ontologyterm (none on the merged test DB, 2026-10-22: 67,495 predicted, 3 curator). The two
LIKE 'ATP:%' CHECKs go; ck_at_least_one_priority is rebuilt on the id columns. No history table.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'd3f5a7c9e142'
down_revision = 'b8d2f4e6a031'
branch_labels = None
depends_on = None

COLUMNS = ('predicted_indexing_priority', 'curator_indexing_priority')


def upgrade():
    for column in COLUMNS:
        op.execute(f"ALTER TABLE indexing_priority ADD COLUMN {column}_id bigint")
        op.execute(f"""
            UPDATE indexing_priority ip SET {column}_id = ot.id
            FROM ontologyterm ot
            WHERE ot.curie = ip.{column} AND ot.ontologytermtype = 'ATPTerm'
        """)
        unresolved = op.get_bind().execute(text(
            f"SELECT {column}, count(*) FROM indexing_priority WHERE {column} IS NOT NULL AND {column}_id IS NULL "
            "GROUP BY 1 ORDER BY 2 DESC")).fetchall()
        if unresolved:
            raise RuntimeError(f"indexing_priority.{column} values that are not ATP terms in ontologyterm: "
                               + ", ".join(f"{value} ({count})" for value, count in unresolved))
        op.execute(f"ALTER TABLE indexing_priority ADD CONSTRAINT indexing_priority_{column}_id_fkey "
                   f"FOREIGN KEY ({column}_id) REFERENCES ontologyterm (id)")
    op.execute("CREATE INDEX ix_indexing_priority_predicted_indexing_priority_id "
               "ON indexing_priority (predicted_indexing_priority_id)")
    op.execute("ALTER TABLE indexing_priority DROP CONSTRAINT ck_at_least_one_priority")
    op.execute("ALTER TABLE indexing_priority ADD CONSTRAINT ck_at_least_one_priority "
               "CHECK (predicted_indexing_priority_id IS NOT NULL OR curator_indexing_priority_id IS NOT NULL)")
    op.execute("ALTER TABLE indexing_priority DROP CONSTRAINT ck_predicted_indexing_priority_prefix")
    op.execute("ALTER TABLE indexing_priority DROP CONSTRAINT ck_curator_indexing_priority_prefix")
    op.execute("DROP INDEX ix_indexing_priority_predicted_indexing_priority")
    for column in COLUMNS:
        op.execute(f"ALTER TABLE indexing_priority DROP COLUMN {column}")


def downgrade():
    for column in COLUMNS:
        op.execute(f"ALTER TABLE indexing_priority ADD COLUMN {column} varchar")
        op.execute(f"UPDATE indexing_priority ip SET {column} = ot.curie "
                   f"FROM ontologyterm ot WHERE ot.id = ip.{column}_id")
        op.execute(f"ALTER TABLE indexing_priority ADD CONSTRAINT ck_{column}_prefix "
                   f"CHECK ({column} IS NULL OR {column} LIKE 'ATP:%')")
    op.execute("CREATE INDEX ix_indexing_priority_predicted_indexing_priority "
               "ON indexing_priority (predicted_indexing_priority)")
    op.execute("ALTER TABLE indexing_priority DROP CONSTRAINT ck_at_least_one_priority")
    op.execute("ALTER TABLE indexing_priority ADD CONSTRAINT ck_at_least_one_priority "
               "CHECK (predicted_indexing_priority IS NOT NULL OR curator_indexing_priority IS NOT NULL)")
    for column in COLUMNS:
        op.execute(f"ALTER TABLE indexing_priority DROP COLUMN {column}_id")
