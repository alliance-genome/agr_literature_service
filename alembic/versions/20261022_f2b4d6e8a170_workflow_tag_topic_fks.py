"""ateam_merge: workflow_tag_topic workflow_tag / topic -> FK ontologyterm

Revision ID: f2b4d6e8a170
Revises: e6a8c0b2d453
Create Date: 2026-10-22 18:00:00.000000

SCRUM-6720 Phase 1, the pattern of b8d2f4e6a031 on two NOT NULL columns:

  workflow_tag_topic.workflow_tag -> workflow_tag_id (ATPTerm, unique index)
  workflow_tag_topic.topic        -> topic_id        (ATPTerm)

The upgrade stops, listing the values, if any value is not an ATP term in ontologyterm (none on
the merged test DB, 2026-10-22: 24 rows). The unique index on workflow_tag moves to workflow_tag_id.
No history table.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'f2b4d6e8a170'
down_revision = 'e6a8c0b2d453'
branch_labels = None
depends_on = None

COLUMNS = ('workflow_tag', 'topic')


def upgrade():
    for column in COLUMNS:
        op.execute(f"ALTER TABLE workflow_tag_topic ADD COLUMN {column}_id bigint")
        op.execute(f"""
            UPDATE workflow_tag_topic w SET {column}_id = ot.id
            FROM ontologyterm ot
            WHERE ot.curie = w.{column} AND ot.ontologytermtype = 'ATPTerm'
        """)
        unresolved = op.get_bind().execute(text(
            f"SELECT {column}, count(*) FROM workflow_tag_topic WHERE {column}_id IS NULL "
            "GROUP BY 1 ORDER BY 2 DESC")).fetchall()
        if unresolved:
            raise RuntimeError(f"workflow_tag_topic.{column} values that are not ATP terms in ontologyterm: "
                               + ", ".join(f"{value} ({count})" for value, count in unresolved))
        op.execute(f"ALTER TABLE workflow_tag_topic ALTER COLUMN {column}_id SET NOT NULL")
        op.execute(f"ALTER TABLE workflow_tag_topic ADD CONSTRAINT workflow_tag_topic_{column}_id_fkey "
                   f"FOREIGN KEY ({column}_id) REFERENCES ontologyterm (id)")
    op.execute("CREATE UNIQUE INDEX ix_workflow_tag_topic_workflow_tag_id ON workflow_tag_topic (workflow_tag_id)")
    op.execute("DROP INDEX ix_workflow_tag_topic_workflow_tag")
    for column in COLUMNS:
        op.execute(f"ALTER TABLE workflow_tag_topic DROP COLUMN {column}")


def downgrade():
    for column in COLUMNS:
        op.execute(f"ALTER TABLE workflow_tag_topic ADD COLUMN {column} varchar")
        op.execute(f"UPDATE workflow_tag_topic w SET {column} = ot.curie "
                   f"FROM ontologyterm ot WHERE ot.id = w.{column}_id")
        op.execute(f"ALTER TABLE workflow_tag_topic ALTER COLUMN {column} SET NOT NULL")
    op.execute("CREATE UNIQUE INDEX ix_workflow_tag_topic_workflow_tag ON workflow_tag_topic (workflow_tag)")
    for column in COLUMNS:
        op.execute(f"ALTER TABLE workflow_tag_topic DROP COLUMN {column}_id")
