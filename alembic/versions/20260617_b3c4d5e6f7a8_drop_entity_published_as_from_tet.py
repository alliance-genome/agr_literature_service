"""drop entity_published_as from topic_entity_tag

Removes the entity_published_as column from topic_entity_tag (SCRUM-4218) —
that information will be stored in the A-team DB instead. The table is
versioned (sqlalchemy-continuum), so the column and its tracking flag
(entity_published_as_mod) are also dropped from topic_entity_tag_version.

Revision ID: b3c4d5e6f7a8
Revises: f1a2b3c4d5e6
Create Date: 2026-06-17

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b3c4d5e6f7a8'
down_revision = 'f1a2b3c4d5e6'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('topic_entity_tag', 'entity_published_as')
    op.drop_column('topic_entity_tag_version', 'entity_published_as')
    op.drop_column('topic_entity_tag_version', 'entity_published_as_mod')


def downgrade():
    op.add_column('topic_entity_tag', sa.Column('entity_published_as', sa.String(), nullable=True))
    op.add_column('topic_entity_tag_version', sa.Column('entity_published_as', sa.String(), autoincrement=False, nullable=True))
    op.add_column('topic_entity_tag_version', sa.Column('entity_published_as_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))
