"""add file_classes to models

Revision ID: e7695936840b
Revises: 78e9d4f87ed5
Create Date: 2026-04-10 17:58:44.493581

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e7695936840b'
down_revision = '8a3f5c7d9e2b'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('ml_model', sa.Column('file_classes', sa.ARRAY(sa.String()), nullable=True))

    conn = op.get_bind()
    conn.execute(
        sa.text(
            "UPDATE ml_model SET file_classes = :file_classes "
            "WHERE task_type = :task_type AND file_classes IS NULL"
        ),
        {"file_classes": ["main"], "task_type": "biocuration_topic_classification"}
    )
    conn.execute(
        sa.text(
            "UPDATE ml_model SET file_classes = :file_classes "
            "WHERE task_type = :task_type AND file_classes IS NULL"
        ),
        {"file_classes": ["main", "supplement"], "task_type": "biocuration_entity_extraction"}
    )


def downgrade():
    op.drop_column('ml_model', 'file_classes')
