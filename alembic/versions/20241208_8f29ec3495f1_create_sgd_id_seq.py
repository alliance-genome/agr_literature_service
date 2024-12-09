"""create_sgd_id_seq

Revision ID: 8f29ec3495f1
Revises: 7a72ee65a911
Create Date: 2024-12-08 03:36:50.391469

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '8f29ec3495f1'
down_revision = '7a72ee65a911'
branch_labels = None
depends_on = None


def upgrade():
    # Create the sgd_id_seq sequence
    op.execute("""
        CREATE SEQUENCE IF NOT EXISTS sgd_id_seq
        START WITH 100000001
        INCREMENT BY 1;
    """)


def downgrade():
    # Drop the sgd_id_seq sequence
    op.execute("""
        DROP SEQUENCE IF EXISTS sgd_id_seq;
    """)
