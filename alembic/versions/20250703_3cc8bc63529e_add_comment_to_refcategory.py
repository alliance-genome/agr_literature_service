"""add_comment_to_refcategory

Revision ID: 3cc8bc63529e
Revises: eeaedb350562
Create Date: 2025-07-03 18:47:24.931729

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '3cc8bc63529e'
down_revision = 'eeaedb350562'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("ALTER TYPE referencecategory ADD VALUE IF NOT EXISTS 'Comment' AFTER 'Retraction'")
    pass
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
