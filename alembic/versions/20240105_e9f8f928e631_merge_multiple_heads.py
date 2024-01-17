"""merge multiple heads

Revision ID: e9f8f928e631
Revises: e5923f158550, 58f23aa35fd2
Create Date: 2024-01-05 20:21:53.518530

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e9f8f928e631'
down_revision = ('e5923f158550', '58f23aa35fd2')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
