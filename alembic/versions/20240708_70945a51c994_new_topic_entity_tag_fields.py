"""new topic_entity_tag fields

Revision ID: 70945a51c994
Revises: 67654300869f
Create Date: 2024-07-08 14:39:15.662425

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import orm

#from agr_literature_service.api.crud.topic_entity_tag_crud import revalidate_all_tags

# revision identifiers, used by Alembic.
revision = '70945a51c994'
down_revision = '67654300869f'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('topic_entity_tag', sa.Column('validation_by_author', sa.String(), nullable=True))
    op.add_column('topic_entity_tag', sa.Column('validation_by_professional_biocurator', sa.String(), nullable=True))
    op.add_column('topic_entity_tag_version', sa.Column('validation_by_author', sa.String(), autoincrement=False, nullable=True))
    op.add_column('topic_entity_tag_version', sa.Column('validation_by_author_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))
    op.add_column('topic_entity_tag_version', sa.Column('validation_by_professional_biocurator', sa.String(), autoincrement=False, nullable=True))
    op.add_column('topic_entity_tag_version', sa.Column('validation_by_professional_biocurator_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))
    # ### end Alembic commands ###
    # session.commit()
    # session.close()
    # print("columns added")
    # revalidate_all_tags()

def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('topic_entity_tag_version', 'validation_by_professional_biocurator_mod')
    op.drop_column('topic_entity_tag_version', 'validation_by_professional_biocurator')
    op.drop_column('topic_entity_tag_version', 'validation_by_author_mod')
    op.drop_column('topic_entity_tag_version', 'validation_by_author')
    op.drop_column('topic_entity_tag', 'validation_by_professional_biocurator')
    op.drop_column('topic_entity_tag', 'validation_by_author')
    # ### end Alembic commands ###
