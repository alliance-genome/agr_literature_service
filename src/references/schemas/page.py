from marshmallow import Schema, fields

from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from references.models.reference import Page


class PageSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Page
        include_relationships = True
        load_instance = True
