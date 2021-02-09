from marshmallow import Schema, fields

from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from references.models.reference import Author


class AuthorSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Author
        include_relationships = True
        load_instance = True
