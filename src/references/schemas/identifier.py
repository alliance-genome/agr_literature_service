from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

from references.models.reference import Identifier


class IdentifierSchema(SQLAlchemyAutoSchema):
    class Meta:
          model = Identifier
          include_relationships = True
          load_instance = True
          exclude = ("id", "reference")

