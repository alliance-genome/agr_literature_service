from marshmallow import Schema, fields

from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from references.models.reference import MeshTerm


class MeshTermSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = MeshTerm
        include_relationships = True
        load_instance = True
