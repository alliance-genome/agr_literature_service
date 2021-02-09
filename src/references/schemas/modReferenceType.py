from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

class ModReferenceTypeSchema(SQLAlchemyAutoSchema):
    id = fields.Int()
    referenceType = fields.Str(required=True)
    source = fields.Str(required=True)
