from marshmallow import Schema, fields

class ModReferenceTypeSchema(Schema):
    id = fields.Int()
    referenceType = fields.Str(required=True)
    source = fields.Str(required=True)
