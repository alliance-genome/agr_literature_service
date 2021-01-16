from marshmallow import Schema, fields

class CrossReferenceSchema(Schema):
    id = fields.Str()
    type = fields.List(fields.Str())
    items = fields.Str()
