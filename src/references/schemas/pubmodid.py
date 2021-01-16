from marshmallow import Schema, fields

class PubModIdSchema(Schema):
    id = fields.Str()
    mod = fields.Str()
    datetimeCreated = fields.DateTime()
