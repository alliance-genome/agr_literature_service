from marshmallow import Schema, fields

class PubMedIdSchema(Schema):
    id = fields.Str()
    datetimeCreated = fields.Str()
