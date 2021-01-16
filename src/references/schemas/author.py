from marshmallow import Schema, fields

class AuthorSchema(Schema):
    id = fields.Int()
    referenceId = fields.Int()
    name = fields.Str()
    firstName = fields.Str()
    lastName = fields.Str()
    #middleNames = fields.List(fields.Str())
    #crossreferences
