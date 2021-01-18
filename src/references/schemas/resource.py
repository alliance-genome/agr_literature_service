from marshmallow import Schema, fields

from .author import AuthorSchema
from .crossreference import CrossReferenceSchema

class ResourceSchema(Schema):
    id = fields.Int()
    primaryId = fields.Str(required=True)
    title = fields.Str(required=True)
    titleSynonyms = fields.List(fields.Str(), unique=True)
    isoAbbreviation = fields.Str()
    medlineAbbreviation = fields.Str()
    copyrightDate = fields.DateTime()
    publisher = fields.Str()
    printISSN = fields.Str()
    onlineISSN = fields.Str()
    editorOrAuthors = fields.List(fields.Nested(AuthorSchema))
    volumes = fields.List(fields.Str())
    pages = fields.Int()
    abstractOrSummary = fields.Str()
    #crossReferences = fields.List(fields.Nested(CrossReferenceSchema))
