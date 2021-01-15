from marshmallow import Schema, fields
from marshmallow_enum import EnumField

from .author import AuthorSchema
from .pubmedid import PubMedIdSchema
from .pubmodid import PubModIdSchema
from .crossreference import CrossReferenceSchema
from .modReferenceType import ModReferenceTypeSchema
from .referenceTag import ReferenceTagSchema

from .allianceCategory import AllianceCategory

class ReferenceSchema(Schema):
    id = fields.Int()
    primaryId = fields.Str(required=True)
    title = fields.Str(required=True)
    authors = fields.List(fields.Nested(AuthorSchema))
    datePublished = fields.Str(required=True)
    dateArrivedInPubMed = fields.Str()
    dateLastModified = fields.Str()
    volume = fields.Str()
    pages = fields.Str()
    abstract = fields.Str()
    citation = fields.Str(required=True)
    keywords = fields.List(fields.Str())
    pubMedType = fields.Str()
    publisher = fields.Str()
    allianceCategory = EnumField(AllianceCategory)
    modReferenceTypes = fields.List(fields.Nested(ModReferenceTypeSchema))
    issueName = fields.Str()
    issueDate = fields.Str()
    tags = fields.List(fields.Nested(ReferenceTagSchema))
    meshTerms = fields.List(fields.Str())
    crossreferences = fields.List(fields.Nested(CrossReferenceSchema))
    pubmedIDs = fields.List(fields.Nested(PubMedIdSchema))
    pubmedIDs = fields.List(fields.Nested(PubMedIdSchema))
    modIDs = fields.List(fields.Str())
    resourceAbbreviation = fields.Str()
    dateTimeCreated = fields.DateTime()
