import logging

from marshmallow import Schema, fields


logger = logging.getLogger('literature logger')


class AuthorSchema(Schema):
    id = fields.Int()
    referenceId = fields.Int()
    name = fields.Str()
    firstName = fields.Str()
    lastName = fields.Str()
    #middleNames = fields.List(fields.Str())
    #crossreferences

class PubModIdSchema(Schema):
    id = fields.Str()
    mod = fields.Str()
    datetimeCreated = fields.DateTime()

class PubMedIdSchema(Schema):
    id = fields.Str()
    datetimeCreated = fields.Str()

class ResourceSchema(Schema):
    id = fields.Int()
    primaryId = fields.Str()
    title = fields.Str()
    authors = fields.List(fields.Nested(AuthorSchema))
    datePublished = fields.Str()
    dateArrivedInPubMed = fields.Str()
    dateLastModified = fields.Str()
    volume = fields.Str()
    pages = fields.Str()
    abstract = fields.Str()
    citation = fields.Str()
    keywords = fields.List(fields.Str())
    pubMedType = fields.Str()
    publisher = fields.Str()
    allianceCategory = fields.Str()
    modReferenceTypes = fields.List(fields.Str())
    issueName = fields.Str()
    issueDate = fields.Str()
    tags = fields.List(fields.Str())
    meshTerms = fields.List(fields.Str())
    # Crossreference
    pubmedIDs = fields.List(fields.Nested(PubMedIdSchema))
    pubmodIDs = fields.List(fields.Nested(PubModIdSchema))
    resourceAbbreviation = fields.Str()
    dateTimeCreated = fields.DateTime()

