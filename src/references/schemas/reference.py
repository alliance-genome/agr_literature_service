from marshmallow import Schema, fields
from marshmallow_enum import EnumField
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

from references.models.reference import Reference

from .author import AuthorSchema
from .page import PageSchema
from .meshTerm import MeshTermSchema
#from .pubmedid import PubMedIdSchema
#from .pubmodid import PubModIdSchema
#from .crossreference import CrossReferenceSchema
from .modReferenceType import ModReferenceTypeSchema
from .referenceTag import ReferenceTagSchema
from .allianceCategory import AllianceCategory

from marshmallow_sqlalchemy.fields import Nested


class ReferenceSchemaIn(SQLAlchemyAutoSchema):
    #authors = fields.List(fields.Nested(AuthorSchema))
    #pages = fields.List(fields.Nested(PageSchema))
    #keywords = fields.List(fields.Str())
    #allianceCategory = EnumField(AllianceCategory)
    #modReferenceTypes = fields.List(fields.Nested(ModReferenceTypeSchema))
    #tags = fields.List(fields.Nested(ReferenceTagSchema))
    #meshTerms = fields.List(fields.Nested(MeshTermSchema))
#    crossreferences = fields.List(fields.Nested(CrossReferenceSchema))
#    pubmedIDs = fields.List(fields.Nested(PubMedIdSchema))
#    pubmedIDs = fields.List(fields.Nested(PubMedIdSchema))
#    modIDs = fields.List(fields.Str())

    class Meta:
        model = Reference
        #include_relationships = True
        #load_instance = True
        exclude = ("dateCreated", "dateUpdated", "id")


class ReferenceSchemaOut(ReferenceSchemaIn):
    class Meta:
        model = Reference
        include_relationships = True
        load_instance = True
        exclude = ("id",)
