from enum import Enum
from marshmallow import fields
from marshmallow import Schema
from marshmallow import validate
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from marshmallow_sqlalchemy.fields import Nested

from references.models.reference import Reference

from .author import AuthorSchema
from .page import PageSchema
from .meshTerm import MeshTermSchema
#from .crossreference import CrossReferenceSchema
from .modReferenceType import ModReferenceTypeSchema
from .referenceTag import ReferenceTagSchema
from .allianceCategory import AllianceCategory

from marshmallow_sqlalchemy.fields import Nested


class ReferenceSchemaIn(SQLAlchemyAutoSchema):
    authors = Nested(AuthorSchema, exclude=('id', 'reference'),  many=True)
    keywords = fields.List(fields.Str())
    pages = Nested(PageSchema, exclude=('id', 'reference'),  many=True)
    allianceCategory = fields.Str(validate=validate.OneOf([el.name for el in AllianceCategory]),
        required=True,
        error='Invalid value specified for "allianceCategory". Valid values are: ' + ' '.join(list(map(str,AllianceCategory))),
        description='Type of reference')
    modReferenceTypes = Nested(ModReferenceTypeSchema, exclude=('id',),  many=True)
    tags = fields.List(fields.Nested(ReferenceTagSchema))
    meshTerms = Nested(MeshTermSchema, exclude=('id', 'reference'),  many=True)
    tags = Nested(ReferenceTagSchema, exclude=('id', 'reference'),  many=True)
#    crossreferences = fields.List(fields.Nested(CrossReferenceSchema))
#    modIDs = fields.List(fields.Str())

    class Meta:
        model = Reference
        include_relationships = True
        load_instance = True
        exclude = ("dateCreated", "dateUpdated", "id")


class ReferenceSchemaOut(SQLAlchemyAutoSchema):
    class Meta:
        model = Reference
        include_relationships = True
        load_instance = True
        exclude = ("id",)
