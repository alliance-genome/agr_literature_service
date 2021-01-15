from marshmallow import Schema, fields
from marshmallow_enum import EnumField
from enum import Enum

class TagName(Enum):
    canShowImages = 1
    PMCOpenAccess = 2
    inCorpus = 3
    notRelevant = 4

class TagSource(Enum):
    SGD = 1
    ZFIN = 2
    RGD = 3
    WB = 4
    MGI = 5
    FB = 6

class ReferenceTagSchema(Schema):
    id = fields.Int()
    tagName = EnumField(TagName)
    tagSource = EnumField(TagSource)
