from marshmallow import fields
from marshmallow import validate
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

from references.models.reference import Tag
from references.models.reference import TagName
from references.models.reference import TagSource

class ReferenceTagSchema(SQLAlchemyAutoSchema):
    tagName = fields.Str(validate=validate.OneOf([el.name for el in TagName]),
                         required=True)
    tagSource = fields.Str(validate=validate.OneOf([el.name for el in TagSource]),
                           required=True)
    class Meta:
        model = Tag
        include_relationships = True
        load_instance = True
