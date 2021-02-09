from marshmallow_sqlalchemy import SQLAlchemyAutoSchema


from references.models.reference import Tag

class ReferenceTagSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Tag
        include_relationships = True
        load_instance = True
