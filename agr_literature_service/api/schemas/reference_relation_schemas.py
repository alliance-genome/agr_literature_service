from typing import Optional

from pydantic import BaseModel, field_validator

from agr_literature_service.api.schemas import ReferenceRelationType


class ReferenceRelationSchemaPost(BaseModel):
    reference_curie_from: str
    reference_curie_to: str
    reference_relation_type: ReferenceRelationType

    @field_validator('reference_curie_from')
    def from_must_be_alliance_reference_curie(cls, v):
        if not v.startswith("AGRKB:101"):
            raise ValueError('must start with AGRKB:101')
        return v

    @field_validator('reference_curie_to')
    def to_must_be_alliance_reference_curie(cls, v):
        if not v.startswith("AGRKB:101"):
            raise ValueError('must start with AGRKB:101')
        return v

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceRelationSchemaShow(ReferenceRelationSchemaPost):
    reference_relation_id: int

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceRelationSchemaPatch(BaseModel):
    reference_curie_from: Optional[str] = None
    reference_curie_to: Optional[str] = None
    reference_relation_type: Optional[ReferenceRelationType] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceRelationSchemaRelated(BaseModel):
    reference_relation_id: Optional[int] = None
    reference_curie_from: Optional[str] = None
    reference_curie_to: Optional[str] = None
    reference_relation_type: Optional[ReferenceRelationType] = None

    class Config():
        orm_mode = True
        extra = "forbid"
