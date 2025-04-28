from typing import List, Optional

from pydantic import ConfigDict, BaseModel, field_validator
from agr_literature_service.api.schemas import AuditedObjectModelSchema


class CrossReferencePageSchemaShow(BaseModel):
    name: Optional[str] = None
    url: Optional[str] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class CrossReferenceSchemaRelated(AuditedObjectModelSchema):
    cross_reference_id: int
    curie: str
    curie_prefix: str
    url: Optional[str] = None
    pages: Optional[List[CrossReferencePageSchemaShow]] = None
    is_obsolete: Optional[bool] = None

    @field_validator('curie')
    def name_must_contain_space(cls, v):
        # if v.count(":") != 1 and not v.startswith("DOI:"):
        if v.count(":") == 0:
            raise ValueError('must contain a single colon')
        return v
    model_config = ConfigDict(from_attributes=True, extra="forbid", json_schema_extra={
        "example": {
            "curie": "MOD:curie",
            "pages": [
                "reference"
            ]
        }
    })


class CrossReferenceSchemaCreate(BaseModel):
    curie: str
    pages: Optional[List[str]] = None
    is_obsolete: Optional[bool] = False


class CrossReferenceSchemaPost(CrossReferenceSchemaCreate):
    resource_curie: Optional[str] = None
    reference_curie: Optional[str] = None
    model_config = ConfigDict(orm_mod=True, extra="forbid", json_schema_extra={
        "example": {
            "curie": "MOD:curie",
            "pages": [
                "reference"
            ],
            "reference_curie": "AGRKB:101"
        }
    })


class CrossReferenceSchemaShow(AuditedObjectModelSchema):
    cross_reference_id: int
    curie: str
    curie_prefix: str
    url: Optional[str] = None
    pages: Optional[List[CrossReferencePageSchemaShow]] = None
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None
    is_obsolete: Optional[bool]


class CrossReferenceSchemaUpdate(BaseModel):
    pages: Optional[List[str]] = None
    resource_curie: Optional[str] = None
    reference_curie: Optional[str] = None
    is_obsolete: Optional[bool] = None
    curie: Optional[str] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")
