from typing import List, Optional

from pydantic import BaseModel, validator


class CrossReferenceSchemaRelated(BaseModel):
    curie: str
    pages: Optional[List[str]] = None
    is_obsolete: Optional[bool] = None

    @validator('curie')
    def name_must_contain_space(cls, v):
        if v.count(":") != 1 and not v.startswith("DOI:"):
            raise ValueError('must contain a single colon')
        return v

    class Config():
        orm_mode = True
        extra = "forbid"
        schema_extra = {
            "example": {
                "curie": "MOD:curie",
                "pages": [
                    "reference"
                ]
            }
        }


class CrossReferenceSchemaPost(CrossReferenceSchemaRelated):
    resource_curie: Optional[str] = None
    reference_curie: Optional[str] = None

    class Config():
        orm_mod = True
        extra = "forbid"
        schema_extra = {
            "example": {
                "curie": "MOD:curie",
                "pages": [
                    "reference"
                ],
                "reference_curie": "AGRKB:101"
            }
        }


class CrossReferencePageSchemaShow(BaseModel):
    name: Optional[str] = None
    url: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class CrossReferenceSchemaShow(BaseModel):
    curie: str
    url: Optional[str] = None
    pages: Optional[List[CrossReferencePageSchemaShow]] = None
    is_obsolete: Optional[bool]


class CrossReferenceSchema(BaseModel):
    cross_reference_id: int
    curie: str
    pages: Optional[List[CrossReferencePageSchemaShow]] = None
    url: Optional[str] = None
    is_obsolete: Optional[bool] = False
    curie_prefix: str

    resource_curie: Optional[str] = None
    reference_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class CrossReferenceSchemaUpdate(BaseModel):
    pages: Optional[List[str]] = None
    resource_curie: Optional[str] = None
    reference_curie: Optional[str] = None
    is_obsolete: Optional[bool] = None
    curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
