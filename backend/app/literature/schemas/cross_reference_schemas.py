from typing import List, Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator


class CrossReferenceSchemaRelated(BaseModel):
    curie: str
    pages: Optional[List[str]] = None

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


class CrossReferenceSchema(BaseModel):
    curie: str
    pages: Optional[List[CrossReferencePageSchemaShow]] = None
    url: Optional[str] = None

    resource_curie:  Optional[str] = None
    reference_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class CrossReferenceSchemaUpdate(BaseModel):
    pages: Optional[List[str]] = None
    resource_curie:  Optional[str] = None
    reference_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
