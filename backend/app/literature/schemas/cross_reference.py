from typing import List, Optional

from pydantic import BaseModel


class CrossReferenceSchemaRelated(BaseModel):
    curie: str
    pages: Optional[List[str]] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class CrossReferenceSchemaShow(CrossReferenceSchemaRelated):
    url: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class CrossReferencePageSchemaShow(BaseModel):
    name: str
    url: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


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
