from typing import List, Optional

from pydantic import BaseModel


class CrossReferenceSchemaRelated(BaseModel):
    curie: str
    pages: Optional[List[str]] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class CrossReferenceSchema(CrossReferenceSchemaRelated):
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
