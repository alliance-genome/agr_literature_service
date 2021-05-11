from typing import List, Optional

from pydantic import BaseModel


class CrossReferenceRelated(BaseModel):
    curie: str
    pages: Optional[List[str]] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class CrossReference(CrossReferenceRelated):
    resource_id:  Optional[int] = None
    reference_id: Optional[int] = None

    class Config():
        orm_mode = True
        extra = "forbid"

