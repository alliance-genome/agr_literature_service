from typing import List, Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas.base import BaseModelShow


class EditorSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str]  = None
    first_name: Optional[str] = None
    middle_names: Optional[List[str]] = None
    last_name: Optional[str] = None
    orcid: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class EditorSchemaShow(BaseModelShow):
    editor_id: int
    order: Optional[int] = None

    name: Optional[str]  = None
    first_name: Optional[str] = None
    middle_names: Optional[List[str]] = None
    last_name: Optional[str] = None
    orcid: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"

class EditorSchemaCreate(BaseModel):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    order: Optional[int] = None

    name: Optional[str]  = None
    first_name: Optional[str] = None
    middle_names: Optional[List[str]] = None
    last_name: Optional[str] = None
    orcid: Optional[str] = None


    class Config():
        orm_mode = True
        extra = "forbid"


class EditorSchemaUpdate(EditorSchemaCreate):
    editor_id: int

    class Config():
        orm_mode = True
