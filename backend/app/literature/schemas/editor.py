from typing import List, Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator


class EditorSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str]  = None
    firstName: Optional[str] = None
    middleNames: Optional[List[str]] = None
    lastName: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class EditorSchemaShow(EditorSchemaPost):
    editor_id: int

    class Config():
        orm_mode = True
        extra = "forbid"

class EditorSchemaCreate(EditorSchemaPost):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class EditorSchemaUpdate(EditorSchemaShow):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
