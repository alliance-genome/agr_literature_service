from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator


class NoteSchemaPost(BaseModel):
    name: str
    note: str

    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class NoteSchemaShow(NoteSchemaPost):
    note_id: int
    date_created: str


class NoteSchemaUpdate(BaseModel):
    name: Optional[str] = None
    note: Optional[str] = None

    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
