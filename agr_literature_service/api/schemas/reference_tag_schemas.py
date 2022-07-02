from typing import Optional

from pydantic import BaseModel


class ReferenceTagSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    tag_type: Optional[str] = None
    value: Optional[str] = None
    mod_abbreviation: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
