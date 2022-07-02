from typing import Optional

from pydantic import BaseModel


class ReferenceTagSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    tag_type: str = None
    value: str = None
    mod_abbreviation: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
