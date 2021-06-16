from typing import List, Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas import BaseModelShow
from literature.schemas import CrossReferenceSchemaShow


class EditorSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str]  = None
    first_name: Optional[str] = None
    middle_names: Optional[List[str]] = None
    last_name: Optional[str] = None
    orcid: Optional[str] = None

    @validator('orcid')
    def check_orchids(cls, v):
        if not v.startswith('ORCID:'):
            raise ValueError('Orcid ID must start with "ORCID: {v}')
        return v

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
    orcid: Optional[CrossReferenceSchemaShow] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class EditorSchemaCreate(EditorSchemaPost):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
