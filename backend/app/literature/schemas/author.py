from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas.base import BaseModelShow
from literature.schemas.cross_reference import CrossReferenceSchemaShow


class AuthorSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str]  = None
    first_name: Optional[str] = None
    middle_names: Optional[List[str]] = None
    last_name: Optional[str] = None

    first_author: Optional[bool] = False
    affiliation: Optional[List[str]] = None
    corresponding_author: Optional[bool] = None

    orcid: Optional[str] = None

    @validator('orcid')
    def check_orchids(cls, v):
        if not v.startswith('ORCID:'):
            raise ValueError('Orcid ID must start with "ORCID: {v}')
        return v

    class Config():
        orm_mode = True
        extra = "forbid"


class AuthorSchemaShow(BaseModelShow):
    author_id: int

    order: Optional[int] = None

    name: Optional[str]  = None
    first_name: Optional[str] = None
    middle_names: Optional[List[str]] = None
    last_name: Optional[str] = None

    first_author: Optional[bool]
    orcid: Optional[CrossReferenceSchemaShow] = None
    affiliation: Optional[List[str]] = None

    corresponding_author: Optional[bool] = None

    class Config():
        orm_mode = True
        extra = "forbid"

class AuthorSchemaCreate(AuthorSchemaPost):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"

