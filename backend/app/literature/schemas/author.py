from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas.base import BaseModelShow


class AuthorSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str]  = None
    first_name: Optional[str] = None
    middle_names: Optional[List[str]] = None
    last_name: Optional[str] = None

    primary: Optional[bool] = False
    orcid: Optional[str] = None
    affiliation: Optional[List[str]] = None

    corresponding_author: Optional[bool] = None

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

    primary: Optional[bool]
    orcid: Optional[str] = None
    affiliation: Optional[List[str]] = None

    corresponding_author: Optional[bool] = None

    class Config():
        orm_mode = True
        extra = "forbid"

class AuthorSchemaCreate(AuthorSchemaPost):
    author_id: int
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class AuthorSchemaUpdate(AuthorSchemaShow):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
