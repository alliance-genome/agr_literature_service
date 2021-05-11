from typing import List, Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator


class AuthorSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str]  = None
    firstName: Optional[str] = None
    middleNames: Optional[List[str]] = None
    lastName: Optional[str] = None

    orcid: Optional[str] = None
    affiliation: Optional[List[str]] = None

    correspondingAuthor: Optional[bool] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class AuthorSchemaShow(AuthorSchemaPost):
    author_id: int

    class Config():
        orm_mode = True
        extra = "forbid"

class AuthorSchemaCreate(AuthorSchemaPost):
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
