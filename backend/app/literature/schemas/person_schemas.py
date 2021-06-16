from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas import BaseModelShow
from literature.schemas import CrossReferenceSchemaShow


class PersonSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str]  = None
    first_name: Optional[str] = None
    middle_names: Optional[List[str]] = None
    last_name: Optional[str] = None

    first_person: Optional[bool] = False
    affiliation: Optional[List[str]] = None
    corresponding_person: Optional[bool] = None

    orcids: Optional[List[str]] = None

    @validator('orcids', each_item=True)
    def check_orchids(cls, v):
        if not v.startswith('ORCID:'):
            raise ValueError('Orcid ID must start with "ORCID: {v}')
        return v

    class Config():
        orm_mode = True
        extra = "forbid"


class PersonSchemaShow(BaseModelShow):
    person_id: int

    order: Optional[int] = None

    name: Optional[str]  = None
    first_name: Optional[str] = None
    middle_names: Optional[List[str]] = None
    last_name: Optional[str] = None

    first_person: Optional[bool]
    orcids: Optional[List[CrossReferenceSchemaShow]] = None
    affiliation: Optional[List[str]] = None

    class Config():
        orm_mode = True
        extra = "forbid"

class PersonSchemaCreate(PersonSchemaPost):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"

