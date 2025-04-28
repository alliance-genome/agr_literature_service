from typing import List, Optional

from pydantic import BaseModel, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class AuthorSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    first_initial: Optional[str] = None
    first_author: Optional[bool] = False
    affiliations: Optional[List[str]] = None
    corresponding_author: Optional[bool] = False
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None
    orcid: Optional[str] = None

    @field_validator('orcid')
    def check_orcids(cls, v):
        if v and not v.startswith('ORCID:'):
            raise ValueError('Orcid ID must start with "ORCID: {v}')
        return v

    class Config():
        orm_mode = True
        extra = "forbid"
        schema_extra = {
            "example": {
                "order": 1,
                "name": "string",
                "first_name": "string",
                "last_name": "string",
                "first_initial": "string",
                "affiliations": [
                    "string"
                ],
                "orcid": "ORCID:string"
            }
        }


class AuthorSchemaShow(AuditedObjectModelSchema):
    author_id: int

    order: Optional[int] = None

    name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    first_initial: Optional[str] = None
    first_author: Optional[bool]
    orcid: Optional[str] = None
    affiliations: Optional[List[str]] = None

    corresponding_author: Optional[bool] = None


# NOTE: Why Create and Post????
class AuthorSchemaCreate(AuthorSchemaPost):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
