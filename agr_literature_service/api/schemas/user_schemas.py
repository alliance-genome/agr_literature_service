from typing import List

from pydantic import BaseModel

from agr_literature_service.api.schemas import ResourceSchemaShow


class UserSchema(BaseModel):
    name: str
    email: str
    password: str


class ShowUserSchema(BaseModel):
    name: str
    email: str
    resources : List[ResourceSchemaShow] = []

    class Config():
        orm_mode = True
