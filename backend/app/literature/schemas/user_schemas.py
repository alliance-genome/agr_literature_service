from typing import List

from pydantic import BaseModel
from literature.schemas import ResourceSchemaShow


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
