from typing import List

from pydantic import ConfigDict, BaseModel

from agr_literature_service.api.schemas import ResourceSchemaShow


class UserSchema(BaseModel):
    name: str
    email: str
    password: str


class ShowUserSchema(BaseModel):
    name: str
    email: str
    resources : List[ResourceSchemaShow] = []
    model_config = ConfigDict(from_attributes=True)
