from typing import List, Optional, Any
from datetime import datetime

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas.resource_schemas import ResourceSchemaShow


class UserSchema(BaseModel):
    name:str
    email:str
    password:str


class ShowUserSchema(BaseModel):
    name:str
    email:str
    resources : List[ResourceSchemaShow] =[]

    class Config():
        orm_mode = True
