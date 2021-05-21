from typing import List, Optional, Any
from datetime import datetime

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator


class User(BaseModel):
    name:str
    email:str
    password:str


class ShowUser(BaseModel):
    name:str
    email:str
    resources : List[ResourceSchemaShow] =[]

    class Config():
        orm_mode = True
