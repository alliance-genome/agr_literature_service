from typing import Optional
from enum import Enum
from pydantic import BaseModel

class messageEnum(str, Enum):
    updated = "updated"

class ResponseMessageSchema(BaseModel):
    message: messageEnum

    details: Optional[str] = None
