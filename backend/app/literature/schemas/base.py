from typing import Optional
from datetime import datetime

from pydantic import BaseModel

class BaseModelShow(BaseModel):
    dateCreated: Optional[datetime] = None
    dateUpdated: Optional[datetime] = None
