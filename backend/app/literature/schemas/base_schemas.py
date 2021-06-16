from typing import Optional
from datetime import datetime

from pydantic import BaseModel

class BaseModelShow(BaseModel):
    date_created: Optional[datetime] = None
    date_updated: Optional[datetime] = None
