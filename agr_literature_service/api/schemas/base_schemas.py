from typing import Optional

from pydantic import BaseModel


class BaseModelShow(BaseModel):
    date_created: str
    date_updated: Optional[str] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
