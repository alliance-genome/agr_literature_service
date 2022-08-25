from typing import Optional

from pydantic import BaseModel


class AuditedObjectModelSchema(BaseModel):
    date_created: Optional[str] = None
    date_updated: Optional[str] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
