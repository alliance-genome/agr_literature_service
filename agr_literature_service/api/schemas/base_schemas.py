from typing import Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict


class AuditedObjectModelSchema(BaseModel):
    model_config = ConfigDict(extra="ignore", from_attributes=True)

    date_created: Optional[datetime] = None
    date_updated: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
