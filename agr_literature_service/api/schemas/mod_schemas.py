from typing import List, Optional
from pydantic import ConfigDict, BaseModel
from agr_literature_service.api.schemas import AuditedObjectModelSchema


class ModSchemaPost(BaseModel):
    abbreviation: str
    short_name: str
    full_name: str
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ModSchemaShow(AuditedObjectModelSchema):
    mod_id: int
    abbreviation: str
    short_name: str
    full_name: str
    taxon_ids: Optional[List[str]] = None


class ModSchemaUpdate(BaseModel):
    abbreviation: Optional[str] = None
    short_name: Optional[str] = None
    full_name: Optional[str] = None
    taxon_ids: Optional[List[str]] = None


class ModSchemaCreate(ModSchemaPost):
    model_config = ConfigDict(from_attributes=True, extra="forbid")
