from typing import Optional

from pydantic import ConfigDict, BaseModel


class MeshDetailSchemaCreate(BaseModel):
    heading_term: str
    qualifier_term: Optional[str] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class MeshDetailSchemaPost(MeshDetailSchemaCreate):
    reference_curie: str
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class MeshDetailSchemaShow(MeshDetailSchemaPost):
    mesh_detail_id: int
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class MeshDetailSchemaRelated(MeshDetailSchemaCreate):
    mesh_detail_id: int
    model_config = ConfigDict(from_attributes=True, exptra="forbid")


class MeshDetailSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    heading_term: Optional[str] = None
    qualifier_term: Optional[str] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")
