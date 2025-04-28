from typing import Optional

from pydantic import ConfigDict, BaseModel


class ModReferenceTypeSchemaCreate(BaseModel):
    reference_type: str
    mod_abbreviation: Optional[str] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ModReferenceTypeSchemaPost(ModReferenceTypeSchemaCreate):
    reference_curie: str
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ModReferenceTypeSchemaShow(ModReferenceTypeSchemaPost):
    mod_reference_type_id: int
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ModReferenceTypeSchemaRelated(ModReferenceTypeSchemaCreate):
    mod_reference_type_id: int
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ModReferenceTypeSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    reference_type: Optional[str] = None
    mod_abbreviation: Optional[str] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")
