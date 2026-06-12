from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator

from .base_schemas import AuditedObjectModelSchema
from .validation_utils import validate_non_empty


class LaboratoryAlleleDesignationSchemaCreate(BaseModel):
    """Create payload for a laboratory allele designation.

    The MOD is identified by its abbreviation (e.g. ``SGD``); the CRUD layer
    resolves it to the numeric ``mod_id``.
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    mod_abbreviation: str
    allele_designation: str

    @field_validator("mod_abbreviation")
    @classmethod
    def _validate_mod_abbreviation(cls, v: str) -> str:
        return validate_non_empty(v, "mod_abbreviation")

    @field_validator("allele_designation")
    @classmethod
    def _validate_allele_designation(cls, v: str) -> str:
        return validate_non_empty(v, "allele_designation")


class LaboratoryAlleleDesignationSchemaPost(LaboratoryAlleleDesignationSchemaCreate):
    """Standalone create payload — names the owning laboratory by curie (or id) in the body."""
    laboratory_curie: str


class LaboratoryAlleleDesignationSchemaUpdate(BaseModel):
    """Partial update payload for a laboratory allele designation."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    mod_abbreviation: Optional[str] = None
    allele_designation: Optional[str] = None

    @field_validator("mod_abbreviation")
    @classmethod
    def _validate_mod_abbreviation(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return validate_non_empty(v, "mod_abbreviation")

    @field_validator("allele_designation")
    @classmethod
    def _validate_allele_designation(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return validate_non_empty(v, "allele_designation")


class LaboratoryAlleleDesignationSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    laboratory_allele_designation_id: int
    laboratory_id: int
    mod_id: int
    mod_abbreviation: Optional[str] = None
    allele_designation: str


class LaboratoryAlleleDesignationSchemaRelated(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    laboratory_allele_designation_id: int
    mod_id: int
    mod_abbreviation: Optional[str] = None
    allele_designation: str
