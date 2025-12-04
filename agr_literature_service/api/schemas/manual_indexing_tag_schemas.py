from typing import Optional, Literal
from datetime import datetime
from pydantic import BaseModel, ConfigDict, field_validator, confloat

from agr_literature_service.api.schemas import AuditedObjectModelSchema

ValidationByBiocurator = Literal["right", "wrong"]


class ConfidenceMixin(BaseModel):
    """Mixin that adds a rounded confidence_score field (0.0–1.0)."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        from_attributes=True,
    )

    confidence_score: Optional[confloat(ge=0.0, le=1.0)] = None  # type: ignore

    @field_validator("confidence_score")
    def _round_confidence_score(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return None
        return round(v, 2)


class ManualIndexingTagSchemaCreate(ConfidenceMixin):
    """
    Create payload (no reference context). Caller provides MOD + tag + optional note/validation.
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    mod_abbreviation: str
    curation_tag: str
    note: Optional[str] = None
    validation_by_biocurator: Optional[ValidationByBiocurator] = None

    @field_validator("curation_tag")
    def _check_atp_prefix(cls, v: str) -> str:
        if not v.startswith("ATP:"):
            raise ValueError("curation_tag must start with 'ATP:'")
        return v


class ManualIndexingTagSchemaPost(ManualIndexingTagSchemaCreate):
    """
    Create (POST) with reference context
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    reference_curie: str


class ManualIndexingTagSchemaRelated(ConfidenceMixin, AuditedObjectModelSchema):
    """
    Read-related (includes audit fields). Suitable for list/detail responses.
    """
    model_config = ConfigDict(extra="ignore", from_attributes=True)

    manual_indexing_tag_id: Optional[int] = None
    reference_curie: Optional[str] = None
    mod_abbreviation: Optional[str] = None

    curation_tag: Optional[str] = None
    note: Optional[str] = None
    validation_by_biocurator: Optional[ValidationByBiocurator] = None

    updated_by_email: Optional[str] = None
    updated_by_name: Optional[str] = None
    date_updated: Optional[datetime] = None  # for output convenience

    @field_validator("curation_tag")
    def _check_atp_prefix_optional(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.startswith("ATP:"):
            raise ValueError("curation_tag must start with 'ATP:'")
        return v


class ManualIndexingTagSchemaShow(ManualIndexingTagSchemaRelated):
    """Alias for detail view; keeps the same fields as Related."""
    pass


class ManualIndexingTagSchemaUpdate(ConfidenceMixin):
    """
    update an existing row
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    reference_curie: Optional[str] = None
    mod_abbreviation: Optional[str] = None
    curation_tag: Optional[str] = None
    note: Optional[str] = None
    validation_by_biocurator: Optional[ValidationByBiocurator] = None

    @field_validator("curation_tag")
    def _check_atp_prefix_optional(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.startswith("ATP:"):
            raise ValueError("curation_tag must start with 'ATP:'")
        return v
