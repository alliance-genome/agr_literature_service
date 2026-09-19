"""
curation_status_source_schemas.py
=================================

Schemas for curation status source attribution (SCRUM-6518).
"""
from typing import Optional

from pydantic import BaseModel, ConfigDict

from agr_literature_service.api.schemas.base_schemas import AuditedObjectModelSchema


class CurationStatusSourceAssociationSchemaPost(BaseModel):
    """Attach one source's reported values to an EXISTING curation_status row."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    curation_status_id: int
    tag_source_id: int
    curation_status: Optional[str] = None
    curation_tag: Optional[str] = None
    note: Optional[str] = None


class CurationStatusSourceAssociationSchemaUpdate(BaseModel):
    """Update what one source reported. Never touches the curation_status row."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    curation_status: Optional[str] = None
    curation_tag: Optional[str] = None
    note: Optional[str] = None


class CurationStatusSourceAssociationSchemaShow(AuditedObjectModelSchema):
    """One source's report, with the source's identifying fields flattened in so
    a caller does not need a second round trip to /tag_source."""
    model_config = ConfigDict(extra='ignore', from_attributes=True)

    curation_status_source_association_id: int
    curation_status_id: int
    tag_source_id: int
    curation_status: Optional[str] = None
    curation_tag: Optional[str] = None
    note: Optional[str] = None
    source_evidence_assertion: Optional[str] = None
    source_method: Optional[str] = None
    data_provider: Optional[str] = None
    # The source's OWNING mod. data_provider is not it - see
    # test_show_all_reference_tags_batch_mod_filter - so a consumer grouping
    # attributions per MOD needs this one. Declared explicitly because
    # extra='ignore' means an undeclared key is dropped from the response.
    secondary_data_provider_abbreviation: Optional[str] = None
    validation_type: Optional[str] = None
