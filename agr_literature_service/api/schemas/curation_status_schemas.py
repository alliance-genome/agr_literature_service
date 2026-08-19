from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class CurationStatusSchemaBase(AuditedObjectModelSchema):
    """Base schema for curation status with audit fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    mod_abbreviation: str
    reference_curie: str


class CurationStatusSchemaPost(CurationStatusSchemaBase):
    """Schema for posting a new curation status."""
    topic: str
    curation_status: Optional[str] = None
    curation_tag: Optional[str] = None
    note: Optional[str] = None


class CurationStatusSchemaUpdate(BaseModel):
    """Schema for updating an existing curation status."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    curation_status: Optional[str] = None
    curation_tag: Optional[str] = None
    note: Optional[str] = None


class CurationStatusSchemaShow(AuditedObjectModelSchema):
    """Schema for returning a curation status (full object) on create/update."""
    model_config = ConfigDict(
        extra='ignore',
        from_attributes=True,
    )

    curation_status_id: int
    mod_id: Optional[int] = None
    reference_id: Optional[int] = None
    topic: Optional[str] = None
    curation_status: Optional[str] = None
    curation_tag: Optional[str] = None
    note: Optional[str] = None


class TETSourcePredictionSchema(BaseModel):
    """One computed (non-manual) topic_entity_tag, for curator validation."""
    model_config = ConfigDict(extra='forbid')

    source_method: Optional[str] = None
    source_evidence_assertion: Optional[str] = None
    confidence_score: Optional[float] = None
    confidence_level: Optional[str] = None
    negated: bool = False
    assessment: Optional[str] = None
    # The data-novelty ATP term (e.g. new-to-DB / new-to-field), so the quick
    # add UI can resolve the specific novelty bucket, not just has/new/no.
    data_novelty: Optional[str] = None
    # The extracted entity (e.g. a gene curie) and its type, so consolidated
    # predictions can be broken down per entity on hover.
    entity: Optional[str] = None
    entity_type: Optional[str] = None


class TETManualAssessmentSchema(BaseModel):
    """One manually recorded topic_entity_tag (author or biocurator), so the UI
    can tell which specific assessment/novelty bucket a curator validated."""
    model_config = ConfigDict(extra='forbid')

    source: Optional[str] = None            # 'author' | 'biocurator'
    negated: bool = False
    assessment: Optional[str] = None        # 'has' | 'new' | 'no'
    data_novelty: Optional[str] = None      # ATP term (new-to-DB / new-to-field / ...)


class TETAssessmentStatesSchema(BaseModel):
    """Per-column state for the quick-add grid: 'validated' (biocurator ✓),
    'unvalidated' (?), or None (blank)."""
    model_config = ConfigDict(extra='forbid')

    has_data: Optional[str] = None
    new_data: Optional[str] = None
    new_to_db: Optional[str] = None
    new_to_field: Optional[str] = None
    no_data: Optional[str] = None


class AggregatedCurationStatusAndTETInfoSchema(BaseModel):
    """Aggregated curation status and TET info, for combined views."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    curst_curation_status_id: Optional[int] = None
    curst_curation_status: Optional[str] = None
    curst_curation_tag: Optional[str] = None
    curst_note: Optional[str] = None
    curst_updated_by: Optional[str] = None
    curst_updated_by_email: Optional[str] = None
    curst_updated_by_name: Optional[str] = None
    curst_date_updated: Optional[str] = None

    topic_curie: str
    topic_name: str

    tet_info_date_created: Optional[str] = None
    tet_info_topic_source: List[str] = Field(default_factory=list)
    tet_info_has_data: bool = False
    tet_info_new_data: bool = False
    tet_info_no_data: bool = False
    # Manual (author/biocurator) assessments already recorded for this topic, so
    # the UI can prevent duplicate curation.
    tet_info_manual_has_data: bool = False
    tet_info_manual_new_data: bool = False
    tet_info_manual_no_data: bool = False
    # Computed predictions with their source method and confidence.
    tet_info_source_predictions: List[TETSourcePredictionSchema] = Field(default_factory=list)
    # Manual (author/biocurator) assessments with their negated flag and novelty,
    # so the quick add UI can render per-bucket validated (biocurator) vs
    # unvalidated (author) state.
    tet_info_manual_assessments: List[TETManualAssessmentSchema] = Field(default_factory=list)
    # Server-computed per-column state for the quick-add grid (validated /
    # unvalidated / blank), applying the "exclude validated_wrong, biocurator
    # wins the row" rules.
    tet_info_assessment_states: TETAssessmentStatesSchema = Field(default_factory=TETAssessmentStatesSchema)
