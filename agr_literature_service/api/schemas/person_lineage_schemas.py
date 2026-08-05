from __future__ import annotations
from datetime import datetime
from typing import Any, Optional, Union, TYPE_CHECKING
from pydantic import BaseModel, ConfigDict, model_validator

from .base_schemas import AuditedObjectModelSchema

if TYPE_CHECKING:
    # Imported for typing only. A runtime import here would create a schemas<->crud
    # import cycle (person_schemas -> ... -> vocabulary_crud -> person_schemas). The
    # relationship forward reference is resolved at runtime by person_lineage_crud,
    # which calls model_rebuild() with VocabularyTermRefSchema in scope.
    from agr_literature_service.api.crud.vocabulary_crud import VocabularyTermRefSchema


class PersonLineageSchemaCreate(BaseModel):
    """Create payload for a validated (canonical) person-to-person relationship.

    Both people are required and given by curie OR integer id (resolved server-side).
    The relationship is the vocabulary_term_abc id of a "person_person_relationship"
    term (validated server-side).
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_subject_curie_or_id: Union[str, int]
    person_object_curie_or_id: Union[str, int]
    relationship: int
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class PersonLineageSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    # Curators may correct a mis-resolved person on the canonical (by curie OR id).
    # The submission's name claim is unchanged; this only fixes which person the
    # name was resolved to, and the submission link is preserved.
    person_subject_curie_or_id: Optional[Union[str, int]] = None
    person_object_curie_or_id: Optional[Union[str, int]] = None
    relationship: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    @model_validator(mode="before")
    @classmethod
    def _reject_null_relationship(cls, data: Any) -> Any:
        # relationship is a NOT NULL FK — reject an explicit null (omit the field to
        # leave it unchanged), matching PersonLineageSubmissionSchemaUpdate's fail-loud
        # behavior rather than silently ignoring it.
        if isinstance(data, dict) and "relationship" in data and data["relationship"] is None:
            raise ValueError("relationship cannot be null; omit the field to leave it unchanged")
        return data


class PersonLineageSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_lineage_id: int
    person_subject_id: int
    person_subject_curie: Optional[str] = None
    person_subject_name: Optional[str] = None
    person_object_id: int
    person_object_curie: Optional[str] = None
    person_object_name: Optional[str] = None
    relationship: VocabularyTermRefSchema
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
