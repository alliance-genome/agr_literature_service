from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from agr_literature_service.api.schemas import AuditedObjectModelSchema
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaRelated


class ReferencefileSchemaPost(BaseModel):
    """Schema for posting a new reference file."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: str
    display_name: str
    file_class: str
    file_publication_status: str
    file_extension: str
    pdf_type: Optional[str] = None
    md5sum: str
    is_annotation: Optional[bool] = None
    mod_abbreviation: Optional[str] = None


class ReferencefileSchemaShow(AuditedObjectModelSchema):
    """Schema for showing a reference file with audit fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    referencefile_id: int
    reference_curie: str
    display_name: str
    file_class: str
    file_publication_status: str
    file_extension: str
    pdf_type: Optional[str] = None
    md5sum: str
    is_annotation: bool
    referencefile_mods: Optional[List[ReferencefileModSchemaRelated]] = None


class ReferencefileSchemaUpdate(BaseModel):
    """Schema for updating a reference file."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: Optional[str] = None
    display_name: Optional[str] = None
    file_class: Optional[str] = None
    file_publication_status: Optional[str] = None
    file_extension: Optional[str] = None
    pdf_type: Optional[str] = None
    is_annotation: Optional[bool] = None
    change_if_already_converted: Optional[bool] = None


class ReferencefileSchemaRelated(AuditedObjectModelSchema):
    """Schema for related reference file entries with audit fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    referencefile_id: int
    display_name: str
    file_class: str
    file_publication_status: str
    file_extension: str
    pdf_type: Optional[str] = None
    md5sum: str
    is_annotation: bool
    referencefile_mods: Optional[List[ReferencefileModSchemaRelated]] = None


class ReferenceFileAllMainPDFIdsSchemaPost(BaseModel):
    """Schema for bulk main PDF IDs retrieval."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    curies: List[str]
    mod_abbreviation: str
