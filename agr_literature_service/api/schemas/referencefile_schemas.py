from typing import List, Optional

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


class ReferencefileSourceSchema(BaseModel):
    """The referencefile a derived file was produced from (upward lineage).

    md/figure -> their source PDF (display-name-suffix convention);
    embedding -> its converted_merged_* markdown (embedding_file FK).
    """
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    referencefile_id: int
    display_name: str
    file_class: str
    file_extension: str
    md5sum: str


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
    # Upward lineage: the referencefile this one was derived from (nullable
    # when unresolved). Added to every derived file by show_all.
    source: Optional[ReferencefileSourceSchema] = None
    # Embedding-only fields (populated from embedding_file when this row is an
    # `embedding` parquet; show_all always lists these rows).
    profile_name: Optional[str] = None
    version: Optional[int] = None
    model_name: Optional[str] = None


class ReferenceFileAllMainPDFIdsSchemaPost(BaseModel):
    """Schema for bulk main PDF IDs retrieval."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    curies: List[str]
    mod_abbreviation: str


class ReferencefileConvertedDerivedSchema(BaseModel):
    """A converted Markdown referencefile derived from a given source PDF/nXML."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    referencefile_id: int
    display_name: str
    file_class: str
    file_extension: str


class ReferencefileByMd5MatchSchema(AuditedObjectModelSchema):
    """A referencefile matched by MD5 plus context needed by PDF-only callers.

    Returned by ``GET /reference/referencefile/by_md5/{md5sum}``. For source
    PDFs (file_class ``main``/``supplement``) or nXML inputs (file_class
    ``nXML``), ``converted_referencefiles`` lists any converted Markdown
    rows currently in the DB that were produced from this same source.
    """
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    referencefile_id: int
    reference_id: int
    reference_curie: str
    display_name: str
    file_class: str
    file_publication_status: str
    file_extension: str
    pdf_type: Optional[str] = None
    md5sum: str
    is_annotation: bool
    open_access: bool
    copyright_license_name: Optional[str] = None
    referencefile_mods: List[ReferencefileModSchemaRelated]
    converted_referencefiles: List[ReferencefileConvertedDerivedSchema]
