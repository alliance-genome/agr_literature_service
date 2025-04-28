from typing import Optional, List

from pydantic import ConfigDict, BaseModel

from agr_literature_service.api.schemas import AuditedObjectModelSchema
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaRelated


class ReferencefileSchemaPost(BaseModel):
    reference_curie: str
    display_name: str
    file_class: str
    file_publication_status: str
    file_extension: str
    pdf_type: Optional[str] = None
    md5sum: str
    is_annotation: Optional[bool] = None
    mod_abbreviation: Optional[str] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ReferencefileSchemaShow(AuditedObjectModelSchema):
    referencefile_id: int
    reference_curie: str
    display_name: str
    file_class: str
    file_publication_status: str
    file_extension: str
    pdf_type: Optional[str] = None
    md5sum: str
    is_annotation: bool
    referencefile_mods: Optional[List[ReferencefileModSchemaRelated]]


class ReferencefileSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    display_name: Optional[str] = None
    file_class: Optional[str] = None
    file_publication_status: Optional[str] = None
    file_extension: Optional[str] = None
    pdf_type: Optional[str] = None
    is_annotation: Optional[bool] = None
    change_if_already_converted: Optional[bool] = None
    model_config = ConfigDict(extra="forbid")


class ReferencefileSchemaRelated(AuditedObjectModelSchema):
    referencefile_id: int
    display_name: str
    file_class: str
    file_publication_status: str
    file_extension: str
    pdf_type: Optional[str] = None
    md5sum: str
    is_annotation: bool
    referencefile_mods: Optional[List[ReferencefileModSchemaRelated]]
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ReferenceFileAllMainPDFIdsSchemaPost(BaseModel):
    curies: List[str]
    mod_abbreviation: str
