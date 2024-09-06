from typing import Optional, List

from pydantic import BaseModel

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
    is_annotation: Optional[bool]
    mod_abbreviation: Optional[str] = None

    class Config:
        orm_mode = True
        extra = "forbid"


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
    reference_curie: Optional[str]
    display_name: Optional[str]
    file_class: Optional[str]
    file_publication_status: Optional[str]
    file_extension: Optional[str]
    pdf_type: Optional[str]
    is_annotation: Optional[bool]

    class Config:
        extra = "forbid"


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

    class Config:
        orm_mode = True
        extra = "forbid"


class ReferenceFileAllMainPDFIdsSchemaPost(BaseModel):
    curies: List[str]
    mod_abbreviation: str
