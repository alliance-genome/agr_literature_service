from typing import Optional

from pydantic import ConfigDict, BaseModel, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class EditorSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    orcid: Optional[str] = None
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    @field_validator('orcid')
    def check_orcids(cls, v):
        if v and not v.startswith('ORCID:'):
            raise ValueError('Orcid ID must start with "ORCID: {v}')
        return v
    model_config = ConfigDict(from_attributes=True, extra="forbid", json_schema_extra={
        "example": {
            "order": 1,
            "name": "string",
            "first_name": "string",
            "last_name": "string",
            "orcid": "ORCID:string"
        }
    })


class EditorSchemaShow(AuditedObjectModelSchema):
    editor_id: int
    order: Optional[int] = None

    name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    orcid: Optional[str] = None


class EditorSchemaCreate(EditorSchemaPost):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")
