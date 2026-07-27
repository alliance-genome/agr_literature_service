from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


def _not_blank(v):
    if v is not None and v.strip() == "":
        raise ValueError("must not be blank")
    return v


class VocabularyTermAbcSchemaPost(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    vocabulary_abc_id: int
    name: str
    is_obsolete: bool = False

    @field_validator("name")
    @classmethod
    def _not_blank_name(cls, v):
        if v is not None and v.strip() == "":
            raise ValueError("must not be blank")
        return v


class VocabularyTermAbcSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    vocabulary_abc_id: Optional[int] = None
    name: Optional[str] = None
    is_obsolete: Optional[bool] = None

    @field_validator("name")
    @classmethod
    def _not_blank_name(cls, v):
        if v is not None and v.strip() == "":
            raise ValueError("must not be blank")
        return v


class VocabularyTermAbcSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    vocabulary_term_abc_id: int
    vocabulary_abc_id: int
    name: str
    is_obsolete: bool
