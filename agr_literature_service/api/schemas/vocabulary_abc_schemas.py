from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


def _not_blank(v):
    if v is not None and v.strip() == "":
        raise ValueError("must not be blank")
    return v


class VocabularyAbcSchemaPost(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    vocabulary: str

    @field_validator("vocabulary")
    @classmethod
    def _not_blank_vocabulary(cls, v):
        return _not_blank(v)


class VocabularyAbcSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    vocabulary: Optional[str] = None

    @field_validator("vocabulary")
    @classmethod
    def _not_blank_vocabulary(cls, v):
        return _not_blank(v)


class VocabularyAbcSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    vocabulary_abc_id: int
    vocabulary: str
