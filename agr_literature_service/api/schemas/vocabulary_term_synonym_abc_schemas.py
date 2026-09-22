from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class VocabularyTermSynonymAbcSchemaPost(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    vocabulary_term_abc_id: int
    synonym_name: str

    @field_validator("synonym_name")
    @classmethod
    def _not_blank_synonym_name(cls, v):
        if v is not None and v.strip() == "":
            raise ValueError("must not be blank")
        return v


class VocabularyTermSynonymAbcSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    vocabulary_term_abc_id: Optional[int] = None
    synonym_name: Optional[str] = None

    @field_validator("synonym_name")
    @classmethod
    def _not_blank_synonym_name(cls, v):
        if v is not None and v.strip() == "":
            raise ValueError("must not be blank")
        return v


class VocabularyTermSynonymAbcSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    vocabulary_term_synonym_abc_id: int
    vocabulary_term_abc_id: int
    synonym_name: str
