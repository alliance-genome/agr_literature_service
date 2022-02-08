from typing import Optional

from pydantic import BaseModel, validator


class ReferenceAutomatedTermTagSchemaPost(BaseModel):
    reference_curie: str = None
    ontology: str = None
    datatype: str = None
    term: str = None
    automated_system: str = None
    confidence_score: Optional[float] = None

    @validator('reference_curie')
    def must_be_alliance_reference_curie(cls, v):
        if not v.startswith("AGR:AGR-Reference-"):
            raise ValueError('must start with AGR:AGR-Reference-<number>')
        return v

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceAutomatedTermTagSchemaShow(ReferenceAutomatedTermTagSchemaPost):
    reference_automated_term_tag_id: int

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceAutomatedTermTagSchemaPatch(BaseModel):
    reference_curie: Optional[str] = None
    ontology: Optional[str] = None
    datatype: Optional[str] = None
    term: Optional[str] = None
    automated_system: Optional[str] = None
    confidence_score: Optional[float] = None

    @validator('reference_curie')
    def reference_is_some(cls, v):
        if not v.startswith("AGR:AGR-Reference-"):
            raise ValueError('must start with AGR:AGR-Reference-<number>')

        return v

    @validator('ontology')
    def ontology_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set ontology to None')
        return v

    @validator('datatype')
    def datatype_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set datatype to None')
        return v

    @validator('term')
    def term_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set term to None')
        return v

    @validator('automated_system')
    def automated_system_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set automated_system to None')
        return v

    class Config():
        orm_mode = True
        extra = "forbid"
