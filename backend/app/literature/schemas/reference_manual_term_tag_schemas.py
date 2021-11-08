from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas import BaseModelShow


class ReferenceManualTermTagSchemaPost(BaseModel):
    reference_curie: str
    ontology: str
    datatype: str
    term: str

    @validator('reference_curie')
    def must_be_alliance_reference_curie(cls, v):
        if not v.startswith("AGR:AGR-Reference-"):
            raise ValueError('must start with AGR:AGR-Reference-<number>')
        return v

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceManualTermTagSchemaShow(ReferenceManualTermTagSchemaPost):
    reference_automated_term_tag_id: int

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceManualTermTagSchemaPatch(BaseModel):
    reference_curie: str
    ontology: str
    datatype: str
    term: str

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

    class Config():
        orm_mode = True
        extra = "forbid"
