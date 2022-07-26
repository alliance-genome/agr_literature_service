from typing import Optional
from pydantic import BaseModel
from agr_literature_service.api.schemas import BaseModelShow


class ReferenceOntologySchemaCreate(BaseModel):
    reference_curie: str
    ontology_id: str
    mod_abbreviation: str
    created_by: str


class ReferenceOntologySchemaShow(BaseModelShow):
    reference_ontology_id: int
    mod_abbreviation: str
    reference_curie: str
    ontology_id: str
    created_by: str


class ReferenceOntologySchemaRelated(BaseModel):
    reference_ontology_id: Optional[int]
    date_created: Optional[str]
    ontology_id: str
    mod_abbreviation: str
    date_updated: Optional[str]

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceOntologySchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    mod_abbreviation: Optional[str] = None
    ontology_id: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
