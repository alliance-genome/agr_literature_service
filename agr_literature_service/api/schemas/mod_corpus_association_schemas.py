from typing import Optional

from pydantic import ConfigDict, BaseModel
from agr_literature_service.api.schemas import ModCorpusSortSourceType, AuditedObjectModelSchema


class ModCorpusAssociationSchemaCreate(BaseModel):
    mod_abbreviation: str
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    corpus: Optional[bool] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ModCorpusAssociationSchemaShowID(BaseModel):
    mod_abbreviation: str
    reference_curie: str
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ModCorpusAssociationSchemaPost(ModCorpusAssociationSchemaCreate):
    reference_curie: str
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ModCorpusAssociationSchemaShow(AuditedObjectModelSchema):
    mod_corpus_association_id: int
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    mod_abbreviation: str
    reference_curie: str
    corpus: Optional[bool] = None


class ModCorpusAssociationSchemaRelated(AuditedObjectModelSchema):
    mod_corpus_association_id: int
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    mod_abbreviation: str
    corpus: Optional[bool] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ModCorpusAssociationSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    corpus: Optional[bool] = None
    index_wft_id: Optional[str] = None
    force_out: Optional[bool] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")
