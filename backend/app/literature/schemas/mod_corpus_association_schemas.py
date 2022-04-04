from typing import Optional
# from typing import List

from pydantic import BaseModel
from literature.schemas import ModCorpusSortSourceType, BaseModelShow


class ModCorpusAssociationSchemaCreate(BaseModel):
    mod_abbreviation: str
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    corpus: bool

    class Config():
        orm_mode = True
        extra = "forbid"


class ModCorpusAssociationSchemaShowID(BaseModel):
    mod_abbreviation: str
    reference_curie: str

    class Config():
        orm_mode = True
        extra = "forbid"


class ModCorpusAssociationSchemaPost(ModCorpusAssociationSchemaCreate):
    reference_curie: str

    class Config():
        orm_mode = True
        extra = "forbid"


class ModCorpusAssociationSchemaShow(BaseModelShow):
    mod_corpus_association_id: int
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    mod_abbreviation: Optional[str] = None
    reference_curie: Optional[str] = None
    corpus: Optional[bool] = None


class ModCorpusAssociationSchemaRelated(BaseModel):
    mod_corpus_association_id: int
    date_created: str
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    mod_abbreviation: str
    date_updated: str
    corpus: bool

    class Config():
        orm_mode = True
        extra = "forbid"


class ModCorpusAssociationSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    mod_abbreviation: Optional[str] = None
    corpus: Optional[bool] = None

    class Config():
        orm_mode = True
        extra = "forbid"
