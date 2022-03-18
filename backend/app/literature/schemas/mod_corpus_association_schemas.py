from typing import Optional

from pydantic import BaseModel


class ModCorpusAssociationSchemaCreate(BaseModel):
    mod_id: int
    mod_corpus_sort_source: Optional[str] = None
    corpus: bool

    class Config():
        orm_mode = True
        extra = "forbid"


class ModCorpusAssociationSchemaPost(ModCorpusAssociationSchemaCreate):
    reference_curie: str

    class Config():
        orm_mode = True
        extra = "forbid"


class ModCorpusAssociationSchemaShow(ModCorpusAssociationSchemaPost):
    mod_corpus_association_id: int

    class Config():
        orm_mode = True
        extra = "forbid"


class ModCorpusAssociationSchemaRelated(BaseModel):
    mod_corpus_association_id: int
    date_created: str
    mod_corpus_sort_source: str
    mod_id: int
    date_updated: str
    corpus: bool

    class Config():
        orm_mode = True
        extra = "forbid"


class ModCorpusAssociationSchemaUpdate(BaseModel):
    reference_curie: Optional[str]
    mod_corpus_sort_source: Optional[str]
    mod_id: Optional[int] = None
    corpus: Optional[bool] = None

    class Config():
        orm_mode = True
        extra = "forbid"