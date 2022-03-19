from typing import Optional

from pydantic import BaseModel


class ModCorpusAssociationSchemaCreate(BaseModel):
    mod_abbreviation: str
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
    date_created: str
    date_updated: str
    mod_corpus_association_id: int

    class Config():
        orm_mode = True
        extra = "forbid"


class ModCorpusAssociationSchemaRelated(BaseModel):
    mod_corpus_association_id: int
    date_created: str
    mod_corpus_sort_source: str
    mod_abbreviation: str
    date_updated: str
    corpus: bool

    class Config():
        orm_mode = True
        extra = "forbid"


class ModCorpusAssociationSchemaUpdate(BaseModel):
    reference_curie: Optional[str]
    mod_corpus_sort_source: Optional[str]
    mod_abbreviation: Optional[str]
    corpus: Optional[bool]

    class Config():
        orm_mode = True
        extra = "forbid"
