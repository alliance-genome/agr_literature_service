from typing import List, Optional

from pydantic import BaseModel, validator

from literature.schemas import BaseModelShow, ModCorpusAssociationSchemaPost


class ModSchemaPost(BaseModel):
    abbreviation: str
    short_name: str
    full_name: str
    #mod_corpus_associations: Optional[List[ModCorpusAssociationSchemaPost]] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ModSchemaShow(BaseModelShow):
    mod_id: int
    abbreviation: str
    short_name: str
    full_name: str

class ModSchemaUpdate(BaseModel):
    abbreviation: Optional[str] = None
    short_name: Optional[str] = None
    full_name: Optional[str] = None


class ModSchemaCreate(ModSchemaPost):

    class Config():
        orm_mode = True
        extra = "forbid"
