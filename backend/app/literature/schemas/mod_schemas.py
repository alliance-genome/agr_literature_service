# from typing import List
from typing import Optional

from pydantic import BaseModel
# from pydantic import validator

from literature.schemas import BaseModelShow
# from literature.schemas import ModCorpusAssociationSchemaPost


class ModSchemaPost(BaseModel):
    abbreviation: str
    short_name: str
    full_name: str
    # mod_corpus_associations: Optional[List[ModCorpusAssociationSchemaPost]] = None

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
