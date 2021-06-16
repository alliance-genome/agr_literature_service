from typing import List, Optional

from pydantic import BaseModel

from literature.schemas import TagName
from literature.schemas import TagSource

class ReferenceTag(BaseModel):
    tag_name: TagName = None
    tag_source: Optional[TagSource] = None

    class Config():
         orm_mode = True
         extra = "forbid"

class ReferenceTagShow(ReferenceTag):
    reference_tag_id: int

    class Config():
         orm_mode = True
         extra = "forbid"
