from pydantic import BaseModel

from literature.schemas import TagName
from literature.schemas import TagSource


class ReferenceTag(BaseModel):
    tag_name: TagName
    tag_source: TagSource

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceTagShow(ReferenceTag):
    reference_tag_id: int

    class Config():
        orm_mode = True
        extra = "forbid"
