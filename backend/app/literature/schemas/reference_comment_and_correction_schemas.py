from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas import BaseModelShow
from literature.schemas import ReferenceCommentAndCorrectionType


class ReferenceCommentAndCorrectionSchemaPost(BaseModel):
    reference_from_curie: str = None
    reference_to_curie: str = None
    reference_comment_and_correction_type: ReferenceCommentAndCorrectionType

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceCommentAndCorrectionSchemaShow(ReferenceCommentAndCorrectionSchemaPost):
    reference_comment_and_correction_id: int

    class Config():
        orm_mode = True
        extra = "forbid"

class ReferenceCommentAndCorrectionSchemaPatch(BaseModel):
    reference_from_curie: Optional[str] = None
    reference_to_curie: Optional[str] = None
    reference_comment_and_correction_type: Optional[ReferenceCommentAndCorrectionType] = None

    class Config():
        orm_mode = True
        extra = "forbid"


