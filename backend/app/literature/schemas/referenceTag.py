from typing import List, Optional

from pydantic import BaseModel

from literature.schemas.tagName import TagName
from literature.schemas.tagSource import TagSource

class ReferenceTag(BaseModel):
    tagName: TagName = None
    tagSoource: Optional[TagSource] = None
