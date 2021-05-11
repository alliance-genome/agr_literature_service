from typing import List, Optional

from pydantic import BaseModel


class ModReferenceType(BaseModel):
    referenceType: str
    source: Optional[str] = None
