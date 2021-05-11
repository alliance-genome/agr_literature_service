from typing import Optional

from pydantic import BaseModel

class MeshDetail(BaseModel):
   headingTerm: str
   qualifierTerm: Optional[str]
