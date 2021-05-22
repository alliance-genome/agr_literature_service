from typing import Optional

from pydantic import BaseModel

class MeshDetail(BaseModel):
   heading_term: str
   qualifier_term: Optional[str]

   class Config():
        orm_mode = True
        extra = "forbid"

class MeshDetailShow(MeshDetail):
   mesh_detail_id: int

   class Config():
        orm_mode = True
        extra = "forbid"
