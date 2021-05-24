from typing import Optional

from pydantic import BaseModel


class MeshDetailSchemaCreate(BaseModel):
   heading_term: str
   qualifier_term: Optional[str]

   class Config():
        orm_mode = True
        extra = "forbid"

class MeshDetailSchemaPost(MeshDetailSchemaCreate):
   reference_curie: str

   class Config():
        orm_mode = True
        extra = "forbid"



class MeshDetailSchemaShow(MeshDetailSchemaCreate):
   mesh_detail_id: int

   class Config():
        orm_mode = True
        extra = "forbid"


class MeshDetailSchemaUpdate(MeshDetailSchemaShow):
   reference_curie: str

   class Config():
        orm_mode = True
        extra = "forbid"
