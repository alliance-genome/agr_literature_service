from typing import List
from typing import Optional

from datetime import datetime

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas import FileCategories
from literature.schemas import ReferenceSchemaShow


class FileSchemaUpdate(BaseModel):
    public: Optional[bool] = None
    extension: Optional[str] =  None
    content_type: Optional[str] = None
    reference_id: Optional[str] = None
    category: Optional[FileCategories] = None
    display_name: Optional[str] = None
    language: Optional[str] = 'english'
    reference_curie: Optional[str] = None
    mod_submitted: Optional[str] = None
    mods_permitted: Optional[List[str]] = None
    institutes_permitted: Optional[List[str]] = None
    synonyms: Optional[List[str]] = None


    @validator('public')
    def public_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set public to None')
        return v

    @validator('extension')
    def extension_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set extension to None')
        return v

    @validator('content_type')
    def content_type_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set content_type to None')
        return v

    @validator('language')
    def language_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set language to None')
        return v

    class Config():
         orm_mode = True
         extra = "forbid"


class FileSchemaShow(BaseModel):
    file_id: int
    s3_filename: str
    folder: str

    md5sum: str
    size: int
    upload_date: datetime
    public: bool
    extension: str
    content_type: str
    language: str = 'english'

    reference_id: Optional[str] = None
    category: Optional[FileCategories] = None
    display_name: Optional[str] = None
    reference: Optional[ReferenceSchemaShow] = None
    mod_submitted: Optional[str] = None
    mods_permitted: Optional[List[str]] = None
    institutes_permitted: Optional[List[str]] = None
    synonyms: Optional[List[str]] = None

    class Config():
         orm_mode = True
         extra = "forbid"
