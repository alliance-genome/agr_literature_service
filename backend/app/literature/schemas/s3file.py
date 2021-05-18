from typing import List
from typing import Optional

from datetime import datetime

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas import FileCategories
from literature.schemas import ReferenceSchemaShow


class FileSchemaUpdate(BaseModel):
    public: bool
    extension: str

    content_type: Optional[str] = None
    category: Optional[FileCategories] = None
    display_name: Optional[str] = None
    reference_id: Optional[str] = None
    mod_submitted: Optional[str] = None
    mod_permission: Optional[List[str]] = None
    institute_permission: Optional[List[str]] = None
    synonyms: Optional[List[str]] = None

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
    content_type: str = None

    reference_id: Optional[str] = None
    category: Optional[FileCategories] = None
    display_name: Optional[str] = None
    reference: Optional[ReferenceSchemaShow] = None
    mod_submitted: Optional[str] = None
    mod_permission: Optional[List[str]] = None
    institute_permission: Optional[List[str]] = None
    synonyms: Optional[List[str]] = None

    class Config():
         orm_mode = True
         extra = "forbid"
