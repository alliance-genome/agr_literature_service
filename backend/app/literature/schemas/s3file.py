from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas.file_category import FileCategories

class FileSchemaUpload(BaseModel):
    category: FileCategories

    display_name: Optional[int] = None
    file_type: Optional[str] = None
    mod_submitted: Optional[str] = None
    public: Optional[str] = None
    synonyms: Optional[List]
