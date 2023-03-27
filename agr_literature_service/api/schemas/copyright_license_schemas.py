from pydantic import BaseModel


class CopyrightLicenseSchemaPost(BaseModel):
    name: str
    url: str
    description: str
    open_access: bool

    class Config():
        orm_mode = True
        extra = "forbid"


class CopyrightLicenseSchemaShow(CopyrightLicenseSchemaPost):
    copyright_license_id: int
    name: str
    url: str
    description: str
    open_access: bool
