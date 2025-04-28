from pydantic import ConfigDict, BaseModel


class CopyrightLicenseSchemaPost(BaseModel):
    name: str
    url: str
    description: str
    open_access: bool
    model_config = ConfigDict(from_attributes=True, extra="forbid")
