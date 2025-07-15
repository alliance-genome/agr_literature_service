from pydantic import BaseModel, ConfigDict


class CopyrightLicenseSchemaPost(BaseModel):
    """Schema for creating or posting a copyright license."""
    model_config = ConfigDict(
        extra='forbid',        # forbid unexpected fields
        from_attributes=True    # enable ORM->model initialization
    )

    name: str
    url: str
    description: str
    open_access: bool
