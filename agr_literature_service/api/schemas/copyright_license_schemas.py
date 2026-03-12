from typing import Optional

from pydantic import BaseModel, ConfigDict


class CopyrightLicenseSchemaPost(BaseModel):
    """Schema for creating or posting a copyright license."""
    model_config = ConfigDict(
        extra='ignore',        # ignore unexpected fields
        from_attributes=True    # enable ORM->model initialization
    )

    name: str
    url: str
    description: str
    open_access: bool


class CopyrightLicenseSchemaShow(BaseModel):
    """Schema for showing a copyright license."""
    model_config = ConfigDict(
        extra='ignore',
        from_attributes=True
    )

    copyright_license_id: int
    name: str
    url: Optional[str] = None
    description: Optional[str] = None
    open_access: Optional[bool] = None
