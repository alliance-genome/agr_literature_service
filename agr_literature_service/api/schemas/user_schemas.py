from typing import List

from pydantic import BaseModel, ConfigDict, Field

from agr_literature_service.api.schemas import ResourceSchemaShow


class UserSchema(BaseModel):
    """Schema for user creation input."""
    model_config = ConfigDict(
        extra='forbid',        # no unexpected fields
        from_attributes=True    # enable ORM->model initialization if used
    )

    name: str
    email: str
    password: str


class ShowUserSchema(BaseModel):
    """Schema for showing user details with associated resources."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    name: str
    email: str
    resources: List[ResourceSchemaShow] = Field(default_factory=list)
