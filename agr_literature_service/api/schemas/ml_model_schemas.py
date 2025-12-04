from typing import Optional

from pydantic import BaseModel, ConfigDict


class MLModelSchemaBase(BaseModel):
    """Base schema for ML model metadata."""
    model_config = ConfigDict(
        extra='forbid',        # forbid unexpected fields
        from_attributes=True    # enable ORM->model initialization
    )

    task_type: str
    mod_abbreviation: str
    topic: Optional[str] = None
    version_num: Optional[int] = None
    model_type: str
    file_extension: str
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    parameters: Optional[str] = None
    dataset_id: Optional[int] = None
    production: Optional[bool] = None
    negated: Optional[bool] = None
    data_novelty: Optional[str] = None
    species: Optional[str] = None


class MLModelSchemaPost(MLModelSchemaBase):
    """Schema used when posting a new ML model entry."""
    pass


class MLModelSchemaShow(MLModelSchemaBase):
    """Schema used when returning ML model with its primary key."""
    ml_model_id: int
