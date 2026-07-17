from datetime import datetime
from typing import List, Optional

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
    file_classes: Optional[List[str]] = None
    description: Optional[str] = None
    # ABC-embedding recipe (SCRUM-5781); NULL for legacy BioWordVec models.
    embedding_profile: Optional[str] = None
    embedding_version: Optional[int] = None
    embedding_model: Optional[str] = None
    embedding_dim: Optional[int] = None
    embedding_pooling: Optional[str] = None
    use_bow_features: Optional[bool] = None


class MLModelSchemaPost(MLModelSchemaBase):
    """Schema used when posting a new ML model entry."""
    pass


class MLModelSchemaShow(MLModelSchemaBase):
    """Schema used when returning ML model with its primary key."""
    ml_model_id: int
    date_created: Optional[datetime] = None
    date_updated: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


class MLModelSchemaShowWithNames(MLModelSchemaShow):
    """Schema for list endpoints; adds human-readable names parallel to topic/data_novelty/species."""
    topic_name: Optional[str] = None
    data_novelty_name: Optional[str] = None
    species_name: Optional[str] = None
