from typing import Union

from pydantic import BaseModel


class MLModelSchemaBase(BaseModel):

    task_type: str
    mod_abbreviation: str
    topic: str
    version_num: Union[int, None]
    model_type: str
    file_extension: str
    precision: float
    recall: float
    f1_score: float
    parameters: str
    dataset_id: Union[int, None]


class MLModelSchemaPost(MLModelSchemaBase):
    pass


class MLModelSchemaShow(MLModelSchemaBase):
    ml_model_id: int

    class Config:
        orm_mode = True
