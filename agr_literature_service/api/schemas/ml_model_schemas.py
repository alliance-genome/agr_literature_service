from typing import Union

from pydantic import BaseModel


class MLModelSchemaBase(BaseModel):

    task_type: str
    mod_abbreviation: str
    topic: Union[str, None]
    version_num: Union[int, None]
    model_type: str
    file_extension: str
    precision: Union[float, None]
    recall: Union[float, None]
    f1_score: Union[float, None]
    parameters: Union[str, None]
    dataset_id: Union[int, None]
    production: Union[bool, None]
    negated: Union[bool, None]
    novel_topic_data: Union[bool, None]
    species: Union[str, None]

class MLModelSchemaPost(MLModelSchemaBase):
    pass


class MLModelSchemaShow(MLModelSchemaBase):
    ml_model_id: int

    class Config:
        orm_mode = True
