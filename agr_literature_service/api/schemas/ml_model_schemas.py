from typing import Union

from pydantic import ConfigDict, BaseModel


class MLModelSchemaBase(BaseModel):

    task_type: str
    mod_abbreviation: str
    topic: Union[str, None] = None
    version_num: Union[int, None] = None
    model_type: str
    file_extension: str
    precision: Union[float, None] = None
    recall: Union[float, None] = None
    f1_score: Union[float, None] = None
    parameters: Union[str, None] = None
    dataset_id: Union[int, None] = None
    production: Union[bool, None] = None
    negated: Union[bool, None] = None
    novel_topic_data: Union[bool, None] = None
    species: Union[str, None] = None


class MLModelSchemaPost(MLModelSchemaBase):
    pass


class MLModelSchemaShow(MLModelSchemaBase):
    ml_model_id: int
    model_config = ConfigDict(from_attributes=True)
