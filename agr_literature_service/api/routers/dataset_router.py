from fastapi import APIRouter, Depends, HTTPException, Security
from sqlalchemy.orm import Session
from typing import List

from starlette import status

from agr_literature_service.api import database
from agr_literature_service.api.crud import dataset_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas.dataset_schema import DatasetSchemaPost, DatasetSchemaShow, DatasetSchemaBase

router = APIRouter(
    prefix='/datasets',
    tags=['Datasets']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post("/",
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create_dataset(dataset: DatasetSchemaPost, db: Session = db_session):
    return dataset_crud.create_dataset(db, dataset)


@router.delete("/{mod_abbreviation}/{data_type_topic}/{dataset_type}/",
               status_code=status.HTTP_204_NO_CONTENT)
def delete_dataset(mod_abbreviation: str, data_type_topic: str, dataset_type: str, db: Session = db_session):
    return dataset_crud.delete_dataset(db, mod_abbreviation=mod_abbreviation, data_type_topic=data_type_topic,
                                       dataset_type=dataset_type)


@router.get("/{mod_abbreviation}/{data_type_topic}/{dataset_type}/", response_model=DatasetSchemaShow)
def download_dataset(mod_abbreviation: str, data_type_topic: str, dataset_type: str, db: Session = Depends(get_db)):
    db_dataset = dataset_crud.download_dataset(db, mod_abbreviation=mod_abbreviation, data_type_topic=data_type_topic,
                                               dataset_type=dataset_type)
    return db_dataset


@router.post("/", response_model=Dataset)
def add_topic_entity_tag_to_dataset(dataset_id: int, dataset: DatasetUpdate, db: Session = Depends(get_db)):
    db_dataset = dataset_crud.update_dataset(db, dataset_id, dataset)
    if db_dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return db_dataset


@router.delete("/", response_model=bool)
def delete_topic_entity_tag_from_dataset(dataset_id: int, db: Session = Depends(get_db)):
    success = dataset_crud.delete_dataset(db, dataset_id)
    if not success:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return success


