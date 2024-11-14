from fastapi import APIRouter, Depends, Security
from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api import database
from agr_literature_service.api.crud import dataset_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas.dataset_schema import DatasetSchemaPost, DatasetSchemaDownload, \
    DatasetSchemaUpdate

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


@router.delete("/{mod_abbreviation}/{data_type}/{dataset_type}/{version}/",
               status_code=status.HTTP_204_NO_CONTENT)
def delete_dataset(mod_abbreviation: str, data_type: str, dataset_type: str, version: int,
                   db: Session = db_session):
    return dataset_crud.delete_dataset(db, mod_abbreviation=mod_abbreviation, data_type=data_type,
                                       dataset_type=dataset_type, version=version)


@router.patch("/",
              status_code=status.HTTP_202_ACCEPTED,
              response_model=str)
def patch_dataset(mod_abbreviation: str, data_type: str, dataset_type: str, version: int,
                  dataset_update: DatasetSchemaUpdate, db: Session = db_session):
    return dataset_crud.patch_dataset(db, mod_abbreviation=mod_abbreviation, data_type=data_type,
                                      dataset_type=dataset_type, version=version, dataset_update=dataset_update)


@router.get("/{mod_abbreviation}/{data_type_topic}/{dataset_type}/{version}/",
            response_model=DatasetSchemaDownload)
def download_dataset(mod_abbreviation: str, data_type_topic: str, dataset_type: str, version: int,
                     db: Session = Depends(get_db)):
    db_dataset = dataset_crud.download_dataset(db, mod_abbreviation=mod_abbreviation, data_type_topic=data_type_topic,
                                               dataset_type=dataset_type, version=version)
    return db_dataset


@router.post("/data_entry/",
             status_code=status.HTTP_201_CREATED)
def add_entry_to_dataset(mod_abbreviation: str, data_type: str, dataset_type: str, version: int,
                         reference_id: int, entity: str,
                         supporting_topic_entity_tag_id: int = None, supporting_workflow_tag_id: int = None,
                         db: Session = Depends(get_db)):
    dataset_crud.add_entry_to_dataset(db, mod_abbreviation=mod_abbreviation,
                                      data_type=data_type,
                                      dataset_type=dataset_type,
                                      version=version,
                                      reference_id=reference_id,
                                      entity=entity,
                                      supporting_topic_entity_tag_id=supporting_topic_entity_tag_id,
                                      supporting_workflow_tag_id=supporting_workflow_tag_id)


@router.delete("/dataset_entry/",
               status_code=status.HTTP_202_ACCEPTED)
def delete_entry_from_dataset(mod_abbreviation: str, data_type: str, dataset_type: str,
                              version: int, reference_id: int, entity: str, db: Session = Depends(get_db)):
    dataset_crud.delete_entry_from_dataset(db, mod_abbreviation=mod_abbreviation,
                                           data_type=data_type,
                                           dataset_type=dataset_type,
                                           version=version,
                                           reference_id=reference_id,
                                           entity=entity)
