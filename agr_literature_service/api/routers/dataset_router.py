from fastapi import APIRouter, Depends, Security
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api import database
from agr_literature_service.api.crud import dataset_crud
from agr_literature_service.api.schemas.dataset_schema import DatasetSchemaPost, DatasetSchemaDownload, \
    DatasetSchemaUpdate, DatasetSchemaShow, DatasetEntrySchemaPost
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(
    prefix='/datasets',
    tags=['Datasets']
)


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post("/",
             status_code=status.HTTP_201_CREATED,
             response_model=DatasetSchemaShow)
def create_dataset(request: DatasetSchemaPost,
                   user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                   db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return dataset_crud.create_dataset(db, dataset=request)


@router.get("/metadata/{mod_abbreviation}/{data_type}/{dataset_type}/{version}/",
            status_code=status.HTTP_200_OK,
            response_model=DatasetSchemaShow)
@router.get("/metadata/{mod_abbreviation}/{data_type}/{dataset_type}/",
            status_code=status.HTTP_200_OK,
            response_model=DatasetSchemaShow)
def show_dataset(mod_abbreviation: str, data_type: str, dataset_type: str, version: int = None,
                 db: Session = db_session,
                 user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    set_global_user_from_cognito(db, user)
    return dataset_crud.show_dataset(db, mod_abbreviation=mod_abbreviation, data_type=data_type,
                                     dataset_type=dataset_type, version=version)


@router.delete("/{mod_abbreviation}/{data_type}/{dataset_type}/{version}/",
               status_code=status.HTTP_204_NO_CONTENT)
def delete_dataset(mod_abbreviation: str, data_type: str, dataset_type: str, version: int,
                   user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                   db: Session = db_session):
    set_global_user_from_cognito(db, user)
    dataset_crud.delete_dataset(db, mod_abbreviation=mod_abbreviation, data_type=data_type,
                                dataset_type=dataset_type, version=version)


@router.patch("/{mod_abbreviation}/{data_type}/{dataset_type}/{version}",
              status_code=status.HTTP_202_ACCEPTED,
              response_model=str)
def patch_dataset(request: DatasetSchemaUpdate, mod_abbreviation: str, data_type: str, dataset_type: str, version: int,
                  user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                  db: Session = db_session):
    set_global_user_from_cognito(db, user)
    dataset_crud.patch_dataset(db, mod_abbreviation=mod_abbreviation, data_type=data_type,
                               dataset_type=dataset_type, version=version, dataset_update=request)
    return "updated"


@router.get("/download/{mod_abbreviation}/{data_type}/{dataset_type}/{version}/",
            response_model=DatasetSchemaDownload)
@router.get("/download/{mod_abbreviation}/{data_type}/{dataset_type}/",
            response_model=DatasetSchemaDownload)
def download_dataset(mod_abbreviation: str, data_type: str, dataset_type: str, version: int = None,
                     db: Session = db_session,
                     user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    set_global_user_from_cognito(db, user)
    db_dataset = dataset_crud.download_dataset(db, mod_abbreviation=mod_abbreviation, data_type=data_type,
                                               dataset_type=dataset_type, version=version)
    return db_dataset


@router.post("/data_entry/",
             status_code=status.HTTP_201_CREATED)
def add_entry_to_dataset(request: DatasetEntrySchemaPost,
                         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                         db: Session = db_session):
    set_global_user_from_cognito(db, user)
    dataset_crud.add_entry_to_dataset(db, request)


@router.delete("/data_entry/{mod_abbreviation}/{data_type}/{dataset_type}/{version}/{reference_curie}/{entity}/",
               status_code=status.HTTP_202_ACCEPTED)
def delete_entry_from_dataset(mod_abbreviation: str, data_type: str, dataset_type: str, version: int,
                              reference_curie: str, entity: str,
                              user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                              db: Session = db_session):
    set_global_user_from_cognito(db, user)
    dataset_crud.delete_entry_from_dataset(db, mod_abbreviation, data_type, dataset_type, version, reference_curie,
                                           entity)


@router.delete("/data_entry/{mod_abbreviation}/{data_type}/{dataset_type}/{version}/{reference_curie}/",
               status_code=status.HTTP_202_ACCEPTED)
def delete_entry_from_dataset_no_entity(mod_abbreviation: str, data_type: str, dataset_type: str, version: int,
                                        reference_curie: str,
                                        user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                                        db: Session = db_session):
    set_global_user_from_cognito(db, user)
    dataset_crud.delete_entry_from_dataset(db, mod_abbreviation, data_type, dataset_type, version, reference_curie)
