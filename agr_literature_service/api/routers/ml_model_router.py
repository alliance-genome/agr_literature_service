import json
from json import JSONDecodeError
from typing import Union

from fastapi import APIRouter, Depends, Response, Security, status, UploadFile, File, HTTPException
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import ml_model_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas.ml_model_schemas import MLModelSchemaPost, MLModelSchemaShow
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/ml_model",
    tags=['ML Model']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post("/upload",
             status_code=status.HTTP_201_CREATED,
             response_model=MLModelSchemaShow
             )
def upload_model(
        task_type: str = None,
        mod_abbreviation: str = None,
        topic: str = None,
        version_num: int = None,
        file_extension: str = None,
        model_type: str = None,
        precision: float = None,
        recall: float = None,
        f1_score: float = None,
        parameters: str = None,
        dataset_id: int = None,
        file: UploadFile = File(...),  # noqa: B008
        model_data_file: Union[UploadFile, None] = File(default=None),  # noqa: B008
        user: OktaUser = db_user,
        db: Session = db_session,
        production: bool = False,
        negated: bool = True,
        novel_topic_data: bool = False,
        novel_topic_qualifier: str = None,
        species: str = None):
    model_data = None
    if task_type and mod_abbreviation:
        model_data = {
            "task_type": task_type,
            "mod_abbreviation": mod_abbreviation,
            "topic": topic,
            "version_num": version_num,
            "file_extension": file_extension,
            "model_type": model_type,
            "precision": precision,
            "recall": recall,
            "f1_score": f1_score,
            "parameters": parameters,
            "dataset_id": dataset_id,
            "production": production,
            "negated": negated,
            "novel_topic_data": novel_topic_data,
            "novel_topic_qualifier": novel_topic_qualifier,
            "species": species
        }
    elif model_data_file is not None:
        try:
            model_data = json.load(model_data_file.file)
        except JSONDecodeError:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="The provided model data file is not a valid json file")
    if not model_data:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="The provided model data is not valid")
    request = MLModelSchemaPost(
        task_type=model_data["task_type"],
        mod_abbreviation=model_data["mod_abbreviation"],
        topic=model_data["topic"],
        version_num=model_data["version_num"],
        file_extension=model_data["file_extension"],
        model_type=model_data["model_type"],
        precision=model_data["precision"],
        recall=model_data["recall"],
        f1_score=model_data["f1_score"],
        parameters=model_data["parameters"],
        dataset_id=model_data["dataset_id"],
        production=model_data["production"],
        negated=model_data["negated"],
        novel_topic_data=model_data["novel_topic_data"],
        novel_topic_qualifier=model_data["novel_topic_qualifier"],
        species=model_data["species"]
    )
    set_global_user_from_okta(db, user)
    return ml_model_crud.upload(db, request, file)


@router.delete('/{ml_model_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(ml_model_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    ml_model_crud.destroy(db, ml_model_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get('/metadata/{task_type}/{mod_abbreviation}/{topic}/{version}',
            response_model=MLModelSchemaShow,
            status_code=200)
@router.get('/metadata/{task_type}/{mod_abbreviation}/{topic}',
            response_model=MLModelSchemaShow,
            status_code=200)
@router.get('/metadata/{task_type}/{mod_abbreviation}',
            response_model=MLModelSchemaShow,
            status_code=200)
def get_model_metadata(task_type: str,
                       mod_abbreviation: str,
                       topic: str = None,
                       version: str = None,
                       db: Session = db_session):
    print(f"BOB: Router mod:{mod_abbreviation} topic:{topic} version:{version}")
    return ml_model_crud.get_model_metadata(db, task_type, mod_abbreviation, topic, version)


@router.get('/download/{task_type}/{mod_abbreviation}/{topic}/{version}',
            response_model=MLModelSchemaShow,
            status_code=200)
@router.get('/download/{task_type}/{mod_abbreviation}/{topic}',
            response_model=MLModelSchemaShow,
            status_code=200)
@router.get('/download/{task_type}/{mod_abbreviation}',
            response_model=MLModelSchemaShow,
            status_code=200)
def download_model_file(task_type: str,
                        mod_abbreviation: str,
                        topic: str = None,
                        version: str = None,
                        db: Session = db_session):
    return ml_model_crud.download_model_file(db, task_type, mod_abbreviation, topic, version)
