from fastapi import APIRouter, Depends, Response, Security, status, UploadFile, File
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import ml_model_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas.ml_model_schemas import MLModelSchemaPost, MLModelSchemaShow
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/ml_model",
    tags=['ML Models']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/upload',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def upload(request: MLModelSchemaPost,
           file: UploadFile = File(...),  # noqa: B008
           user: OktaUser = db_user,
           db: Session = db_session):
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


@router.get('/download/{task_type}/{mod_abbreviation}/{topic}/{version_num}',
            response_model=MLModelSchemaShow,
            status_code=200)
def download_model(task_type: str,
                   mod_abbreviation: str,
                   topic: str,
                   version_num: int,
                   db: Session = db_session):
    return ml_model_crud.download(db, task_type, topic, mod_abbreviation, version_num)
