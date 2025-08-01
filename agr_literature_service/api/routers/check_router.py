from fastapi import APIRouter, Depends, Security
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import check_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (AteamApiSchemaShow, DatabaseSchemaShow, EnvironmentsSchemaShow)

router = APIRouter(
    prefix="/check",
    tags=['Check']
)

get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.get('/ateamapi',
            response_model=AteamApiSchemaShow,
            status_code=200)
def check_ateam_api():
    res = check_crud.check_ateam_api()
    return res


@router.get('/database',
            response_model=DatabaseSchemaShow,
            status_code=200)
def check_database(db: Session = db_session):
    return {"db_details": check_crud.check_database(db)}


@router.get('/check_obsolete_entities',
            status_code=200)
def check_obsolete_entities():
    return check_crud.check_obsolete_entities()


@router.get('/check_redacted_references_with_tags',
            status_code=200)
def check_redacted_references_with_tags():
    return check_crud.check_redacted_references_with_tags()


@router.get('/check_obsolete_pmids',
            status_code=200)
def check_obsolete_pmids():
    return check_crud.check_obsolete_pmids()


@router.get('/check_duplicate_orcids',
            status_code=200)
def check_duplicate_orcids():
    return check_crud.check_duplicate_orcids()


@router.get('/environments',
            response_model=EnvironmentsSchemaShow,
            status_code=200)
def show_environments():
    res = check_crud.show_environments()
    return {'envs': res}
