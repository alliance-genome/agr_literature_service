import threading
from os import environ
from fastapi import (APIRouter, Depends, HTTPException, Response,
                     Security, status)
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import cross_reference_crud, reference_crud
from agr_literature_service.api.s3 import download
from agr_literature_service.api.deps import s3_auth
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (ReferenceSchemaPost, ReferenceSchemaShow,
                                                ReferenceSchemaUpdate, ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_id, get_global_user_id

import logging

from agr_literature_service.lit_processing.process_single_pmid import process_pmid
from agr_literature_service.lit_processing.dump_json_data import dump_data

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reference",
    tags=['Reference'])


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)
s3_session = Depends(s3_auth)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: ReferenceSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_id(db, user.id)
    return reference_crud.create(db, request)


@router.post('/add/{pubmed_id}/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def add(pubmed_id: str,
        user: OktaUser = db_user,
        db: Session = db_session):
    set_global_user_id(db, user.id)

    return process_pmid(pubmed_id)


@router.delete('/{curie}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(curie: str,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_id(db, user.id)
    reference_crud.destroy(db, curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{curie}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(curie: str,
                request: ReferenceSchemaUpdate,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)
    return reference_crud.patch(db, curie, patch)


@router.get('/dumps/latest/{mod}',
            status_code=200)
def download_data_by_mod(mod: str,
                  user: OktaUser = db_user,
                  db: Session = db_session):

    set_global_user_id(db, user.id)
    return download.get_json_file(mod)


@router.get('/dumps/{filename}',
            status_code=200)
def download_data_by_filename(filename: str,
                              user: OktaUser = db_user,
                              db: Session = db_session):

    set_global_user_id(db, user.id)
    return download.get_json_file(None, filename)


@router.get('/dumps/ondemand/{mod}',
            status_code=200)
def generate_data_ondemand(mod: str,
                           user: OktaUser = db_user,
                           db: Session = db_session):

    set_global_user_id(db, user.id)

    my_threads = []
    for t in threading.enumerate():
        my_threads.append(t.name)

    email = get_global_user_id()

    # for testing purpose
    if '@' not in email:
        email = 'sweng@stanford.edu'

    thread_name = mod + "|" + email
    if thread_name in my_threads:
        return {"message": "You have already submitted a request for generating " + mod + " Reference json file so no need to submit again."}

    t = threading.Thread(daemon=True, target=dump_data(mod, email, 1, environ.get('API_URL')))
    threading.current_thread().name = thread_name
    t.start()

    return {"message": "Generating " + mod + " Reference json file now. An email will be sent to you shortly when the json file is ready."}


@router.get('/by_cross_reference/{curie:path}',
            status_code=200,
            response_model=ReferenceSchemaShow)
def show_xref(curie: str,
              db: Session = db_session):
    cross_reference = cross_reference_crud.show(db, curie)

    if 'reference_curie' not in cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference {curie} is not associated to a reference entity")

    return reference_crud.show(db, cross_reference['reference_curie'])


@router.get('/{curie}',
            status_code=200,
            response_model=ReferenceSchemaShow)
def show(curie: str,
         db: Session = db_session):
    return reference_crud.show(db, curie)


@router.get('/{curie}/versions',
            status_code=200)
def show_versions(curie: str,
                  db: Session = db_session):
    return reference_crud.show_changesets(db, curie)


@router.post('/merge/{old_curie}/{new_curie}',
             status_code=201)
def merge_references(old_curie: str,
                     new_curie: str,
                     user: OktaUser = db_user,
                     db: Session = db_session):
    set_global_user_id(db, user.id)
    return reference_crud.merge_references(db, old_curie, new_curie)
