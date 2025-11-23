from typing import Union, List, Dict, Any

from fastapi import (APIRouter, Depends, HTTPException, Response,
                     Security, status)
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session
from multiprocessing import Process, Manager, Lock

from agr_literature_service.api import database
from agr_literature_service.api.crud import cross_reference_crud, reference_crud
from agr_literature_service.api.s3 import download
from agr_literature_service.api.deps import s3_auth
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (ReferenceSchemaPost, ReferenceSchemaShow,
                                                ReferenceSchemaUpdate, ResponseMessageSchema)
from agr_literature_service.api.schemas.reference_schemas import ReferenceSchemaAddPmid
from agr_literature_service.api.user import set_global_user_from_okta

from agr_cognito_auth import get_current_user_swagger, get_current_user, require_groups

import datetime
import logging

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.process_single_pmid import process_pmid
from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import dump_data


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reference",
    tags=['Reference'])


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)
s3_session = Depends(s3_auth)

running_processes_dumps_ondemand: Union[dict, None] = None
lock_dumps_ondemand = None


# Cognito stuff to test
# @router.post('/')
# async def create_reference(
#     request: ReferenceSchemaPost,
#     user: Dict[str, Any] = Security(get_current_user_swagger)
#     db: Session = Depends(get_db)
# ):
#     """
#     Create a new reference.
# 
#     Requires authentication with valid Cognito token.
#     """
#     # Set user context from Cognito token
#     set_global_user_from_cognito(db, user)
# 
#     # Create reference
#     return reference_crud.create(db, request)


@router.get('/whoami')
async def get_current_user_info(
    user: Dict[str, Any] = Security(get_current_user_swagger)
):
    """Get information about the currently authenticated user."""
    return {
        "user_id": user["sub"],
        "email": user["email"],
        "name": user["name"],
        "groups": user["cognito:groups"]
    }


# @router.post('/admin/bulk-import')	# sample thing ?  I don't know why claude made this up
# async def bulk_import_references(
#     file: UploadFile,
#     user: Dict[str, Any] = Depends(require_groups("SuperAdmin", "WBStaff")),
#     db: Session = Depends(get_db)
# ):
#     """
#     Bulk import references (admin only).
# 
#     Requires user to be in SuperAdmin or WBStaff group.
#     """
#     # Only users with required groups can access this endpoint
#     ...


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: ReferenceSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return reference_crud.create(db, request)


# @router.post('/add/{pubmed_id}/{mod_curie}/{mod_mca}/',
@router.post('/add/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def add(request: ReferenceSchemaAddPmid,
        user: OktaUser = db_user,
        db: Session = db_session):
    set_global_user_from_okta(db, user)
    mod_curie = request.mod_curie
    if mod_curie is None:
        mod_curie = ''
    return process_pmid(request.pubmed_id, mod_curie, request.mod_mca)


@router.delete('/{curie_or_reference_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(curie_or_reference_id: str,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    reference_crud.destroy(db, curie_or_reference_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# @router.patch('/{curie_or_reference_id}',
#               status_code=status.HTTP_202_ACCEPTED,
#               response_model=ResponseMessageSchema)
# async def patch(curie_or_reference_id: str,
#                 request: ReferenceSchemaUpdate,
#                 user: OktaUser = db_user,
#                 db: Session = db_session):
#     set_global_user_from_okta(db, user)
#     patch = request.model_dump(exclude_unset=True)
#     return reference_crud.patch(db, curie_or_reference_id, patch)


# probably broken and needs global_user
@router.patch('/{curie_or_reference_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(curie_or_reference_id: str,
                request: ReferenceSchemaUpdate,
                user: Dict[str, Any] = Security(get_current_user_swagger),
                db: Session = db_session):
#     set_global_user_from_okta(db, user)
# don't know how global_user works instead do ?
# from agr_literature_service.api.crud.user_utils import map_to_user_id
#     global _current_user_id = map_to_user_id(user["email"], db)
    patch = request.model_dump(exclude_unset=True)
    return reference_crud.patch(db, curie_or_reference_id, patch)


@router.get('/dumps/latest/{mod}',
            status_code=200)
def download_data_by_mod(mod: str,
                         user: OktaUser = db_user,
                         db: Session = db_session):

    set_global_user_from_okta(db, user)
    return download.get_json_file(mod)


@router.get('/dumps/{filename}',
            status_code=200)
def download_data_by_filename(filename: str,
                              user: OktaUser = db_user,
                              db: Session = db_session):

    set_global_user_from_okta(db, user)
    return download.get_json_file(None, filename)


def dump_data_process_wrapper(running_processes_dict, lock, mod: str, email: str, ondemand: int, api_url: str):
    dump_data(mod=mod, email=email, ondemand=ondemand, ui_root_url=api_url)
    try:
        lock.acquire()
        process_name = email
        del running_processes_dict[process_name]
    finally:
        lock.release()


@router.post('/dumps/ondemand',
             status_code=201)
def generate_data_ondemand(mod: str,
                           email: str,
                           ui_root_url: str,
                           user: OktaUser = db_user,
                           db: Session = db_session):

    set_global_user_from_okta(db, user)
    global running_processes_dumps_ondemand
    global lock_dumps_ondemand
    if not running_processes_dumps_ondemand:
        running_processes_dumps_ondemand = Manager().dict()
    if not lock_dumps_ondemand:
        lock_dumps_ondemand = Lock()

    if mod is None or email is None or ui_root_url is None:
        return {
            "message": "You need to be logged in order to download files."
        }

    process_name = email
    try:
        lock_dumps_ondemand.acquire()
        if process_name in running_processes_dumps_ondemand:
            return {
                "message": "Your file is getting generated, no need to submit the request again."
            }
        else:
            running_processes_dumps_ondemand[process_name] = 1
            p = Process(target=dump_data_process_wrapper,
                        args=(running_processes_dumps_ondemand, lock_dumps_ondemand, mod, email, 1,
                              ui_root_url))
            p.start()
            return {
                "message": "Generating a new reference file for " + mod + ". A download link will be emailed to " + email + "."
            }
    finally:
        lock_dumps_ondemand.release()


@router.get('/by_cross_reference/{curie_or_cross_reference_id}',
            status_code=200,
            response_model=ReferenceSchemaShow)
def show_xref(curie_or_cross_reference_id: str,
              db: Session = db_session):
    cross_reference = cross_reference_crud.show(db, curie_or_cross_reference_id)

    if 'reference_curie' not in cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference {curie_or_cross_reference_id} is not associated to "
                                   f"a reference entity")

    return reference_crud.show(db, cross_reference['reference_curie'])


@router.get('/{curie_or_reference_id}',
            status_code=200,
            response_model=ReferenceSchemaShow)
def show(curie_or_reference_id: str,
         db: Session = db_session):
    return reference_crud.show(db, curie_or_reference_id)


@router.get('/{curie_or_reference_id}/versions',
            status_code=200)
def show_versions(curie_or_reference_id: str,
                  db: Session = db_session):
    return reference_crud.show_changesets(db, curie_or_reference_id)


@router.post('/merge/{old_curie}/{new_curie}',
             status_code=201)
def merge_references(old_curie: str,
                     new_curie: str,
                     user: OktaUser = db_user,
                     db: Session = db_session):
    set_global_user_from_okta(db, user)
    return reference_crud.merge_references(db, old_curie, new_curie)


@router.post('/add_license/{curie}/{license}',
             status_code=201)
def add_license(curie: str,
                license: str,
                user: OktaUser = db_user,
                db: Session = db_session):

    set_global_user_from_okta(db, user)
    return reference_crud.add_license(db, curie, license)


@router.get('/missing_files/{mod_abbreviation}',
            status_code=status.HTTP_200_OK)
def missing_files(mod_abbreviation: str,
                  order_by: str,
                  page: int,
                  filter: str,
                  db: Session = db_session):
    missing_files = reference_crud.missing_files(db, mod_abbreviation, order_by, page, filter)
    if not missing_files:
        return []
    return missing_files


@router.get('/download_tracker_table/{mod_abbreviation}',
            status_code=status.HTTP_200_OK)
def download_tracker_table(mod_abbreviation: str,
                           order_by: str,
                           filter: str,
                           db: Session = db_session):
    return reference_crud.download_tracker_table(db, mod_abbreviation, order_by, filter)


@router.get('/get_bib_info/{curie}',
            status_code=status.HTTP_200_OK)
def get_bib_info(curie: str,
                 mod_abbreviation: str,
                 return_format: str = 'txt',
                 user: OktaUser = db_user,
                 db: Session = db_session):
    set_global_user_from_okta(db, user)
    return reference_crud.get_bib_info(db, curie, mod_abbreviation, return_format)


@router.get('/get_textpresso_reference_list/{mod_abbreviation}',
            status_code=status.HTTP_200_OK,
            response_model=List[Dict])
def get_textpresso_reference_list(mod_abbreviation: str,
                                  files_updated_from_date: datetime.date = None,
                                  reference_type: str = None,
                                  species: str = None,
                                  from_reference_id: int = None,
                                  page_size: int = 1000,
                                  user: OktaUser = db_user,
                                  db: Session = db_session):
    set_global_user_from_okta(db, user)
    return reference_crud.get_textpresso_reference_list(db, mod_abbreviation,
                                                        files_updated_from_date,
                                                        reference_type,
                                                        species,
                                                        from_reference_id,
                                                        page_size)


@router.post('/add_to_corpus/{mod_abbreviation}/{reference_curie}',
             status_code=201)
def add_to_corpus(mod_abbreviation: str,
                  reference_curie: str,
                  user: OktaUser = db_user,
                  db: Session = db_session):

    set_global_user_from_okta(db, user)
    return reference_crud.add_to_corpus(db, mod_abbreviation, reference_curie)


@router.get('/get_recently_sorted_references/{mod_abbreviation}',
            status_code=status.HTTP_200_OK)
def get_recently_sorted_references(mod_abbreviation: str,
                                   days: int = 7,
                                   db: Session = db_session):
    references = reference_crud.get_recently_sorted_references(db, mod_abbreviation, days)

    return references


@router.get('/get_recently_sorted_pmids/{mod_abbreviation}',
            status_code=status.HTTP_200_OK)
def get_recently_sorted_pmids(mod_abbreviation: str,
                              days: int = 7,
                              db: Session = db_session):
    pmid_only = True
    pmids = reference_crud.get_recently_sorted_references(db, mod_abbreviation,
                                                          days, pmid_only)
    return pmids


@router.get('/get_recently_deleted_references/{mod_abbreviation}',
            status_code=status.HTTP_200_OK)
def get_recently_deleted_references(mod_abbreviation: str,
                                    days: int = 7,
                                    db: Session = db_session):
    references = reference_crud.get_recently_deleted_references(db, mod_abbreviation, days)

    return references


@router.get('/lock_status/{referenceCurie}',
            status_code=status.HTTP_200_OK)
def lock_status(referenceCurie: str,
                db: Session = db_session):
    lock_details = reference_crud.lock_status(db, referenceCurie)

    return lock_details
