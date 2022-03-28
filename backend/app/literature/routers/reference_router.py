import subprocess
from typing import List, cast

from botocore.client import BaseClient
from fastapi import (APIRouter, Depends, File, HTTPException, Response,
                     Security, UploadFile, status)
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from literature import database
from literature.crud import cross_reference_crud, file_crud, reference_crud
from literature.deps import s3_auth
from literature.routers.authentication import auth
from literature.schemas import (FileSchemaShow, NoteSchemaShow,
                                ReferenceSchemaPost, ReferenceSchemaShow,
                                ReferenceSchemaUpdate, ResponseMessageSchema)
from literature.user import set_global_user_id

import logging

from process_single_pmid import process_pmid

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


@router.get('/{curie}/files',
            status_code=200,
            response_model=List[FileSchemaShow])
def show_files(curie: str,
               db: Session = db_session):
    return reference_crud.show_files(db, curie)


@router.get('/{curie}/notes',
            status_code=200,
            response_model=List[NoteSchemaShow])
def show_notes(curie: str,
               db: Session = db_session):
    return reference_crud.show_notes(db, curie)


@router.post('/{curie}/upload_file',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
async def create_upload_file(curie: str,
                             file_obj: UploadFile = File(...),  # noqa
                             s3: BaseClient = s3_session,
                             user: OktaUser = db_user,
                             db: Session = db_session):
    set_global_user_id(db, user.id)

    file_contents = bytes()
    # Check if file is in binary mode. read() will return bytes
    if "b" in file_obj.file.mode:
        file_contents = cast(bytes, await file_obj.read())
    else:
        # file is in text mode. So convert read() to bytes
        contents = cast(str, await file_obj.read())
        file_contents = bytes(contents, "utf-8")

    filename = file_obj.filename
    content_type = file_obj.content_type

    return file_crud.create(db, s3, 'reference', curie, file_contents, filename, content_type)


@router.get('/{curie}/versions',
            status_code=200)
def show_versions(curie: str,
                  db: Session = db_session):
    return reference_crud.show_changesets(db, curie)
