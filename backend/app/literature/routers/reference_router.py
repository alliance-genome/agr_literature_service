import subprocess

from typing import List

from sqlalchemy.orm import Session

from botocore.client import BaseClient

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security
from fastapi import File
from fastapi import UploadFile
from fastapi import HTTPException

from fastapi_okta import OktaUser

from literature import database

from literature.user import set_global_user_id

from literature.schemas import ReferenceSchemaShow
from literature.schemas import ReferenceSchemaPost
from literature.schemas import ReferenceSchemaUpdate
from literature.schemas import FileSchemaShow
from literature.schemas import NoteSchemaShow
from literature.schemas import ResponseMessageSchema

from literature.crud import reference_crud
from literature.crud import file_crud
from literature.crud import cross_reference_crud

from literature.routers.authentication import auth
from literature.deps import s3_auth
from literature.s3.upload import upload_file_to_bucket

from literature.models import CrossReferenceModel


router = APIRouter(
    prefix="/reference",
    tags=['Reference'])


get_db = database.get_db


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: ReferenceSchemaPost,
           user: OktaUser = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return reference_crud.create(db, request)


@router.post('/add/{pubmed_id}/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(pubmed_id: str,
           user: OktaUser = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)

    process = subprocess.run('python3 src/helloworld.py ' + pubmed_id, shell=True, stdout=subprocess.PIPE)

    return process.stdout.decode('utf-8')


@router.delete('/{curie}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(curie: str,
            user: OktaUser = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    reference_crud.destroy(db, curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{curie}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(curie: str,
                request: ReferenceSchemaUpdate,
                user: OktaUser = Security(auth.get_user),
                db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)

    return reference_crud.patch(db, curie, patch)

@router.get('/by_cross_reference/{curie:path}',
            status_code=200,
            response_model=ReferenceSchemaShow)
def show(curie: str,
         db: Session = Depends(get_db)):
    cross_reference = cross_reference_crud.show(db, curie)


    if 'reference_curie' not in cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference {curie} is not associated to a reference entity")

    return reference_crud.show(db, cross_reference['reference_curie'])


@router.get('/{curie}',
            status_code=200,
            response_model=ReferenceSchemaShow)
def show(curie: str,
         db: Session = Depends(get_db)):
    return reference_crud.show(db, curie)

@router.get('/{curie}/files',
            status_code=200,
            response_model=List[FileSchemaShow])
def show(curie: str,
         db: Session = Depends(get_db)):
    return reference_crud.show_files(db, curie)


@router.get('/{curie}/notes',
            status_code=200,
            response_model=List[NoteSchemaShow])
def show(curie: str,
         db: Session = Depends(get_db)):
    return reference_crud.show_notes(db, curie)


@router.post('/{curie}/upload_file',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
async def create_upload_file(curie: str,
                             file_obj: UploadFile = File(...),
                             s3: BaseClient = Depends(s3_auth),
                             user: OktaUser = Security(auth.get_user),
                             db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    file_contents = await file_obj.read()
    filename = file_obj.filename
    content_type = file_obj.content_type

    return file_crud.create(db, s3, 'reference', curie, file_contents, filename, content_type)


@router.get('/{curie}/versions',
            status_code=200)
def show(curie: str,
         db: Session = Depends(get_db)):
    return reference_crud.show_changesets(db, curie)
