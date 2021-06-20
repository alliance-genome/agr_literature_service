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

from fastapi_auth0 import Auth0User

from literature import database

from literature.user import set_global_user_id

from literature.schemas import ReferenceSchemaShow
from literature.schemas import ReferenceSchemaPost
from literature.schemas import ReferenceSchemaUpdate
from literature.schemas import FileSchemaShow

from literature.crud import reference_crud
from literature.crud import file_crud

from literature.routers.authentication import auth
from literature.deps import s3_auth
from literature.s3.upload import upload_file_to_bucket


router = APIRouter(
    prefix="/reference",
    tags=['Reference'])


get_db = database.get_db


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: ReferenceSchemaPost,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return reference_crud.create(db, request)


@router.delete('/{curie}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(curie: str,
            user: Auth0User = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    reference_crud.destroy(db, curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{curie}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=str,
              dependencies=[Depends(auth.implicit_scheme)])
async def patch(curie: str,
                request: ReferenceSchemaUpdate,
                user: Auth0User = Security(auth.get_user),
                db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)

    return reference_crud.patch(db, curie, patch)


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


@router.post('/{curie}/upload_file',
             status_code=status.HTTP_201_CREATED,
             response_model=FileSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
async def create_upload_file(curie: str,
                             file_obj: UploadFile = File(...),
                             s3: BaseClient = Depends(s3_auth),
                             user: Auth0User = Security(auth.get_user),
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
