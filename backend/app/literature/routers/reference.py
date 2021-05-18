from typing import List

from botocore.client import BaseClient

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security
from fastapi import File
from fastapi import UploadFile

from fastapi_auth0 import Auth0User

from literature.schemas import ReferenceSchemaShow
from literature.schemas import ReferenceSchemaPost
from literature.schemas import ReferenceSchemaUpdate
from literature.schemas import FileSchemaShow

from literature.crud import reference
from literature.crud import s3file

from literature.routers.authentication import auth
from literature.deps import s3_auth
from literature.s3.upload import upload_file_to_bucket


router = APIRouter(
    prefix="/reference",
    tags=['Reference'])


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=ReferenceSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: ReferenceSchemaPost,
           user: Auth0User = Security(auth.get_user)):
    return reference.create(request)


@router.delete('/{curie}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(curie: str,
            user: Auth0User = Security(auth.get_user)):
    reference.destroy(curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{curie}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=ReferenceSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(curie: str,
           request: ReferenceSchemaUpdate,
           user: Auth0User = Security(auth.get_user)):
    return reference.update(curie, request)


@router.get('/',
            response_model=List[ReferenceSchemaShow])
def all():
    return reference.get_all()


@router.get('/{curie}',
            status_code=200,
            response_model=ReferenceSchemaShow)
def show(curie: str):
    return reference.show(curie)


@router.post('/{curie}/upload_file',
             status_code=status.HTTP_201_CREATED,
             response_model=FileSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
async def create_upload_file(curie: str,
                             file_obj: UploadFile = File(...),
                             s3: BaseClient = Depends(s3_auth),
                             user: Auth0User = Security(auth.get_user)):
    file_contents = await file_obj.read()
    filename = file_obj.filename
    content_type = file_obj.content_type

    return s3file.create(s3, 'reference', curie, file_contents, filename, content_type)


@router.get('/{curie}/versions',
            status_code=200)
def show(curie: str):
    return reference.show_changesets(curie)
