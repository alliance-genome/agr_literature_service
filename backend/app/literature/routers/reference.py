from typing import List

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
from literature.schemas import FileSchemaUpload

from literature.crud import reference
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/reference",
    tags=['Reference']
)

import hashlib

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
             status_code=200,
             dependencies=[Depends(auth.implicit_scheme)])
async def create_upload_file(curie: str,
                             file: UploadFile = File(...),
                             user: Auth0User = Security(auth.get_user)):
   print(file.content_type)
   file_contents = await file.read()
   md5sum = hashlib.md5(file_contents).hexdigest()
   print(md5sum)
   return {'filename': file.filename}


@router.get('/{curie}/versions',
            status_code=200)
def show(curie: str):
    return reference.show_changesets(curie)
