from typing import List

from sqlalchemy.orm import Session

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi.responses import StreamingResponse

from botocore.client import BaseClient

from fastapi_auth0 import Auth0User
#from literature.okta_auth0 import OktaUser
from fastapi_okta import OktaUser

from literature import database

from literature.user import set_global_user_id

from literature.schemas import FileSchemaShow
from literature.schemas import FileSchemaUpdate

from literature.deps import s3_auth

from literature.crud import file_crud

from literature.routers.authentication import auth


router = APIRouter(
    prefix="/file",
    tags=['File']
)


get_db = database.get_db


@router.delete('/{filename}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(filename: str,
            s3: BaseClient = Depends(s3_auth),
            user: OktaUser = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    s3file_crud.destroy(db, s3, filename)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{filename}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=FileSchemaShow)
async def patch(filename: str,
                request: FileSchemaUpdate,
                user: OktaUser = Security(auth.get_user),
                db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)

    return s3file_crud.update(db, filename, request)


@router.get('/{filename}',
            response_model=FileSchemaShow,
            status_code=200)
def show(filename: str,
         db: Session = Depends(get_db)):
    return s3file_crud.show(db, filename)


@router.get('/{filename}/download',
            status_code=200)
async def show(filename: str,
         s3: BaseClient = Depends(s3_auth),
         user: OktaUser = Security(auth.get_user),
         db: Session = Depends(get_db)):
   [file_stream, media_type] = s3file_crud.download(db, s3, filename)
   return StreamingResponse(file_stream, media_type=media_type)


@router.get('/{filename}/versions',
            status_code=200)
def show(filename: str,
         db: Session = Depends(get_db)):
    return s3file_crud.show_changesets(db, filename)
