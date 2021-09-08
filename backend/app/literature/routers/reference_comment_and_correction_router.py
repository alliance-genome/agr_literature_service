from sqlalchemy.orm import Session

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_okta import OktaUser

from literature import database

from literature.user import set_global_user_id

from literature.schemas import ReferenceCommentAndCorrectionSchemaShow
from literature.schemas import ReferenceCommentAndCorrectionSchemaPost
from literature.schemas import ReferenceCommentAndCorrectionSchemaPatch
from literature.schemas import ResponseMessageSchema

from literature.crud import reference_comment_and_correction_crud
from literature.routers.authentication import auth


router = APIRouter(
    prefix="/reference_comment_and_correction",
    tags=['Reference Comment and Correction']
)


get_db = database.get_db


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: ReferenceCommentAndCorrectionSchemaPost,
           user: OktaUser = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return reference_comment_and_correction_crud.create(db, request)


@router.delete('/{reference_comment_and_correction_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(reference_comment_and_correction_id: int,
            user: OktaUser = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    reference_comment_and_correction_crud.destroy(db, reference_comment_and_correction_id)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{reference_comment_and_correction_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(reference_comment_and_correction_id: int,
                request: ReferenceCommentAndCorrectionSchemaPatch,
                user: OktaUser = Security(auth.get_user),
                db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)

    return reference_comment_and_correction_crud.patch(db, reference_comment_and_correction_id, patch)


@router.get('/{reference_comment_and_correction_id}',
            response_model=ReferenceCommentAndCorrectionSchemaShow,
            status_code=200)
def show(reference_comment_and_correction_id: int,
         db: Session = Depends(get_db)):
    return reference_comment_and_correction_crud.show(db, reference_comment_and_correction_id)


@router.get('/{reference_comment_and_correction_id}/versions',
            status_code=200)
def show(reference_comment_and_correction_id: int,
         db: Session = Depends(get_db)):
    return reference_comment_and_correction_crud.show_changesets(db, reference_comment_and_correction_id)
