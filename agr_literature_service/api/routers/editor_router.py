from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import editor_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import EditorSchemaPost, ResponseMessageSchema, EditorSchemaCreate
from agr_literature_service.api.user import set_global_user_id

router = APIRouter(
    prefix="/editor",
    tags=['Editor']
)

get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: EditorSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_id(db, user.id)
    return editor_crud.create(db, request)


@router.delete('/{editor_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(editor_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_id(db, user.id)
    editor_crud.destroy(db, editor_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{editor_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(editor_id: int,
                request: EditorSchemaPost,
                user: OktaUser = db_user,
                db: Session = db_session):

    set_global_user_id(db, user.id)

    return editor_crud.patch(db, editor_id, EditorSchemaCreate(**request.dict()))


@router.get('/{editor_id}',
            status_code=200)
def show(editor_id: int,
         db: Session = db_session):
    return editor_crud.show(db, editor_id)


@router.get('/{editor_id}/versions',
            status_code=200)
def show_versions(editor_id: int,
                  db: Session = db_session):
    return editor_crud.show_changesets(db, editor_id)
