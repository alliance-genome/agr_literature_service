from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import cross_reference_crud
from agr_literature_service.api.crud.utils import patterns_check
from agr_literature_service.api.schemas import (CrossReferenceSchemaPost,
                                                CrossReferenceSchemaUpdate,
                                                CrossReferenceSchemaShow)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user, read_auth_bypass
from agr_literature_service.api.util.resource_urls import cross_reference_url

router = APIRouter(
    prefix="/cross_reference",
    tags=['Cross Reference']
)

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=CrossReferenceSchemaShow)
def create(request: CrossReferenceSchemaPost,
           response: Response,
           user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
           db: Session = db_session):
    set_global_user_from_cognito(db, user)
    new_id = cross_reference_crud.create(db, request)
    response.headers["Location"] = cross_reference_url(new_id)
    return cross_reference_crud.show(db, str(new_id))


@router.delete('/{cross_reference_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(cross_reference_id: int,
            user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
            db: Session = db_session):
    set_global_user_from_cognito(db, user)
    cross_reference_crud.destroy(db, cross_reference_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{cross_reference_id}',
              status_code=status.HTTP_200_OK,
              response_model=CrossReferenceSchemaShow)
async def patch(cross_reference_id: int,
                request: CrossReferenceSchemaUpdate,
                user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                db: Session = db_session):
    set_global_user_from_cognito(db, user)
    patch = request.model_dump(exclude_unset=True)
    cross_reference_crud.patch(db, cross_reference_id, patch)
    return cross_reference_crud.show(db, str(cross_reference_id))


@router.get('/{cross_reference_id}/versions',
            status_code=200)
def show_version(cross_reference_id: int,
                 user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                 db: Session = db_session):
    return cross_reference_crud.show_changesets(db, cross_reference_id)


@router.post('/show_all',
             response_model=List[CrossReferenceSchemaShow],
             status_code=200)
@read_auth_bypass
def show_all(curies: List[str],
             user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
             db: Session = db_session):
    return cross_reference_crud.show_from_curies(db, curies)


@router.get('/check/patterns/{datatype}',
            status_code=200,
            )
def show_patterns_reference(datatype: str,
                            user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    # No db access needed, user param only for auth enforcement
    return patterns_check.get_patterns()[datatype]


@router.get('/check/curie/{datatype}/{curie}',
            status_code=200,
            )
def check_curie_reference_pattern(datatype: str,
                                  curie: str,
                                  user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    # No db access needed, user param only for auth enforcement
    ret = patterns_check.check_pattern(datatype, curie)
    if ret is None:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    return ret


@router.get('/{curie:path}',
            response_model=CrossReferenceSchemaShow,
            status_code=200)
def show(curie: str,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
         db: Session = db_session):
    return cross_reference_crud.show(db, curie)
