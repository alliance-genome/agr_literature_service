import json
import logging
from json import JSONDecodeError
from typing import Union

from fastapi import APIRouter, Depends, Security, status, File, UploadFile, HTTPException, Response
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.deps import s3_auth
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import ResponseMessageSchema
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaShow, ReferencefileSchemaUpdate
from agr_literature_service.api.user import set_global_user_from_okta
from agr_literature_service.api.crud import referencefile_crud

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reference/referencefile",
    tags=['Reference'])


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)
s3_session = Depends(s3_auth)


@router.post('/file_upload/',
             status_code=status.HTTP_201_CREATED,
             response_model=str
             )
def file_upload(reference_curie: str = None,
                display_name: str = None,
                file_class: str = None,
                file_publication_status: str = None,
                file_extension: str = None,
                pdf_type: str = None,
                is_annotation: bool = False,
                mod_abbreviation: str = None,
                file: UploadFile = File(...),  # noqa: B008
                metadata_file: Union[UploadFile, None] = File(default=None),  # noqa: B008
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_from_okta(db, user)
    metadata = None
    if reference_curie and display_name and file_class and file_publication_status and file_extension:
        metadata = {
            "reference_curie": reference_curie,
            "display_name": display_name,
            "file_class": file_class,
            "file_publication_status": file_publication_status,
            "file_extension": file_extension,
            "pdf_type": pdf_type,
            "is_annotation": is_annotation,
            "mod_abbreviation": mod_abbreviation
        }
    elif metadata_file is not None:
        try:
            metadata = json.load(metadata_file.file)
        except JSONDecodeError:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="The provided metadata file is not a valid json file")
    if not metadata or not metadata["reference_curie"] or not metadata["display_name"] or not \
            metadata["file_class"] or not metadata["file_publication_status"] or not metadata["file_extension"]:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="The provided metadata is not valid")
    return referencefile_crud.file_upload(db, metadata, file)


@router.delete('/file_delete/{md5sum}',
               status_code=status.HTTP_204_NO_CONTENT)
def file_delete(md5sum: str,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_from_okta(db, user)
    referencefile_crud.destroy(db, md5sum)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get('/{md5sum_or_referencefile_id}',
            status_code=status.HTTP_200_OK,
            response_model=ReferencefileSchemaShow)
def show(md5sum_or_referencefile_id: str,
         db: Session = db_session):
    return referencefile_crud.show(db, md5sum_or_referencefile_id)


@router.patch('/{md5sum_or_referencefile_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch(md5sum_or_referencefile_id: str,
          request: ReferencefileSchemaUpdate,
          user: OktaUser = db_user,
          db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.patch(db, md5sum_or_referencefile_id, request.dict(exclude_unset=True))
