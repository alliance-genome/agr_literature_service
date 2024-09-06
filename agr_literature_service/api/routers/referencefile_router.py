import json
import logging
from json import JSONDecodeError
from typing import Union, List

from fastapi import APIRouter, Depends, Security, status, File, UploadFile, HTTPException, Response
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import referencefile_crud
from agr_literature_service.api.deps import s3_auth
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.routers.okta_utils import get_okta_mod_access
from agr_literature_service.api.schemas import ResponseMessageSchema
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaShow, ReferencefileSchemaUpdate, \
    ReferencefileSchemaRelated, ReferenceFileAllMainPDFIdsSchemaPost
from agr_literature_service.api.user import set_global_user_from_okta

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
                file_extension: str = "",
                pdf_type: str = None,
                is_annotation: bool = None,
                mod_abbreviation: str = None,
                upload_if_already_converted: bool = False,
                file: UploadFile = File(...),  # noqa: B008
                metadata_file: Union[UploadFile, None] = File(default=None),  # noqa: B008
                user: OktaUser = db_user,
                db: Session = db_session):
    """

    Sample usage with curl

    - metadata provided as file

        metadata file json format:

            {
                "reference_curie": "AGRKB:101000000000001",
                "display_name": "test",
                "file_class": "main",
                "file_publication_status": "final",
                "file_extension": "txt",
                "pdf_type": null,
                "is_annotation": "false",
                "mod_abbreviation": "WB"
            }

        request:

            curl -X 'POST' 'http://localhost:8080/reference/referencefile/file_upload/' \\
             -H 'accept: application/json' \\
             -H 'Authorization: Bearer <okta_token>' \\
             -H 'Content-Type: multipart/form-data' \\
             -F 'file=@test2.txt;type=text/plain' \\
             -F 'metadata_file=@metadata_file.txt;type=text/plain'

    - metadata as url parameters

        request:

            curl -X 'POST' 'http://localhost:8080/reference/referencefile/file_upload/?reference_curie=AGRKB:101000000000001&display_name=test&file_class=main&file_publication_status=final&file_extension=txt&pdf_type=null&is_annotation=false' \\
             -H 'accept: application/json' \\
             -H 'Authorization: Bearer <okta_token>' \\
             -H 'Content-Type: multipart/form-data' \\
             -F 'file=@test2.txt;type=text/plain' \\
             -F 'metadata_file='

    """
    if is_annotation is None:
        is_annotation = False
    set_global_user_from_okta(db, user)
    metadata = None
    if reference_curie and display_name and file_class and file_publication_status:
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
            metadata["file_class"] or not metadata["file_publication_status"]:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="The provided metadata is not valid")
    return referencefile_crud.file_upload(db, metadata, file, upload_if_already_converted)


@router.get('/download_file/{referencefile_id}',
            status_code=status.HTTP_200_OK)
def download_file(referencefile_id: int,
                  user: OktaUser = db_user,
                  db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.download_file(db, referencefile_id, get_okta_mod_access(user))


@router.get('/additional_files_tarball/{reference_id}',
            status_code=status.HTTP_200_OK)
def download_additional_files_tarball(reference_id: int,
                                      user: OktaUser = db_user,
                                      db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.download_additional_files_tarball(db, reference_id, get_okta_mod_access(user))


@router.delete('/{referencefile_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def delete(referencefile_id: int,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    referencefile_crud.destroy(db, referencefile_id, get_okta_mod_access(user))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get('/{referencefile_id}',
            status_code=status.HTTP_200_OK,
            response_model=ReferencefileSchemaShow)
def show(referencefile_id: int,
         db: Session = db_session):
    return referencefile_crud.show(db, referencefile_id)


@router.get('/show_all/{curie_or_reference_id}',
            status_code=status.HTTP_200_OK,
            response_model=List[ReferencefileSchemaRelated])
def show_all(curie_or_reference_id: str,
             db: Session = db_session):
    return referencefile_crud.show_all(db, curie_or_reference_id)


@router.post('/show_main_pdf_ids_for_curies',
             status_code=status.HTTP_200_OK,
             response_model=dict)
def show_main_pdf_ids_for_curies(data: ReferenceFileAllMainPDFIdsSchemaPost,
                                 db: Session = db_session):
    return referencefile_crud.get_main_pdf_referencefile_ids_for_ref_curies_list(
        db=db, curies=data.curies, mod_abbreviation=data.mod_abbreviation)


@router.patch('/{referencefile_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch(referencefile_id: int,
          request: ReferencefileSchemaUpdate,
          user: OktaUser = db_user,
          db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.patch(db, referencefile_id, request.dict(exclude_unset=True))


@router.post('/merge/{curie_or_reference_id}/{losing_referencefile_id}/{winning_referencefile_id}',
             status_code=201)
def merge_referencefiles(curie_or_reference_id: str,
                         losing_referencefile_id: int,
                         winning_referencefile_id: int,
                         user: OktaUser = db_user,
                         db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.merge_referencefiles(db, curie_or_reference_id, losing_referencefile_id, winning_referencefile_id)
