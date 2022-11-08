import logging
from os import environ

from fastapi import HTTPException, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferencefileModel, ReferenceModel
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost
from agr_literature_service.api.crud.referencefile_mod_utils import create as create_referencefile_mod


logger = logging.getLogger(__name__)


def read_referencefile_db_obj_from_md5sum_or_id(db: Session, md5sum_or_referencefile_id: str):
    referencefile_id = int(md5sum_or_referencefile_id) if md5sum_or_referencefile_id.isdigit() else None
    referencefile = db.query(ReferencefileModel).filter(or_(
        ReferencefileModel.md5sum == md5sum_or_referencefile_id,
        ReferencefileModel.referencefile_id == referencefile_id)).one_or_none()

    if not referencefile:
        logger.warning(f"Referencefile not found for {md5sum_or_referencefile_id}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Referencefile with the referencefile_id or md5sum {md5sum_or_referencefile_id} "
                                   f"is not available")
    return referencefile


def create(db: Session, request: ReferencefileSchemaPost):
    request_dict = request.dict()
    ref_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == request.reference_curie).one_or_none()
    if ref_obj is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {request.reference_curie} does not exist")
    del request_dict["reference_curie"]
    request_dict["reference_id"] = ref_obj.reference_id
    mod_abbreviation = request_dict["mod_abbreviation"]
    del request_dict["mod_abbreviation"]
    new_ref_file_obj = ReferencefileModel(**request_dict)
    db.add(new_ref_file_obj)
    db.commit()
    create_referencefile_mod(db, ReferencefileModSchemaPost(referencefile_id=new_ref_file_obj.referencefile_id,
                                                            mod_abbreviation=mod_abbreviation))
    return new_ref_file_obj.referencefile_id


def get_s3_folder_from_md5sum(md5sum: str):
    if environ.get("ENV_STATE", "test") == "test":
        folder = "develop"
    else:
        folder = "prod"
    folder += "/reference/documents/"
    folder += "/".join([char for char in md5sum[0:4]])
    return folder
