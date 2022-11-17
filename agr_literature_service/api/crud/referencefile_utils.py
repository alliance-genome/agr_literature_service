import logging
from os import environ

from fastapi import HTTPException, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferencefileModel, ReferenceModel, ModModel
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost
from agr_literature_service.api.crud.referencefile_mod_utils import create as create_referencefile_mod


logger = logging.getLogger(__name__)


def read_referencefile_db_obj(db: Session, referencefile_id: int):
    referencefile = db.query(ReferencefileModel).filter(
        ReferencefileModel.referencefile_id == referencefile_id).one_or_none()

    if not referencefile:
        logger.warning(f"Referencefile not found for referencefile id {referencefile_id}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Referencefile with referencefile_id {referencefile_id} is not available")
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
    if mod_abbreviation is not None:
        mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).one_or_none()
        if mod is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Mod with abbreviation {request.mod_abbreviation} does not exist")
    del request_dict["mod_abbreviation"]
    new_ref_file_obj = ReferencefileModel(**request_dict)
    db.add(new_ref_file_obj)
    db.commit()
    create_referencefile_mod(db, ReferencefileModSchemaPost(referencefile_id=new_ref_file_obj.referencefile_id,
                                                            mod_abbreviation=mod_abbreviation))
    return new_ref_file_obj.referencefile_id


def get_s3_folder_from_md5sum(md5sum: str):
    env_state = environ.get("ENV_STATE", "")
    if env_state == "" or env_state == "test":
        folder = "test"
    elif env_state == "prod":
        folder = "prod"
    else:
        folder = "develop"
    folder += "/reference/documents/"
    folder += "/".join([char for char in md5sum[0:4]])
    return folder
