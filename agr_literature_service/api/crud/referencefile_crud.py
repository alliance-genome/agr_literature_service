import logging

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import or_
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferencefileModel, ReferenceModel
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost, ReferencefileSchemaShow, \
    ReferencefileSchemaUpdate
from agr_literature_service.api.schemas.response_message_schemas import messageEnum

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
    new_ref_file_obj = ReferencefileModel(**request_dict)
    db.add(new_ref_file_obj)
    db.commit()
    return new_ref_file_obj.referencefile_id


def show(db: Session, md5sum_or_referencefile_id: str):
    referencefile = read_referencefile_db_obj_from_md5sum_or_id(db, md5sum_or_referencefile_id)
    referencefile_dict = jsonable_encoder(referencefile)
    referencefile_dict["reference_curie"] = db.query(ReferenceModel.curie).filter(
        ReferenceModel.reference_id == referencefile_dict["reference_id"]).one()[0]
    del referencefile_dict["reference_id"]

    return referencefile_dict


def patch(db: Session, md5sum_or_referencefile_id: str, request: ReferencefileSchemaUpdate):
    referencefile = read_referencefile_db_obj_from_md5sum_or_id(db, md5sum_or_referencefile_id)
    request_dict = dict(request)
    if "reference_curie" in request_dict:
        res = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == request.reference_curie).one_or_none()
        if res is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Reference with curie {request.reference_curie} is not available")

        request_dict["reference_id"] = res[0]
        del request_dict["reference_curie"]
    for field, value in request_dict.items():
        setattr(referencefile, field, value)
    db.commit()
    return {"message": messageEnum.updated}


def destroy(db: Session, md5sum_or_referencefile_id: str):
    referencefile = read_referencefile_db_obj_from_md5sum_or_id(db, md5sum_or_referencefile_id)
    db.delete(referencefile)
    db.commit()
