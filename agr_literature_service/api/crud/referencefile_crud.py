import logging

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_utils import read_referencefile_db_obj_from_md5sum_or_id
from agr_literature_service.api.models import ReferencefileModel, ReferenceModel
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost
from agr_literature_service.api.schemas.response_message_schemas import messageEnum
from agr_literature_service.api.crud.referencefile_mod_utils import create as create_referencefile_mod

logger = logging.getLogger(__name__)


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


def show(db: Session, md5sum_or_referencefile_id: str):
    referencefile = read_referencefile_db_obj_from_md5sum_or_id(db, md5sum_or_referencefile_id)
    referencefile_dict = jsonable_encoder(referencefile)
    referencefile_dict["reference_curie"] = db.query(ReferenceModel.curie).filter(
        ReferenceModel.reference_id == referencefile_dict["reference_id"]).one()[0]
    del referencefile_dict["reference_id"]
    referencefile_dict["referencefile_mods"] = []
    if referencefile.referencefile_mods:
        for ref_file_mod in referencefile.referencefile_mods:
            ref_file_mod_dict = jsonable_encoder(ref_file_mod)
            del ref_file_mod_dict["mod_id"]
            if ref_file_mod.mod is not None:
                ref_file_mod_dict["mod_abbreviation"] = ref_file_mod.mod.abbreviation
            else:
                ref_file_mod_dict["mod_abbreviation"] = None
            referencefile_dict["referencefile_mods"].append(ref_file_mod_dict)
    return referencefile_dict


def patch(db: Session, md5sum_or_referencefile_id: str, request):
    referencefile = read_referencefile_db_obj_from_md5sum_or_id(db, md5sum_or_referencefile_id)
    if "reference_curie" in request:
        res = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == request.reference_curie).one_or_none()
        if res is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Reference with curie {request.reference_curie} is not available")

        request["reference_id"] = res[0]
        del request["reference_curie"]
    for field, value in request.items():
        setattr(referencefile, field, value)
    db.commit()
    return {"message": messageEnum.updated}


def destroy(db: Session, md5sum_or_referencefile_id: str):
    # TODO: delete from s3 through api call
    referencefile = read_referencefile_db_obj_from_md5sum_or_id(db, md5sum_or_referencefile_id)
    db.delete(referencefile)
    db.commit()
