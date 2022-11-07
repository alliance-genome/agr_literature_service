import logging

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_mod_utils import read_referencefile_mod_obj_from_db
from agr_literature_service.api.crud.referencefile_utils import read_referencefile_db_obj_from_md5sum_or_id
from agr_literature_service.api.models import ReferencefileModAssociationModel, ModModel, ReferencefileModel
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost
from agr_literature_service.api.schemas.response_message_schemas import messageEnum

logger = logging.getLogger(__name__)


def create(db: Session, request: ReferencefileModSchemaPost):
    if db.query(ReferencefileModel.referencefile_id).filter(
            ReferencefileModel.referencefile_id == request.referencefile_id).one_or_none() is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Referencefile with referencefile_id {str(request.referencefile_id)} "
                                   f"is not available")
    if request.mod_abbreviation:
        mod_id = db.query(ModModel.mod_id).filter(ModModel.abbreviation == request.mod_abbreviation).one_or_none()
        if mod_id is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Mod {request.mod_abbreviation} is not available")

        new_referencefile_mod = ReferencefileModAssociationModel(mod_id=mod_id[0], referencefile_id=request.referencefile_id)
    else:
        new_referencefile_mod = ReferencefileModAssociationModel(referencefile_id=request.referencefile_id)
    db.add(new_referencefile_mod)
    db.commit()
    return new_referencefile_mod.referencefile_mod_id


def show(db: Session, referencefile_mod_id):
    referencefile_mod = read_referencefile_mod_obj_from_db(db, referencefile_mod_id)
    mod_abbreviation = db.query(ModModel.abbreviation).filter(ModModel.mod_id == referencefile_mod.mod_id).one()[0]
    referencefile_mod_dict = jsonable_encoder(referencefile_mod)
    del referencefile_mod_dict["mod_id"]
    referencefile_mod_dict["mod_abbreviation"] = mod_abbreviation
    return referencefile_mod_dict


def patch(db: Session, referencefile_mod_id: int, request):
    referencefile_mod = read_referencefile_mod_obj_from_db(db, referencefile_mod_id)
    if "referencefile_id" in request:
        if read_referencefile_db_obj_from_md5sum_or_id(db, str(request["referencefile_id"])):
            referencefile_mod.referencefile_id = request["referencefile_id"]
    if "mod_abbreviation" in request:
        if request["mod_abbreviation"] is not None:
            mod = db.query(ModModel.mod_id).filter(ModModel.abbreviation == request["mod_abbreviation"]).one_or_none()
            if mod is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail=f"Mod {request['mod_abbreviation']} is not available")
            referencefile_mod.mod_id = mod[0]
        else:
            referencefile_mod.mod_id = None
    db.commit()
    return {"message": messageEnum.updated}


def destroy(db: Session, referencefile_mod_id: int):
    referencefile_mod = read_referencefile_mod_obj_from_db(db, referencefile_mod_id)
    db.delete(referencefile_mod)
    db.commit()
