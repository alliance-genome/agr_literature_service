import logging

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_mod_utils import (
    read_referencefile_mod_obj_from_db,
    resync_derived_embeddings,
)
from agr_literature_service.api.crud.referencefile_utils import read_referencefile_db_obj
from agr_literature_service.api.models import ModModel
from agr_literature_service.api.schemas.response_message_schemas import messageEnum
from agr_literature_service.api.crud.referencefile_mod_utils import create

logger = logging.getLogger(__name__)

create = create


def show(db: Session, referencefile_mod_id):
    referencefile_mod = read_referencefile_mod_obj_from_db(db, referencefile_mod_id)
    mod_abbreviation = db.query(ModModel.abbreviation).filter(ModModel.mod_id == referencefile_mod.mod_id).one()[0]
    referencefile_mod_dict = jsonable_encoder(referencefile_mod)
    del referencefile_mod_dict["mod_id"]
    referencefile_mod_dict["mod_abbreviation"] = mod_abbreviation
    return referencefile_mod_dict


def patch(db: Session, referencefile_mod_id: int, request):
    referencefile_mod = read_referencefile_mod_obj_from_db(db, referencefile_mod_id)
    old_referencefile_id = referencefile_mod.referencefile_id
    if "referencefile_id" in request:
        if read_referencefile_db_obj(db, request["referencefile_id"]):
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
    # Access changed: propagate to embedding parquets derived from the
    # affected referencefile(s) — both files when the association moved.
    resync_derived_embeddings(db, old_referencefile_id)
    if referencefile_mod.referencefile_id != old_referencefile_id:
        resync_derived_embeddings(db, referencefile_mod.referencefile_id)
    return {"message": messageEnum.updated}
