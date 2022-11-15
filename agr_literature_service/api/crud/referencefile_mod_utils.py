from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferencefileModAssociationModel, ReferencefileModel, ModModel
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost


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


def read_referencefile_mod_obj_from_db(db: Session, referencefile_mod_id: int):
    referencefile_mod = db.query(ReferencefileModAssociationModel).filter(
        ReferencefileModAssociationModel.referencefile_mod_id == referencefile_mod_id).one_or_none()
    if referencefile_mod is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ReferencefileMod with referencefile_mod_id {str(referencefile_mod_id)} "
                                   f"is not avaliable")
    return referencefile_mod
