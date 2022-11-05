from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferencefileModAssociationModel


def read_referencefile_mod_obj_from_db(db: Session, referencefile_mod_id: int):
    referencefile_mod = db.query(ReferencefileModAssociationModel).filter(
        ReferencefileModAssociationModel.referencefile_mod_id == referencefile_mod_id).one_or_none()
    if referencefile_mod is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ReferencefileMod with referencefile_mod_id {str(referencefile_mod_id)} "
                                   f"is not avaliable")
    return referencefile_mod
