import logging

from fastapi import HTTPException, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferencefileModel


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
