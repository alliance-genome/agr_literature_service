import logging

from sqlalchemy import or_
from sqlalchemy.orm import Session
from fastapi import HTTPException, status

from agr_literature_service.api.models import ReferenceModel, ObsoleteReferenceModel

logger = logging.getLogger(__name__)


def get_merged(db: Session, curie):
    logger.debug("Looking up if '{}' is a merged entry".format(curie))
    # Is the curie in the merged set
    try:
        obs_ref_cur: ObsoleteReferenceModel = db.query(ObsoleteReferenceModel).filter(
            ObsoleteReferenceModel.curie == curie).one_or_none()
    except Exception:
        logger.debug("No merge data found so give error message")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")

    # If found in merge then get new reference.
    if obs_ref_cur:
        logger.debug("Merge found looking up the id '{}' instead now".format(obs_ref_cur.new_id))
    try:
        reference = db.query(ReferenceModel).filter(ReferenceModel.reference_id == obs_ref_cur.new_id).one_or_none()
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")
    return reference


def get_reference(db: Session, curie_or_reference_id: str):
    reference_id = int(curie_or_reference_id) if curie_or_reference_id.isdigit() else None
    reference = None
    try:
        reference = db.query(ReferenceModel).filter(or_(
            ReferenceModel.curie == curie_or_reference_id,
            ReferenceModel.reference_id == reference_id)).one()
    except Exception:
        if reference_id is None:
            reference = get_merged(db, curie_or_reference_id)
            logger.debug("Found from merged '{}'".format(reference))
    if not reference:
        logger.warning("Reference not found for {}?".format(curie_or_reference_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the reference_id or curie {curie_or_reference_id} is not available")
    return reference
