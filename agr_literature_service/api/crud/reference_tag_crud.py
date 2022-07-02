"""
reference_tag_crud.py
===========================
"""
import logging
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferenceTagModel, ReferenceModel, ModModel
from agr_literature_service.api.schemas import ReferenceTagSchemaUpdate

logger = logging.getLogger(__name__)


def patch(db: Session, reference_tag: ReferenceTagSchemaUpdate) -> int:
    """
    Create a new reference_tag
    :param db:
    :param reference_tag:
    :return:
    """

    reference_tag_data = jsonable_encoder(reference_tag)
    logger.debug(reference_tag_data)
    reference_curie = reference_tag_data["reference_curie"]
    del reference_tag_data["reference_curie"]
    mod_abbreviation = reference_tag_data["mod_abbreviation"]
    del reference_tag_data["mod_abbreviation"]

    # Lookup the reference
    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {reference_curie} does not exist")

    # Lookup the mod
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Mod with abbreviation {mod_abbreviation} does not exist")

    # This presumes we can only have 1 tag, mod value.
    # May need to add to future tag table if this can have multiple values
    reference_tag_db_obj = db.query(ReferenceTagModel).filter(
        ReferenceTagModel.reference_id == reference.reference_id,
        ReferenceTagModel.tag_type == reference_tag_data["tag_type"],
        ReferenceTagModel.mod_id == mod.mod_id).one_or_none()

    # It already exists do just update the value
    if reference_tag_db_obj:
        reference_tag_db_obj.value = reference_tag_data["value"]
        return reference_tag_db_obj.reference_tag_id

    # New reference tag
    reference_tag_data["reference"] = reference
    reference_tag_data["mod"] = mod
    reference_tag_db_obj = ReferenceTagModel(**reference_tag_data)
    print(reference_tag_db_obj)
    db.add(reference_tag_db_obj)
    db.commit()

    return reference_tag_db_obj.reference_tag_id
