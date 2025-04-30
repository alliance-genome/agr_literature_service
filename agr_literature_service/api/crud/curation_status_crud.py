"""
curation_status_crud.py
=============
"""

from datetime import datetime


from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from sqlalchemy import text

from agr_literature_service.api.models import CurationStatusModel, ReferenceModel, ModModel
from agr_literature_service.api.schemas import CurationStatusSchemaPost


def create(db: Session, curation_status: CurationStatusSchemaPost) -> int:
    """

    :param db:
    :param curation_status:
    :return:
    """

    curation_status_data = jsonable_encoder(curation_status)
    reference_curie = curation_status_data.pop("reference_curie", None)
    if reference_curie is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="reference_curie not within curation_status_data")
    try:
        # get ref_id from curie
        ref_id = db.query(ReferenceModel).filter_by(curie=reference_curie).one().reference_id
        curation_status_data["reference_id"] = ref_id
        # look up mod
        abbreviation = curation_status_data.pop("mod_abbreviation", None)
        mod_id = db.query(ModModel).filter_by(abbreviation=abbreviation).one().mod_id
        curation_status_data["mod_id"] = mod_id
        curation_status_data["date_created"] = datetime.now().isoformat()
        db_obj = CurationStatusModel(**curation_status_data)
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
    except Exception as err:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Error creating curation_status: {err}")
    return db_obj.curation_status_id


def destroy(db: Session, curation_status_id: int) -> None:
    """

    :param db:
    :param curation_status_id:
    :return:
    """

    curation_status = db.query(CurationStatusModel).filter(CurationStatusModel.curation_status_id == curation_status_id).first()
    if not curation_status:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CurationStatus with curation_status_id {curation_status_id} not found")
    db.delete(curation_status)
    db.commit()

    return None


def patch(db: Session, curation_status_id: int, curation_status_update) -> dict:
    """

    :param db:
    :param curation_status_id:
    :param curation_status_update:
    :return:
    """

    curation_status_data = jsonable_encoder(curation_status_update)
    curation_status_db_obj = db.query(CurationStatusModel).filter(CurationStatusModel.curation_status_id == curation_status_id).first()
    if not curation_status_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CurationStatus with curation_status_id {curation_status_id} not found")

    for field, value in curation_status_data.items():
        setattr(curation_status_db_obj, field, value)

    curation_status_db_obj.dateUpdated = datetime.utcnow()
    db.add(curation_status_db_obj)
    db.commit()

    return {"message": "updated"}


def show(db: Session, curation_status_id: int) -> dict:
    """

    :param db:
    :param curation_status_id:
    :return:
    """

    curation_status = db.query(CurationStatusModel).filter(CurationStatusModel.curation_status_id == curation_status_id).first()
    curation_status_data = jsonable_encoder(curation_status)

    if not curation_status:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CurationStatus with the curation_status_id {curation_status_id} is not available")

    return curation_status_data


def list_by_ref_and_mod(db: Session, reference_curie: str, mod_abbr: str):
    try:
        # get ref_id from curie
        ref_id = db.query(ReferenceModel).filter_by(curie=reference_curie).one().reference_id
        # look up mod
        mod_id = db.query(ModModel).filter_by(abbreviation=mod_abbr).one().mod_id
    except Exception as err:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Error creating curation_status: {err}")
    # css = db.query(CurationStatusModel).filter_by(reference_id=ref_id, mod_id=mod_id).all()
    # TODO: we really need to get lost of possible topics and then
    #       create an empty dict for these and then fill in for those that exist
    query = f"SELECT * FROM curation_status WHERE mod_id = {mod_id} AND reference_id = {ref_id}"
    src_rows = db.execute(text(query)).mappings().fetchall()
    data = [dict(row) for row in src_rows]
    return {"data": data}
