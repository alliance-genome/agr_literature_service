"""
cross_reference_crud.py
=======================
"""
import os

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import or_, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.reference_resource import (add_reference_resource,
                                                                create_obj)
from agr_literature_service.api.models import (CrossReferenceModel, ReferenceModel,
                                               ResourceDescriptorModel, ResourceModel)


def set_curie_prefix(xref_db_obj: CrossReferenceModel):
    xref_db_obj.curie_prefix = xref_db_obj.curie.split(":")[0]


def get_cross_reference(db: Session, curie_or_id: str) -> CrossReferenceModel:
    cross_reference_id = int(curie_or_id) if curie_or_id.isdigit() else None
    cross_reference = db.query(CrossReferenceModel).filter(
        or_(CrossReferenceModel.curie == curie_or_id,
            CrossReferenceModel.cross_reference_id == cross_reference_id)).order_by(
        CrossReferenceModel.is_obsolete).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie or id {curie_or_id} not found")
    return cross_reference


def create(db: Session, cross_reference) -> int:
    cross_reference_data = jsonable_encoder(cross_reference)
    db_obj = create_obj(db, CrossReferenceModel, cross_reference_data)
    set_curie_prefix(db_obj)
    try:
        db.add(db_obj)
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Cannot add cross reference with curie {cross_reference_data['curie']}. "
                                   f"Error details: {str(e.orig.args[0])}")
    return db_obj.cross_reference_id


def destroy(db: Session, cross_reference_id: int) -> None:
    cross_reference = get_cross_reference(db, str(cross_reference_id))
    db.delete(cross_reference)
    db.commit()
    return None


def patch(db: Session, cross_reference_id: int, cross_reference_update) -> dict:
    cross_reference_data = jsonable_encoder(cross_reference_update)
    cross_reference_db_obj = get_cross_reference(db, str(cross_reference_id))
    add_reference_resource(db, cross_reference_db_obj, cross_reference_update, non_fatal=True)
    for field, value in cross_reference_data.items():
        setattr(cross_reference_db_obj, field, value)
    if "curie" in cross_reference_update:
        set_curie_prefix(cross_reference_db_obj)
    db.commit()
    return {"message": "updated"}


def show(db: Session, curie_or_cross_reference_id: str) -> dict:
    cross_reference = get_cross_reference(db, curie_or_cross_reference_id)
    cross_reference_data = jsonable_encoder(cross_reference)
    if cross_reference_data["resource_id"]:
        cross_reference_data["resource_curie"] = db.query(ResourceModel.curie).filter(
            ResourceModel.resource_id == cross_reference_data["resource_id"]).first().curie
    del cross_reference_data["resource_id"]

    if cross_reference_data["reference_id"]:
        cross_reference_data["reference_curie"] = db.query(ReferenceModel.curie).filter(
            ReferenceModel.reference_id == cross_reference_data['reference_id']).first().curie
    del cross_reference_data["reference_id"]

    [db_prefix, local_id] = cross_reference.curie.split(":", 1)
    resource_descriptor = db.query(ResourceDescriptorModel).filter(
        ResourceDescriptorModel.db_prefix == db_prefix).first()
    if resource_descriptor:
        default_url = resource_descriptor.default_url.replace("[%s]", local_id)
        cross_reference_data["url"] = default_url

        if cross_reference_data["pages"]:
            pages_data = []
            for cr_page in cross_reference_data["pages"]:
                page_url = ""
                for rd_page in resource_descriptor.pages:
                    if rd_page.name == cr_page:
                        page_url = rd_page.url
                        break
                pages_data.append({"name": cr_page,
                                   "url": page_url.replace("[%s]", local_id)})
            cross_reference_data["pages"] = pages_data
    elif cross_reference_data["pages"]:
        pages_data = []
        for cr_page in cross_reference_data["pages"]:
            pages_data.append({"name": cr_page})
        cross_reference_data["pages"] = pages_data

    return cross_reference_data


def check_xref_and_generate_mod_id(db: Session, reference_obj: ReferenceModel, mod_abbreviation: str):
    cross_reference = db.query(CrossReferenceModel).filter(
        and_(CrossReferenceModel.reference_id == reference_obj.reference_id,
             CrossReferenceModel.curie_prefix == mod_abbreviation)).order_by(
        CrossReferenceModel.is_obsolete).first()
    if not cross_reference:
        env_state = os.environ.get("ENV_STATE", "")
        if mod_abbreviation == 'WB' and env_state != "prod":
            cross_reference = db.query(CrossReferenceModel.curie).filter(
                and_(CrossReferenceModel.curie.startswith("WB:WBPaper0"),
                     CrossReferenceModel.curie_prefix == mod_abbreviation)).order_by(
                CrossReferenceModel.curie.desc()).first()
            new_wbpaper_number = int(cross_reference.curie[11:]) + 1
            new_wbpaper_string = str(new_wbpaper_number).zfill(8)
            new_wbpaper_curie = f"WB:WBPaper{new_wbpaper_string}"
            new_wbpaper_xref = {
                "curie": new_wbpaper_curie,
                "pages": [
                    "reference"
                ],
                "reference_curie": reference_obj.curie
            }
            create(db, new_wbpaper_xref)


def show_changesets(db: Session, cross_reference_id: int):
    cross_reference = get_cross_reference(db, str(cross_reference_id))
    history = []
    for version in cross_reference.versions:
        tx = version.transaction
        history.append(
            {
                "transaction": {
                    "id": tx.id,
                    "issued_at": tx.issued_at,
                    "user_id": tx.user_id,
                },
                "changeset": version.changeset,
            }
        )
    return history
