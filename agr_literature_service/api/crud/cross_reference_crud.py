"""
cross_reference_crud.py
=======================
"""

from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.reference_resource import (add_reference_resource,
                                                                create_obj)
from agr_literature_service.api.models import (CrossReferenceModel, ReferenceModel,
                                               ResourceDescriptorModel, ResourceModel)


def set_curie_prefix(xref_db_obj: CrossReferenceModel):
    xref_db_obj.curie_prefix = xref_db_obj.curie.split(":")[0]


def create(db: Session, cross_reference) -> str:
    """

    :param db:
    :param cross_reference:
    :return:
    """

    cross_reference_data = jsonable_encoder(cross_reference)

    if db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == cross_reference_data["curie"]).first():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"CrossReference with curie {cross_reference_data['curie']} already exists")

    db_obj = create_obj(db, CrossReferenceModel, cross_reference_data)
    set_curie_prefix(db_obj)
    db.add(db_obj)
    db.commit()

    return "created"


def destroy(db: Session, curie: str) -> None:
    """
    Delete a CrossReference.
    :param db:
    :param curie:
    :return:
    """

    cross_reference = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} not found")
    db.delete(cross_reference)
    db.commit()

    return None


def patch(db: Session, curie: str, cross_reference_update) -> dict:
    """
    Update a CrossReference.
    :param db:
    :param curie:
    :param cross_reference_update:
    :return:
    """

    cross_reference_data = jsonable_encoder(cross_reference_update)
    cross_reference_db_obj = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} not found")
    add_reference_resource(db, cross_reference_db_obj, cross_reference_update, non_fatal=True)

    for field, value in cross_reference_data.items():
        setattr(cross_reference_db_obj, field, value)

    if "curie" in cross_reference_update:
        set_curie_prefix(cross_reference_db_obj)
    db.commit()

    return {"message": "updated"}


def show(db: Session, curie: str, indirect=True) -> dict:
    """
    Show a cross reference
    :param db:
    :param curie:
    :param indirect:
    :return:
    """

    cross_reference = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CrossReference with the curie {curie} is not available")

    cross_reference_data = jsonable_encoder(cross_reference)
    if cross_reference_data["resource_id"]:
        cross_reference_data["resource_curie"] = db.query(ResourceModel.curie).filter(ResourceModel.resource_id == cross_reference_data["resource_id"]).first().curie
    del cross_reference_data["resource_id"]

    if cross_reference_data["reference_id"]:
        cross_reference_data["reference_curie"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == cross_reference_data['reference_id']).first().curie
    del cross_reference_data["reference_id"]

    [db_prefix, local_id] = curie.split(":", 1)
    resource_descriptor = db.query(ResourceDescriptorModel).filter(ResourceDescriptorModel.db_prefix == db_prefix).first()
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


def show_changesets(db: Session, curie: str):
    """

    :param db:
    :param curie:
    :return:
    """

    cross_reference = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} is not available")

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
