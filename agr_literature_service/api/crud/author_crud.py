"""
author_crud.py
==============
"""

from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.reference_resource import add, create_obj, stripout
from agr_literature_service.api.crud.user_utils import map_to_user_id
from agr_literature_service.api.models import (
    AuthorModel,
    PersonModel,
    ReferenceModel
)
from agr_literature_service.api.schemas import AuthorSchemaPost


def _resolve_person_curie(db: Session, author_data: dict):
    """Pop person_curie from the payload and return the resolved person_id (or None)."""
    curie = author_data.pop("person_curie", None)
    if not curie:
        return None
    person_id = db.query(PersonModel.person_id).filter(PersonModel.curie == curie).scalar()
    if person_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Person with curie {curie} not found")
    return person_id


def link_person(db: Session, author_db_obj: AuthorModel, person_id: int):
    """Set person_id on author_db_obj, merging/erroring per the uniqueness rules."""
    if person_id is None:
        return
    existing = db.query(AuthorModel).filter(
        AuthorModel.reference_id == author_db_obj.reference_id,
        AuthorModel.person_id == person_id,
        AuthorModel.author_id != author_db_obj.author_id,
    ).one_or_none()
    if existing is not None:
        if existing.author_order is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Person is already author #{existing.author_order} on this reference; "
                       f"unlink there first")
        # existing is a link-only stub -> delete it first (per-statement uniqueness), then link
        db.delete(existing)
        db.flush()
    author_db_obj.person_id = person_id


def create(db: Session, author: AuthorSchemaPost) -> AuthorModel:
    """
    Create a new author
    :param db:
    :param author:
    :return:
    """

    author_data = jsonable_encoder(author)

    person_id = _resolve_person_curie(db, author_data)
    if person_id is not None:
        author_data["person_id"] = person_id

    # orcid = None
    # if "orcid" in author_data:
    #    orcid = author_data["orcid"]
    #    del author_data["orcid"]

    if "created_by" in author_data and author_data["created_by"] is not None:
        author_data["created_by"] = map_to_user_id(author_data["created_by"], db)
    if "updated_by" in author_data and author_data["updated_by"] is not None:
        author_data["updated_by"] = map_to_user_id(author_data["updated_by"], db)

    author_model = create_obj(db, AuthorModel, author_data)  # type: AuthorModel

    db.add(author_model)
    db.commit()
    db.refresh(author_model)

    return author_model


def destroy(db: Session, author_id: int):
    """

    :param db:
    :param author_id:
    :return:
    """

    author = db.query(AuthorModel).filter(AuthorModel.author_id == author_id).first()
    if not author:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Author with author_id {author_id} not found")
    db.delete(author)
    db.commit()

    return None


def patch(db: Session, author_id: int, author_patch) -> AuthorModel:
    """
    Update an author
    :param db:
    :param author_id:
    :param author_patch:
    :return:
    """

    author_data = jsonable_encoder(author_patch)

    if "resource_curie" in author_data and author_data["resource_curie"] and \
            "reference_curie" in author_data and author_data["reference_curie"]:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Only supply either resource_curie or reference_curie")

    if "created_by" in author_data and author_data["created_by"] is not None:
        author_data["created_by"] = map_to_user_id(author_data["created_by"], db)
    if "updated_by" in author_data and author_data["updated_by"] is not None:
        author_data["updated_by"] = map_to_user_id(author_data["updated_by"], db)

    author_db_obj = db.query(AuthorModel).filter(AuthorModel.author_id == author_id).first()
    if not author_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Author with author_id {author_id} not found")
    res_ref = stripout(db, author_data, non_fatal=True)
    add(res_ref, author_db_obj)

    person_id = _resolve_person_curie(db, author_data)
    author_data.pop("person_id", None)  # never set person_id directly from the payload

    for field, value in author_data.items():
        setattr(author_db_obj, field, value)
    if person_id is not None:
        link_person(db, author_db_obj, person_id)

    author_db_obj.dateUpdated = datetime.utcnow()
    db.add(author_db_obj)
    db.commit()
    db.refresh(author_db_obj)

    return author_db_obj


def show(db: Session, author_id: int):
    """

    :param db:
    :param author_id:
    :return:
    """

    author = db.query(AuthorModel).filter(AuthorModel.author_id == author_id).first()
    author_data = jsonable_encoder(author)

    if not author:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Author with the author_id {author_id} is not available")

    if author_data["reference_id"]:
        author_data["reference_curie"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == author_data["reference_id"]).first()
    del author_data["reference_id"]
    del author_data["reference_curie"]
    author_data["person_id"] = author.person_id
    author_data["person_curie"] = (
        db.query(PersonModel.curie).filter(PersonModel.person_id == author.person_id).scalar()
        if author.person_id else None
    )
    return author_data


def show_changesets(db: Session, author_id: int):
    author = db.query(AuthorModel).filter(AuthorModel.author_id == author_id).first()
    if not author:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Author with the author_id {author_id} is not available")

    history = []
    for version in author.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
