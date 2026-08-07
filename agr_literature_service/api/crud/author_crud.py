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


_AUTHOR_METADATA_FIELDS = ("name", "first_name", "last_name", "first_initial", "orcid", "affiliations")


def _coerce_person_only_metadata(author_data: dict):
    """For a person-only row (no author_order) coerce empty-string / empty-list
    metadata to ``None`` in place so it lands as NULL and satisfies
    ck_person_only_link_only, rather than surfacing a raw IntegrityError/500.

    A UI sending ``affiliations: []`` or ``name: ""`` to mean "no metadata" then
    creates a valid person-only stub. Only the string/array metadata fields are
    touched; ``first_author``/``corresponding_author`` are left as-is (a ``True``
    on a person-only row is real metadata and must still be rejected)."""
    if author_data.get("author_order") is not None:
        return
    for field in _AUTHOR_METADATA_FIELDS:
        value = author_data.get(field)
        if value == "" or value == []:
            author_data[field] = None


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


def _validate_author_constraints(author_data: dict, person_id, require_reference: bool = False):
    """Pre-validate the author-table CHECK / NOT NULL constraints and raise a clear 422
    instead of letting a raw IntegrityError surface as a 500.

    ``author_data`` is expected to have had ``person_curie`` already popped;
    ``person_id`` is the resolved person link (or ``None``). Set
    ``require_reference`` on the standalone POST /author path, where the row must
    carry a reference_curie (embedded-in-reference authors inherit the parent)."""
    # Normalize empty-string/empty-list metadata to NULL on a person-only row so a
    # UI sending e.g. ``affiliations: []`` or ``name: ""`` creates a valid stub
    # instead of tripping ck_person_only_link_only at INSERT (raw 500).
    _coerce_person_only_metadata(author_data)

    has_order = author_data.get("author_order") is not None
    has_person = person_id is not None

    # author.reference_id is NOT NULL: every author must belong to a reference.
    if require_reference and not author_data.get("reference_curie"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="An author must belong to a reference; supply reference_curie")

    # ck_author_person_or_order: person_id IS NOT NULL OR author_order IS NOT NULL.
    if not has_person and not has_order:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="An author must have an author_order or be linked to a person (person_curie)")

    # ck_person_only_link_only: with no author_order the row is a person-only link
    # and may not carry any author metadata.
    if not has_order:
        has_metadata = (any(author_data.get(f) for f in _AUTHOR_METADATA_FIELDS)
                        or bool(author_data.get("first_author"))
                        or bool(author_data.get("corresponding_author")))
        if has_metadata:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="A person-only link (no author_order) cannot carry author metadata "
                       "(name/first_name/last_name/first_initial/orcid/affiliations/"
                       "first_author/corresponding_author)")


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
    author_data.pop("person_id", None)  # never set person_id directly from the payload

    _validate_author_constraints(author_data, person_id, require_reference=True)

    # orcid = None
    # if "orcid" in author_data:
    #    orcid = author_data["orcid"]
    #    del author_data["orcid"]

    if "created_by" in author_data and author_data["created_by"] is not None:
        author_data["created_by"] = map_to_user_id(author_data["created_by"], db)
    if "updated_by" in author_data and author_data["updated_by"] is not None:
        author_data["updated_by"] = map_to_user_id(author_data["updated_by"], db)

    reference_curie = author_data.get("reference_curie")
    reference_id = db.query(ReferenceModel.reference_id).filter(
        ReferenceModel.curie == reference_curie).scalar() if reference_curie else None

    if person_id is not None:
        # Handle a pre-existing person link BEFORE inserting, and build the new row
        # already carrying person_id, so the row never flushes in a person-less /
        # order-less state that would violate ck_author_person_or_order.
        new_has_order = author_data.get("author_order") is not None
        existing = db.query(AuthorModel).filter(
            AuthorModel.reference_id == reference_id,
            AuthorModel.person_id == person_id,
        ).one_or_none()
        if existing is not None:
            if existing.author_order is not None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Person is already author #{existing.author_order} on this "
                           f"reference; unlink there first")
            if not new_has_order:
                # both the existing row and the new row are link-only stubs
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Person is already linked to this reference")
            # existing is a link-only stub and the new row is a real author -> absorb
            # the stub (per-statement uniqueness) then let the new row carry the person.
            db.delete(existing)
            db.flush()
        author_data["person_id"] = person_id

    # uq_author_ref_order is DEFERRABLE INITIALLY IMMEDIATE, so a duplicate
    # (reference_id, author_order) would otherwise surface as a raw IntegrityError/500
    # at flush/commit. Pre-check here and raise a clean 409, symmetric with the
    # (reference_id, person_id) handling above. (Done after the stub-absorb so a
    # freed order is honored; an absorbed stub is order-NULL and frees nothing.)
    new_order = author_data.get("author_order")
    if new_order is not None and reference_id is not None:
        order_taken = db.query(AuthorModel.author_id).filter(
            AuthorModel.reference_id == reference_id,
            AuthorModel.author_order == new_order,
        ).first()
        if order_taken is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"author_order {new_order} is already taken on this reference; "
                       f"use POST /author/reorder")

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

    if author_data.get("author_order") is not None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="author_order cannot be changed via PATCH; use POST /author/reorder")

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

    # A person-only row has author_order IS NULL, and PATCH cannot set author_order
    # (rejected above). Adding real metadata onto such a stub would violate
    # ck_person_only_link_only at commit -> raw 500. Coerce empty metadata to NULL
    # first (a no-op stub update stays a stub), then reject any real metadata as 422.
    if author_db_obj.author_order is None:
        _coerce_person_only_metadata(author_data)
        has_metadata = (any(author_data.get(f) is not None for f in _AUTHOR_METADATA_FIELDS)
                        or bool(author_data.get("first_author"))
                        or bool(author_data.get("corresponding_author")))
        if has_metadata:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cannot add author details to a person-only row; add or link this "
                       "person as an ordered author instead (which merges the stub)")

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
