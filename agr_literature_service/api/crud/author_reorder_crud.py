from typing import List, Dict, Any
from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferenceModel, AuthorModel


def reorder_authors(db: Session, reference_curie: str, ordering: List[Any]):
    """Renumber a reference's authors in ONE UPDATE statement so the per-statement
    uniqueness check on (reference_id, author_order) passes even for swaps.

    ``ordering`` is a list of items exposing ``author_id`` and ``author_order``
    (Pydantic ``AuthorOrderItem`` from the route)."""
    ref_id = db.query(ReferenceModel.reference_id).filter(
        ReferenceModel.curie == reference_curie).scalar()
    if ref_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference {reference_curie} not found")
    if not ordering:
        return

    author_ids = [item.author_id for item in ordering]
    author_orders = [item.author_order for item in ordering]

    # a duplicate author_id in the payload makes the single UPDATE's winner
    # nondeterministic; reject it up front.
    if len(set(author_ids)) != len(author_ids):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="author_id values must be unique within the reorder request")

    # every author_id must belong to this reference; a foreign/nonexistent id would
    # otherwise be silently skipped by the WHERE guard yet still return 200.
    owned_ids = {
        aid for (aid,) in db.query(AuthorModel.author_id).filter(
            AuthorModel.reference_id == ref_id,
            AuthorModel.author_id.in_(author_ids)).all()
    }
    unknown = [aid for aid in author_ids if aid not in owned_ids]
    if unknown:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"author_id(s) {unknown} do not belong to reference {reference_curie}")

    # target author_order values must be unique within the payload; duplicates would
    # otherwise blow up as a deferred-constraint IntegrityError (500) at COMMIT.
    if len(set(author_orders)) != len(author_orders):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="author_order values must be unique within the reorder request")

    # target orders must not collide with ordered authors of this reference that are
    # ABSENT from the payload: those rows keep their current order, so an overlap
    # would trip the deferred uq_author_ref_order at COMMIT (500). Require the request
    # to renumber those rows too.
    non_payload_orders = {
        order for (order,) in db.query(AuthorModel.author_order).filter(
            AuthorModel.reference_id == ref_id,
            AuthorModel.author_order.isnot(None),
            AuthorModel.author_id.notin_(author_ids)).all()
    }
    colliding = sorted(set(author_orders) & non_payload_orders)
    if colliding:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"author_order value(s) {colliding} collide with authors of "
                   f"reference {reference_curie} not included in the reorder request; "
                   f"include every author of the reference")

    values = ", ".join(f"(:id{i}, :ord{i})" for i in range(len(ordering)))
    params: Dict = {"ref_id": ref_id}
    for i, item in enumerate(ordering):
        params[f"id{i}"] = item.author_id
        params[f"ord{i}"] = item.author_order
    # Defer the (reference_id, author_order) uniqueness check to COMMIT so the swap
    # can be done in ONE UPDATE without a transient mid-statement collision.
    db.execute(text("SET CONSTRAINTS uq_author_ref_order DEFERRED"))
    db.execute(text(
        f"UPDATE author AS a SET author_order = v.new_order "
        f"FROM (VALUES {values}) AS v(author_id, new_order) "
        f"WHERE a.author_id = v.author_id AND a.reference_id = :ref_id"), params)
    db.commit()
