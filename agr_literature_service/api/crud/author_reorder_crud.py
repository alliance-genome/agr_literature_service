from typing import List, Dict
from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferenceModel


def reorder_authors(db: Session, reference_curie: str, ordering: List[Dict]):
    """Renumber a reference's authors in ONE UPDATE statement so the per-statement
    uniqueness check on (reference_id, author_order) passes even for swaps."""
    ref_id = db.query(ReferenceModel.reference_id).filter(
        ReferenceModel.curie == reference_curie).scalar()
    if ref_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference {reference_curie} not found")
    if not ordering:
        return
    values = ", ".join(f"(:id{i}, :ord{i})" for i in range(len(ordering)))
    params: Dict = {"ref_id": ref_id}
    for i, item in enumerate(ordering):
        params[f"id{i}"] = item["author_id"]
        params[f"ord{i}"] = item["author_order"]
    # Defer the (reference_id, author_order) uniqueness check to COMMIT so the swap
    # can be done in ONE UPDATE without a transient mid-statement collision.
    db.execute(text("SET CONSTRAINTS uq_author_ref_order DEFERRED"))
    db.execute(text(
        f"UPDATE author AS a SET author_order = v.new_order "
        f"FROM (VALUES {values}) AS v(author_id, new_order) "
        f"WHERE a.author_id = v.author_id AND a.reference_id = :ref_id"), params)
    db.commit()
