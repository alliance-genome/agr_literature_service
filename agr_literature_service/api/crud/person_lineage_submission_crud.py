import logging
from typing import Any, Dict, List, Optional, Union

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from agr_literature_service.api.models import PersonLineageSubmissionModel
from agr_literature_service.api.crud import person_lineage_crud, person_crud
from agr_literature_service.api.crud import vocabulary_crud
from agr_literature_service.api.crud.vocabulary_crud import VocabularyTermRefSchema
from agr_literature_service.api.crud.vocabulary_seed_data import PERSON_LINEAGE_VOCAB
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)

_SCALAR_FIELDS = {
    "person_subject_name", "person_object_name", "relationship_vocabulary_term_abc_id",
    "who_sent_this", "start_date", "end_date", "status",
}
_NOT_NULL = {
    "person_subject_name", "person_object_name", "relationship_vocabulary_term_abc_id",
    "who_sent_this", "status",
}


def _resolve_person(db: Session, curie_or_id: Optional[Union[str, int]]) -> Optional[int]:
    """Resolve a curie-or-id person reference to its integer person_id, or None
    when not supplied. Raises 404 (via resolve_person_id) for an unknown person."""
    if curie_or_id is None or (isinstance(curie_or_id, str) and not curie_or_id.strip()):
        return None
    return person_crud.resolve_person_id(db, str(curie_or_id))


def _to_show_dict(db: Session, obj: PersonLineageSubmissionModel) -> Dict[str, Any]:
    return {
        "person_lineage_submission_id": obj.person_lineage_submission_id,
        "person_subject_name": obj.person_subject_name,
        "person_object_name": obj.person_object_name,
        "relationship": vocabulary_crud.serialize_term_ref(
            db, obj.relationship_vocabulary_term_abc_id),
        "who_sent_this": obj.who_sent_this,
        "person_subject_id": obj.person_subject_id,
        "person_subject_curie": obj.person_subject_curie,
        "person_object_id": obj.person_object_id,
        "person_object_curie": obj.person_object_curie,
        "start_date": obj.start_date,
        "end_date": obj.end_date,
        "status": obj.status,
        "person_lineage_id": obj.person_lineage_id,
        "date_created": obj.date_created,
        "date_updated": obj.date_updated,
        "created_by": obj.created_by,
        "updated_by": obj.updated_by,
    }


def create(db: Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    data = jsonable_encoder(payload)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    # The relationship is the incoming vocabulary_term_abc id; validate before storing.
    term_id = data["relationship"]
    vocabulary_crud.validate_term_id(db, PERSON_LINEAGE_VOCAB, term_id)

    obj = PersonLineageSubmissionModel(
        person_subject_name=data["person_subject_name"],
        person_object_name=data["person_object_name"],
        relationship_vocabulary_term_abc_id=term_id,
        who_sent_this=data["who_sent_this"],
        person_subject_id=_resolve_person(db, data.get("person_subject_curie_or_id")),
        person_object_id=_resolve_person(db, data.get("person_object_curie_or_id")),
        start_date=data.get("start_date"),
        end_date=data.get("end_date"),
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Database constraint violation; please verify input and retry.",
        )
    db.refresh(obj)
    return _to_show_dict(db, obj)


_PERSON_OBJS = (
    selectinload(PersonLineageSubmissionModel.person_subject_obj),
    selectinload(PersonLineageSubmissionModel.person_object_obj),
)


def _get_or_404(db: Session, person_lineage_submission_id: int) -> PersonLineageSubmissionModel:
    obj = (
        db.query(PersonLineageSubmissionModel)
        .options(*_PERSON_OBJS)
        .filter(PersonLineageSubmissionModel.person_lineage_submission_id == person_lineage_submission_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineageSubmission with id {person_lineage_submission_id} not found",
        )
    return obj


def show(db: Session, person_lineage_submission_id: int) -> Dict[str, Any]:
    return _to_show_dict(db, _get_or_404(db, person_lineage_submission_id))


def list_for_person(db: Session, person_id: int) -> List[Dict[str, Any]]:
    """All submissions in which the person is resolved on either side
    (person_subject_id or person_object_id)."""
    rows = (
        db.query(PersonLineageSubmissionModel)
        .options(*_PERSON_OBJS)
        .filter(
            or_(
                PersonLineageSubmissionModel.person_subject_id == person_id,
                PersonLineageSubmissionModel.person_object_id == person_id,
            )
        )
        .order_by(PersonLineageSubmissionModel.person_lineage_submission_id.asc())
        .all()
    )
    return [_to_show_dict(db, o) for o in rows]


def patch(db: Session, person_lineage_submission_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj: Optional[PersonLineageSubmissionModel] = (
        db.query(PersonLineageSubmissionModel)
        .filter(PersonLineageSubmissionModel.person_lineage_submission_id == person_lineage_submission_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineageSubmission with id {person_lineage_submission_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    # Resolve person links by curie-or-id (an explicit null clears the link).
    if "person_subject_curie_or_id" in data:
        obj.person_subject_id = _resolve_person(db, data["person_subject_curie_or_id"])
    if "person_object_curie_or_id" in data:
        obj.person_object_id = _resolve_person(db, data["person_object_curie_or_id"])

    # The relationship is the incoming vocabulary_term_abc id; validate before storing.
    # (The Update schema already rejects an explicit null for relationship.)
    if "relationship" in data and data["relationship"] is not None:
        term_id = data["relationship"]
        vocabulary_crud.validate_term_id(db, PERSON_LINEAGE_VOCAB, term_id)
        obj.relationship_vocabulary_term_abc_id = term_id

    for field, value in data.items():
        if field not in _SCALAR_FIELDS:
            continue
        if field in _NOT_NULL and value is None:
            continue
        setattr(obj, field, value)

    db.commit()
    return {"message": "updated"}


def destroy(db: Session, person_lineage_submission_id: int) -> None:
    obj = (
        db.query(PersonLineageSubmissionModel)
        .filter(PersonLineageSubmissionModel.person_lineage_submission_id == person_lineage_submission_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineageSubmission with id {person_lineage_submission_id} not found",
        )
    db.delete(obj)
    db.commit()


def validate(
    db: Session,
    person_lineage_submission_id: int,
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Promote a fully-resolved submission to a canonical person_lineage.

    Requires both person ids to be resolved. Finds or creates the canonical PPR
    for (person_subject_id, person_object_id, relationship), links the submission to it,
    and sets status to 'validated' (new canonical) or 'duplicate' (already existed).

    Curator overrides (one-shot promote): `overrides` may carry the people
    (`person_subject_curie_or_id`, `person_object_curie_or_id`), `relationship`,
    `start_date` and/or `end_date` used to build the canonical row instead of the
    submission's submitted values (the user's claim could be wrong). A key absent
    from `overrides` falls back to the submission's value; `relationship` is never
    taken as null. The submission row is never modified by validate — its names,
    relationship, dates and person-id links stay as submitted; only the canonical
    row uses the curated values, and later edits go to PATCH /person_lineage. (The
    submission's `status` and `person_lineage_id` link are the promotion outcome,
    not part of the claim, and are set below.)

    Guards:
      - a 'rejected' submission is refused (422) until its status is reset, so a
        rejection is never silently reversed;
      - an already-linked submission is an idempotent no-op (returned unchanged);
      - both people must resolve (from the override body or the submission).
    """
    overrides = overrides or {}
    obj = _get_or_404(db, person_lineage_submission_id)

    # A rejected submission must be deliberately un-rejected (its status reset)
    # before it can be validated — validate() won't silently reverse a rejection
    # (which could otherwise create or link a canonical row).
    if obj.status == "rejected":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="A rejected submission cannot be validated; reset its status first.",
        )

    # Idempotent: if the submission is already linked to a canonical PPR,
    # re-validating is a no-op — return it unchanged (don't re-link or flip
    # 'validated' to 'duplicate'). This also makes "reset status, validate again"
    # harmless without resurrecting a stale state.
    if obj.person_lineage_id is not None:
        return _to_show_dict(db, obj)

    # Resolve the canonical's people from the override body when supplied, else
    # fall back to the submission's stored links. Resolution does NOT write back to
    # the submission — only the canonical uses these ids. relationship/dates follow
    # the same fall-back rule; relationship must remain non-null (unique key).
    if "person_subject_curie_or_id" in overrides:
        subject_id = _resolve_person(db, overrides["person_subject_curie_or_id"])
    else:
        subject_id = obj.person_subject_id
    if "person_object_curie_or_id" in overrides:
        object_id = _resolve_person(db, overrides["person_object_curie_or_id"])
    else:
        object_id = obj.person_object_id

    if subject_id is None or object_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Both subject and object persons must be resolved (via the validate "
                   "body or the submission) before validating.",
        )

    # The relationship steering the canonical is a vocabulary_term_abc id (from the
    # override body or, falling back, the submission's stored term id). Validate it
    # before building the canonical.
    relationship_term_id = overrides.get("relationship") or obj.relationship_vocabulary_term_abc_id
    vocabulary_crud.validate_term_id(db, PERSON_LINEAGE_VOCAB, relationship_term_id)
    start_date = overrides["start_date"] if "start_date" in overrides else obj.start_date
    end_date = overrides["end_date"] if "end_date" in overrides else obj.end_date

    # Wrap find_or_create (which flushes a new canonical) through commit, so a
    # concurrent validation creating the same (subject, object, relationship) row
    # surfaces as a clean 422 rather than an uncaught flush-time IntegrityError.
    try:
        canonical, created = person_lineage_crud.find_or_create(
            db,
            person_subject_id=subject_id,
            person_object_id=object_id,
            relationship_term_id=relationship_term_id,
            start_date=start_date,
            end_date=end_date,
        )
        obj.person_lineage_id = canonical.person_lineage_id
        obj.status = "validated" if created else "duplicate"
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Database constraint violation; please verify input and retry.",
        )
    db.refresh(obj)
    return _to_show_dict(db, obj)


# --- Forward-reference resolution -------------------------------------------------
# person_lineage_submission_schemas declares its relationship field type
# (VocabularyTermRefSchema) under TYPE_CHECKING to avoid a schemas<->crud import
# cycle. VocabularyTermRefSchema is a name in this module's globals (imported above),
# so calling model_rebuild() here completes the read schemas. This module is imported
# by the person_lineage_submission router at startup, so the ref resolves before any
# request.
_ = VocabularyTermRefSchema  # keep the name bound for model_rebuild's namespace
from agr_literature_service.api.schemas.person_lineage_submission_schemas import (  # noqa: E402
    PersonLineageSubmissionSchemaShow,
    PersonLineageSubmissionSchemaRelated,
)

PersonLineageSubmissionSchemaShow.model_rebuild()
PersonLineageSubmissionSchemaRelated.model_rebuild()
