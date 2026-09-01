import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from agr_literature_service.api.models import PersonLineageModel, PersonLineageSubmissionModel
from agr_literature_service.api.crud import person_crud
from agr_literature_service.api.crud import vocabulary_crud
from agr_literature_service.api.crud.vocabulary_seed_data import (
    PERSON_LINEAGE_VOCAB, SYMMETRIC_RELATIONSHIP_NAMES,
)
from agr_literature_service.api.crud.user_utils import map_to_user_id
from agr_literature_service.api.models import VocabularyTermAbcModel

logger = logging.getLogger(__name__)

# The relationship FK is handled explicitly in patch() (validate_term_id before
# storing) and is deliberately NOT listed here: keeping it out of the generic
# copy-through loop means the raw FK can never be written unvalidated, even if the
# Update schema were ever widened to expose it.
_SCALAR_FIELDS = {"start_date", "end_date"}


def _is_symmetric_term(db: Session, term_id: int) -> bool:
    """A relationship term is non-directional when its term NAME is in the
    seed-data symmetric set (e.g. "Collaborator of").

    The lookup runs under ``no_autoflush`` so it never flushes a caller's pending
    (dirty) row: in ``patch`` the person/relationship edits are already staged, and
    an autoflush here would raise the unique-constraint IntegrityError outside the
    commit's try/except. Deferring the flush to the explicit commit keeps the
    violation catchable (surfaced as a clean 422).
    """
    with db.no_autoflush:
        term = db.query(VocabularyTermAbcModel).filter(
            VocabularyTermAbcModel.vocabulary_term_abc_id == term_id
        ).first()
    return bool(term and term.name in SYMMETRIC_RELATIONSHIP_NAMES)


def _normalize_pair(
    db: Session, person_subject_id: int, person_object_id: int, relationship_term_id: int
) -> Tuple[int, int]:
    """For non-directional relationships, return the pair in ascending id order so
    (A, B) and (B, A) collapse to the same canonical row. Directional relationships
    keep the submitted order.
    """
    if _is_symmetric_term(db, relationship_term_id) and person_subject_id > person_object_id:
        return person_object_id, person_subject_id
    return person_subject_id, person_object_id


def _reject_self_pair(person_subject_id: int, person_object_id: int) -> None:
    if person_subject_id == person_object_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="person_subject_id and person_object_id must be different people",
        )


def _to_show_dict(db: Session, obj: PersonLineageModel) -> Dict[str, Any]:
    return {
        "person_lineage_id": obj.person_lineage_id,
        "person_subject_id": obj.person_subject_id,
        "person_subject_curie": obj.person_subject_curie,
        "person_subject_name": obj.person_subject_name,
        "person_object_id": obj.person_object_id,
        "person_object_curie": obj.person_object_curie,
        "person_object_name": obj.person_object_name,
        "relationship": vocabulary_crud.serialize_term_ref(
            db, obj.relationship_vocab_term_abc_id),
        "start_date": obj.start_date,
        "end_date": obj.end_date,
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

    term_id = data["relationship"]
    vocabulary_crud.validate_term_id(db, PERSON_LINEAGE_VOCAB, term_id)

    # Both people are required; given by curie OR integer id (404 if unknown).
    subject_id = person_crud.resolve_person_id(db, str(data["person_subject_curie_or_id"]))
    object_id = person_crud.resolve_person_id(db, str(data["person_object_curie_or_id"]))
    _reject_self_pair(subject_id, object_id)

    one_id, two_id = _normalize_pair(db, subject_id, object_id, term_id)
    obj = PersonLineageModel(
        person_subject_id=one_id,
        person_object_id=two_id,
        relationship_vocab_term_abc_id=term_id,
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
            detail=(
                "A person_lineage with this person_subject_id, person_object_id and "
                "relationship already exists."
            ),
        )
    db.refresh(obj)
    return _to_show_dict(db, obj)


def find_or_create(
    db: Session,
    person_subject_id: int,
    person_object_id: int,
    relationship_term_id: int,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Tuple[PersonLineageModel, bool]:
    """Return (canonical, created). Looks up an existing canonical PPR for the
    (person_subject_id, person_object_id, relationship_term_id) triple; creates one
    if absent. For non-directional relationships the pair is normalized to ascending
    id order first, so a reversed submission matches the existing row.
    """
    _reject_self_pair(person_subject_id, person_object_id)
    person_subject_id, person_object_id = _normalize_pair(
        db, person_subject_id, person_object_id, relationship_term_id
    )
    existing = (
        db.query(PersonLineageModel)
        .filter(
            PersonLineageModel.person_subject_id == person_subject_id,
            PersonLineageModel.person_object_id == person_object_id,
            PersonLineageModel.relationship_vocab_term_abc_id == relationship_term_id,
        )
        .first()
    )
    if existing:
        return existing, False

    obj = PersonLineageModel(
        person_subject_id=person_subject_id,
        person_object_id=person_object_id,
        relationship_vocab_term_abc_id=relationship_term_id,
        start_date=start_date,
        end_date=end_date,
    )
    db.add(obj)
    db.flush()
    return obj, True


_PERSON_OBJS = (
    selectinload(PersonLineageModel.person_subject_obj),
    selectinload(PersonLineageModel.person_object_obj),
)


def show(db: Session, person_lineage_id: int) -> Dict[str, Any]:
    obj = (
        db.query(PersonLineageModel)
        .options(*_PERSON_OBJS)
        .filter(PersonLineageModel.person_lineage_id == person_lineage_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineage with id {person_lineage_id} not found",
        )
    return _to_show_dict(db, obj)


def list_for_person(db: Session, person_id: int) -> List[Dict[str, Any]]:
    """All canonical PPRs in which the person appears, on either side
    (person_subject_id or person_object_id)."""
    rows = (
        db.query(PersonLineageModel)
        .options(*_PERSON_OBJS)
        .filter(
            or_(
                PersonLineageModel.person_subject_id == person_id,
                PersonLineageModel.person_object_id == person_id,
            )
        )
        .order_by(PersonLineageModel.person_lineage_id.asc())
        .all()
    )
    return [_to_show_dict(db, o) for o in rows]


def patch(db: Session, person_lineage_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj: Optional[PersonLineageModel] = (
        db.query(PersonLineageModel)
        .filter(PersonLineageModel.person_lineage_id == person_lineage_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineage with id {person_lineage_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    # Everything from the first attribute mutation up to the commit runs under
    # no_autoflush: once obj is dirtied, any intervening query (validate_term_id,
    # the _normalize_pair term lookup, ...) would otherwise autoflush the dirty row
    # and surface a unique-constraint IntegrityError BEFORE the commit's try/except,
    # turning the intended 422 into an uncaught 500. Deferring the only flush to the
    # explicit commit keeps every constraint violation catchable.
    with db.no_autoflush:
        # Person corrections: resolve curie-or-id and repoint the canonical. The
        # submission link (person_lineage_id) is independent of which persons the
        # canonical references, so any linked submissions stay attached — their name
        # claim is now simply resolved to the corrected person.
        if "person_subject_curie_or_id" in data and data["person_subject_curie_or_id"] is not None:
            obj.person_subject_id = person_crud.resolve_person_id(db, str(data["person_subject_curie_or_id"]))
        if "person_object_curie_or_id" in data and data["person_object_curie_or_id"] is not None:
            obj.person_object_id = person_crud.resolve_person_id(db, str(data["person_object_curie_or_id"]))
        _reject_self_pair(obj.person_subject_id, obj.person_object_id)

        # The relationship is the incoming vocabulary_term_abc id; validate before storing.
        if "relationship" in data and data["relationship"] is not None:
            term_id = data["relationship"]
            vocabulary_crud.validate_term_id(db, PERSON_LINEAGE_VOCAB, term_id)
            obj.relationship_vocab_term_abc_id = term_id

        for field, value in data.items():
            if field not in _SCALAR_FIELDS:
                continue
            setattr(obj, field, value)

        # If the (possibly updated) relationship is non-directional, re-normalize the
        # id order so a row patched into a symmetric relationship can't become a reversed
        # duplicate of an existing row for the same pair.
        obj.person_subject_id, obj.person_object_id = _normalize_pair(
            db, obj.person_subject_id, obj.person_object_id, obj.relationship_vocab_term_abc_id
        )

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Database constraint violation; please verify input and retry.",
        )
    return {"message": "updated"}


def destroy(db: Session, person_lineage_id: int) -> None:
    obj = (
        db.query(PersonLineageModel)
        .filter(PersonLineageModel.person_lineage_id == person_lineage_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineage with id {person_lineage_id} not found",
        )
    # Submissions promoted to this canonical have person_lineage_id cleared by the
    # FK's ON DELETE SET NULL, but their status would stay 'validated'/'duplicate'.
    # Reset those back to 'pending' so they return to the unvalidated pool cleanly
    # and can be re-validated.
    db.query(PersonLineageSubmissionModel).filter(
        PersonLineageSubmissionModel.person_lineage_id == person_lineage_id,
        PersonLineageSubmissionModel.status.in_(["validated", "duplicate"]),
    ).update({"status": "pending"}, synchronize_session=False)
    db.delete(obj)
    db.commit()


# --- Forward-reference resolution -------------------------------------------------
# person_lineage_schemas declares its relationship field type (VocabularyTermRefSchema)
# under TYPE_CHECKING to avoid a schemas<->crud import cycle. VocabularyTermRefSchema is
# a name in this module's globals (imported via vocabulary_crud below), so calling
# model_rebuild() here completes the read schema. This module is imported by the
# person_lineage router at startup, so the ref resolves before any request.
from agr_literature_service.api.crud.vocabulary_crud import VocabularyTermRefSchema  # noqa: E402,F401
from agr_literature_service.api.schemas.person_lineage_schemas import (  # noqa: E402
    PersonLineageSchemaShow,
)

PersonLineageSchemaShow.model_rebuild()
