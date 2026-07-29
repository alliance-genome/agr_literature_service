"""
vocabulary_crud.py
==================
Serves controlled-vocabulary value lists to the UI from a single source.

For short, stable app enums, the ABC API's own Pydantic ``Literal`` types are the
source of truth: this registry maps a public vocabulary name to the ``Literal`` that
already validates the field, so validation and the UI dropdown read the same definition
(no duplication, no DB table). Longer curated vocabularies (lab_position,
person_person_relationship) are served from the ``vocabulary_abc`` tables under the
same endpoint.
"""
from typing import Any, Dict, List, Optional, get_args

from fastapi import HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session, selectinload

from agr_literature_service.api.schemas.person_schemas import ActiveStatus, Privacy
from agr_literature_service.api.schemas.laboratory_schemas import (
    LaboratoryStatus, EmailVisibility
)

# public vocabulary name -> the static Literal that defines/validates it
_STATIC_VOCABULARIES: Dict[str, Any] = {
    "person_active_status": ActiveStatus,
    "person_privacy": Privacy,
    "laboratory_status": LaboratoryStatus,
    "laboratory_email_visibility": EmailVisibility,
}


def list_vocabularies(db: Session) -> List[str]:
    """Return all known vocabulary names (sorted, stable): the static ``Literal``
    vocabularies plus every table-backed vocabulary registered in ``vocabulary_abc``."""
    from agr_literature_service.api.models import VocabularyAbcModel
    table_backed = {name for (name,) in db.query(VocabularyAbcModel.vocabulary).all()}
    return sorted(set(_STATIC_VOCABULARIES) | table_backed)


def get_vocabulary(db: Session, name: str) -> List[Dict[str, Any]]:
    """Return the vocabulary's terms as uniform ``{value, label, is_obsolete}`` objects.

    The shape is the same regardless of source so the UI has one code path. For a
    static ``Literal`` vocabulary ``value == label`` (the string) and ``is_obsolete``
    is always ``False`` (the DB is never touched, so ``db`` may be ``None``).
    Table-backed vocabularies carry the term id as ``value``, the term name as
    ``label``, and the real ``is_obsolete`` flag — and return obsolete terms too, so
    the UI can resolve a stored id to its label while filtering obsolete terms out of
    the dropdown.
    """
    literal = _STATIC_VOCABULARIES.get(name)
    if literal is not None:
        return [{"value": v, "label": v, "is_obsolete": False} for v in get_args(literal)]

    # Imported lazily so the static-enum path stays free of the ORM/DB import chain.
    from agr_literature_service.api.models import (
        VocabularyAbcModel, VocabularyTermAbcModel
    )
    vocab = db.query(VocabularyAbcModel).filter(
        VocabularyAbcModel.vocabulary == name
    ).first()
    if vocab is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown vocabulary '{name}'",
        )
    terms = db.query(VocabularyTermAbcModel).filter(
        VocabularyTermAbcModel.vocabulary_abc_id == vocab.vocabulary_abc_id
    ).order_by(VocabularyTermAbcModel.name).all()
    return [{"value": t.vocabulary_term_abc_id, "label": t.name, "is_obsolete": t.is_obsolete}
            for t in terms]


def search_vocabulary(db: Session, name: str, q: str) -> List[Dict[str, Any]]:
    """Autocomplete over term names + synonyms; returns matching canonical terms."""
    from agr_literature_service.api.models import (
        VocabularyAbcModel, VocabularyTermAbcModel
    )
    vocab = db.query(VocabularyAbcModel).filter(
        VocabularyAbcModel.vocabulary == name
    ).first()
    if vocab is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown vocabulary '{name}'",
        )
    needle = q.strip().lower()
    terms = db.query(VocabularyTermAbcModel).options(
        selectinload(VocabularyTermAbcModel.synonyms)
    ).filter(
        VocabularyTermAbcModel.vocabulary_abc_id == vocab.vocabulary_abc_id
    ).all()
    out = []
    for t in terms:
        names = [t.name] + [s.synonym_name for s in t.synonyms]
        if any(needle in n.lower() for n in names):
            out.append({"value": t.vocabulary_term_abc_id, "label": t.name,
                        "is_obsolete": t.is_obsolete})
    return out


class VocabularyTermRefSchema(BaseModel):
    value: int
    label: str
    is_obsolete: bool


def serialize_term_ref(db: Session, term_id: Optional[int]) -> Optional[Dict[str, Any]]:
    """Expand a stored vocabulary_term_abc id to the uniform {value,label,is_obsolete}
    object the UI consumes, or None when unset."""
    if term_id is None:
        return None
    from agr_literature_service.api.models import VocabularyTermAbcModel
    term = db.query(VocabularyTermAbcModel).filter(
        VocabularyTermAbcModel.vocabulary_term_abc_id == term_id
    ).first()
    if term is None:
        return None
    return {"value": term.vocabulary_term_abc_id, "label": term.name,
            "is_obsolete": term.is_obsolete}


def validate_term_id(db: Session, vocabulary_key: str, term_id: int) -> None:
    """Fail-closed: raise 422 unless term_id is a non-obsolete term of the named
    vocabulary."""
    from agr_literature_service.api.models import (
        VocabularyAbcModel, VocabularyTermAbcModel
    )
    term = (
        db.query(VocabularyTermAbcModel)
        .join(VocabularyAbcModel,
              VocabularyTermAbcModel.vocabulary_abc_id == VocabularyAbcModel.vocabulary_abc_id)
        .filter(
            VocabularyTermAbcModel.vocabulary_term_abc_id == term_id,
            VocabularyAbcModel.vocabulary == vocabulary_key,
        )
        .first()
    )
    if term is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown or wrong-vocabulary term id {term_id} for '{vocabulary_key}'",
        )
    if term.is_obsolete:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Term id {term_id} is obsolete and cannot be assigned",
        )
