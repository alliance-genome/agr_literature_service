"""
vocabulary_crud.py
==================
Serves controlled-vocabulary value lists to the UI from a single source.

For short, stable app enums, the ABC API's own Pydantic ``Literal`` types are the
source of truth: this registry maps a public vocabulary name to the ``Literal`` that
already validates the field, so validation and the UI dropdown read the same definition
(no duplication, no DB table). Longer/curated vocabularies (e.g. lab_position,
person-person roles) will be served from the A-team-backed cache under the same
endpoint in a later phase.
"""
from typing import Any, Dict, List, get_args

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

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


def list_vocabularies() -> List[str]:
    """Return the known vocabulary names (sorted, stable)."""
    return sorted(_STATIC_VOCABULARIES)


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
    terms = db.query(VocabularyTermAbcModel).filter(
        VocabularyTermAbcModel.vocabulary_abc_id == vocab.vocabulary_abc_id
    ).all()
    out = []
    for t in terms:
        names = [t.name] + [s.synonym_name for s in t.synonyms]
        if any(needle in n.lower() for n in names):
            out.append({"value": t.vocabulary_term_abc_id, "label": t.name,
                        "is_obsolete": t.is_obsolete})
    return out
