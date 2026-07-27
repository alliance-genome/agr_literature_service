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


def get_vocabulary(name: str) -> List[Dict[str, Any]]:
    """Return the vocabulary's terms as uniform ``{value, label, is_obsolete}`` objects.

    The shape is the same regardless of source so the UI has one code path. For a
    static ``Literal`` vocabulary ``value == label`` (the string) and ``is_obsolete``
    is always ``False``. Table-backed vocabularies (added later) will carry the term
    id as ``value``, the term name as ``label``, and the real ``is_obsolete`` flag —
    and will return obsolete terms too, so the UI can resolve a stored id to its label
    while filtering obsolete terms out of the dropdown.
    """
    literal = _STATIC_VOCABULARIES.get(name)
    if literal is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown vocabulary '{name}'",
        )
    return [{"value": v, "label": v, "is_obsolete": False} for v in get_args(literal)]
