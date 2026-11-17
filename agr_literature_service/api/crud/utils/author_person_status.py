"""SCRUM-6827: the author-person curation status the sort page grants.

A paper sorted into the corpus with the author-person checkbox starts at
"complete" instead of "needed" when every ordered author already has a person --
including a paper with no authors. Read-only checks only: the automated status
changes (author reload, ORCID/email linker), which may switch off workflow_tag
history, live in lit_processing.data_ingest.utils.author_person_status_updates
and must never switch it off in the API process.
"""
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.utils.corpus_entry_tags import (
    AUTHOR_PERSON_CURATION_COMPLETE,
    AUTHOR_PERSON_CURATION_NEEDED,
)
from agr_literature_service.api.models import AuthorModel


def all_authors_connected(db: Session, reference_id: int) -> bool:
    """Every ordered author of the reference has a person. True for a reference
    with no ordered authors: there is nobody left to connect."""
    return db.query(AuthorModel.author_id).filter(
        AuthorModel.reference_id == reference_id,
        AuthorModel.author_order.isnot(None),
        AuthorModel.person_id.is_(None),
    ).first() is None


def author_person_entry_state(db: Session, reference_id: int) -> str:
    """The author-person state the sort page's checkbox moves a paper to from the
    author-person curation process term (SCRUM-6845): "needed", or "complete"
    when every ordered author already has a person (or there are none)."""
    if all_authors_connected(db, reference_id):
        return AUTHOR_PERSON_CURATION_COMPLETE
    return AUTHOR_PERSON_CURATION_NEEDED
