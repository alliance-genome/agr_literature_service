"""SCRUM-6827: keep a reference's author-person curation status in step with what
automation does to its authors.

The four states are siblings under "author-person curation" (ATP:0000375). A
curator moves a paper between them on the person page; the automated paths here
only ever move a paper between "needed" and "complete":

* the PubMed/DQM author reload (``update_authors``), which only reloads a paper
  no curator has touched: "complete" goes back to "needed", since the reloaded
  authors have no persons -- or to "complete", from any state, when the reloaded
  author list is empty (``apply_author_reload_statuses``);
* the ORCID/email linker: a paper it linked whose ordered authors now all have
  a person goes to "complete", from any state (``complete_when_all_connected``).

(The sort page's starting status is api.crud.utils.author_person_status.)

Status changes go through the transition table (``transition_to_workflow_status``,
as an automated transition), so the WB transitions must be of type "any". They
write no history unless a curator has made an author-person connection on the
paper (or the caller is a curator action): automation keeps only the latest state
of a paper only automation has worked on.

History is switched off for the workflow_tag table alone, through its
SQLAlchemy-Continuum ``__versioned__['versioning']`` option; every other table
keeps its history. The option is a class attribute, so while it is off no session
or thread in the process writes workflow_tag history: this is for the
single-threaded ingest scripts only, never for the API process. A caller running
inside the API (the reference merge) must pass ``record_history=True``, which
leaves the switch alone.
"""
import logging
from contextlib import contextmanager, nullcontext
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from sqlalchemy import or_
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.utils.author_person_status import all_authors_connected
from agr_literature_service.api.crud.utils.corpus_entry_tags import (
    AUTHOR_PERSON_CURATION_COMPLETE,
    AUTHOR_PERSON_CURATION_NEEDED,
    AUTHOR_PERSON_CURATION_STATES,
)
from agr_literature_service.api.models import (
    AuthorModel,
    ModCorpusAssociationModel,
    ModModel,
    UserModel,
    WorkflowTagModel,
)

logger = logging.getLogger(__name__)

# session.info key: reference_ids whose authors update_authors dropped and reloaded
# in this session, waiting for the caller's commit (apply_author_reload_statuses).
RELOADED_REFERENCE_IDS = "author_person_reloaded_reference_ids"


def curator_connected(db: Session, reference_id: int) -> bool:
    """A curator (a person-backed user, as opposed to an automation user) created
    or last updated an author row of the reference that carries a person: a link
    made on the person page, or a person added who is not an author."""
    return db.query(AuthorModel.author_id).join(
        UserModel,
        or_(
            UserModel.user_id == AuthorModel.created_by_id,
            UserModel.user_id == AuthorModel.updated_by_id,
        ),
    ).filter(
        AuthorModel.reference_id == reference_id,
        AuthorModel.person_id.isnot(None),
        UserModel.person_id.isnot(None),
    ).first() is not None


def author_person_statuses(db: Session, reference_id: int) -> List[Tuple[str, str]]:
    """(mod abbreviation, current state) for each MOD whose corpus holds the
    reference and that has it in author-person curation."""
    rows = db.query(ModModel.abbreviation, WorkflowTagModel.workflow_tag_id).join(
        WorkflowTagModel, WorkflowTagModel.mod_id == ModModel.mod_id
    ).join(
        ModCorpusAssociationModel,
        (ModCorpusAssociationModel.mod_id == ModModel.mod_id)
        & (ModCorpusAssociationModel.reference_id == WorkflowTagModel.reference_id)
    ).filter(
        WorkflowTagModel.reference_id == reference_id,
        WorkflowTagModel.workflow_tag_id.in_(AUTHOR_PERSON_CURATION_STATES),
        ModCorpusAssociationModel.corpus.is_(True),
    ).all()
    return [(abbreviation, atp) for abbreviation, atp in rows]


@contextmanager
def _workflow_tag_history_off() -> Iterator[None]:
    """No workflow_tag_version rows while inside; other tables keep their history."""
    options = WorkflowTagModel.__versioned__
    had_option = 'versioning' in options
    previous = options.get('versioning')
    options['versioning'] = False
    try:
        yield
    finally:
        if had_option:
            options['versioning'] = previous
        else:
            del options['versioning']


def set_author_person_status(db: Session, reference_id: int, mod_abbreviation: str,
                             new_atp: str, record_history: bool) -> None:
    """Move the reference to ``new_atp`` through the transition table as an
    automated transition; commits. Without ``record_history`` no
    workflow_tag_version row is written."""
    # Imported here: workflow_tag_crud pulls in most of the crud layer.
    from agr_literature_service.api.crud.workflow_tag_crud import transition_to_workflow_status
    if not record_history and any(isinstance(obj, WorkflowTagModel)
                                  for obj in (*db.new, *db.dirty, *db.deleted)):
        raise RuntimeError("commit the session before an automated author-person status change: "
                           "its pending workflow_tag changes would be written without history")
    with nullcontext() if record_history else _workflow_tag_history_off():
        transition_to_workflow_status(db, str(reference_id), mod_abbreviation, new_atp,
                                      transition_type="automated")


def _set_logged(db: Session, reference_id: int, mod_abbreviation: str, current_atp: str,
                new_atp: str, record_history: bool, counts: Dict[str, int], log) -> None:
    try:
        set_author_person_status(db, reference_id, mod_abbreviation, new_atp, record_history)
    except Exception as e:  # one paper's failure must not stop the rest
        db.rollback()
        counts["author_person_status_errors"] = counts.get("author_person_status_errors", 0) + 1
        log.error(f"reference_id={reference_id} {mod_abbreviation}: author-person status "
                  f"{current_atp} -> {new_atp} failed: {e}")
        return
    key = f"author_person_status_to_{new_atp}"
    counts[key] = counts.get(key, 0) + 1
    log.info(f"reference_id={reference_id} {mod_abbreviation}: author-person status "
             f"{current_atp} -> {new_atp}" + ("" if record_history else " (no history)"))


def mark_authors_reloaded(db: Session, reference_id: int) -> None:
    """Called by update_authors after a drop-and-reload; the caller applies the
    status changes once its batch is committed."""
    db.info.setdefault(RELOADED_REFERENCE_IDS, set()).add(reference_id)


def discard_author_reload_marks(db: Session) -> None:
    """For a caller that rolled its batch back: those reloads never happened."""
    db.info.pop(RELOADED_REFERENCE_IDS, None)


def apply_author_reload_statuses(db: Session, log=None, record_history: bool = False) -> Dict[str, int]:
    """After the caller has committed the reloads marked in this session: a
    reloaded paper at "complete" goes back to "needed" (its new authors have no
    persons), and one whose reloaded author list is empty goes to "complete" from
    any state. Other states are left for the curator. No history, since
    update_authors only reloads a paper no curator has touched -- unless the
    caller is itself a curator action (``record_history``, required in the API)."""
    log = log or logger
    counts: Dict[str, int] = {}
    for reference_id in sorted(db.info.pop(RELOADED_REFERENCE_IDS, set())):
        statuses = author_person_statuses(db, reference_id)
        if not statuses:
            continue
        connected = all_authors_connected(db, reference_id)
        for mod_abbreviation, current_atp in statuses:
            if connected:
                new_atp: Optional[str] = (AUTHOR_PERSON_CURATION_COMPLETE
                                          if current_atp != AUTHOR_PERSON_CURATION_COMPLETE else None)
            else:
                new_atp = (AUTHOR_PERSON_CURATION_NEEDED
                           if current_atp == AUTHOR_PERSON_CURATION_COMPLETE else None)
            if new_atp is not None:
                _set_logged(db, reference_id, mod_abbreviation, current_atp, new_atp,
                            record_history, counts, log)
    return counts


def complete_when_all_connected(db: Session, reference_ids: Iterable[int],
                                dry_run: bool = False, log=None) -> Dict[str, int]:
    """For the ORCID/email linker: each reference whose ordered authors now all
    have a person goes to "complete" from any author-person state, for every MOD
    that has it in curation. History is written only if a curator has made an
    author-person connection on the paper."""
    log = log or logger
    counts: Dict[str, int] = {}
    for reference_id in sorted(set(reference_ids)):
        if not all_authors_connected(db, reference_id):
            continue
        pending = [(mod_abbreviation, current_atp)
                   for mod_abbreviation, current_atp in author_person_statuses(db, reference_id)
                   if current_atp != AUTHOR_PERSON_CURATION_COMPLETE]
        if not pending:
            continue
        record_history = curator_connected(db, reference_id)
        for mod_abbreviation, current_atp in pending:
            if dry_run:
                key = f"author_person_status_to_{AUTHOR_PERSON_CURATION_COMPLETE}"
                counts[key] = counts.get(key, 0) + 1
                log.info(f"reference_id={reference_id} {mod_abbreviation}: author-person status "
                         f"{current_atp} -> {AUTHOR_PERSON_CURATION_COMPLETE} (dry run)")
                continue
            _set_logged(db, reference_id, mod_abbreviation, current_atp,
                        AUTHOR_PERSON_CURATION_COMPLETE, record_history, counts, log)
    return counts
