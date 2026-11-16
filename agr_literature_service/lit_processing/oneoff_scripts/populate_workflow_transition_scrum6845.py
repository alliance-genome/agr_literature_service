"""
SCRUM-6845: the workflow_transition rows that the code now needs, because every
workflow tag change goes through transition_to_workflow_status().

- A paper's first state in a process is a row from the process term to that
  state; a row from a state to the process term removes the tag.
- Rows of type 'action' are job-poll connectors (get_jobs reads their
  condition) and are never fired as transitions.
- The WF editor clears a tag with a manual transition to the process term, so
  every process it shows gets state -> process rows.

The script only adds missing rows, and changes transition_type on the rows
listed in TYPE_CHANGES. Existing rows are otherwise left as they are. It is
idempotent; without --apply it only reports what it would do.

    python3 populate_workflow_transition_scrum6845.py           # dry run
    python3 populate_workflow_transition_scrum6845.py --apply
"""
import argparse
import logging
from typing import Dict, List, NamedTuple, Optional, Tuple

from agr_literature_service.api.models import ModModel, WorkflowTransitionModel

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

ALL_MODS = ("FB", "MGI", "RGD", "SGD", "WB", "XB", "ZFIN")
EMAIL_MODS = ("FB", "SGD", "WB", "XB", "ZFIN")

FILE_UPLOAD = "ATP:0000140"
FILE_NEEDED = "ATP:0000141"
FILE_UPLOAD_IN_PROGRESS = "ATP:0000139"
FILES_UPLOADED = "ATP:0000134"
FILE_UNAVAILABLE = "ATP:0000135"
TEXT_CONVERSION = "ATP:0000161"
TEXT_CONVERSION_NEEDED = "ATP:0000162"
MANUAL_INDEXING = "ATP:0000273"
MANUAL_INDEXING_STATES = ("ATP:0000274",   # manual indexing needed
                          "ATP:0000275",   # manual indexing complete
                          "ATP:0000276",   # manual indexing in progress
                          "ATP:0000336",
                          "ATP:0000343",   # won't manually index
                          "ATP:0000359")   # manual indexing status TBD
MANUAL_INDEXING_NEEDED = "ATP:0000274"
WONT_MANUALLY_INDEX = "ATP:0000343"
EMAIL_EXTRACTION = "ATP:0000354"
EMAIL_EXTRACTION_NEEDED = "ATP:0000358"
EMAIL_EXTRACTION_IN_PROGRESS = "ATP:0000357"
AUTHOR_REVIEW = "ATP:0000388"
AUTHOR_REVIEW_NEEDED = "ATP:0000389"
AUTHOR_REVIEW_COMPLETE = "ATP:0000392"
AUTHOR_PERSON_CURATION = "ATP:0000375"
AUTHOR_PERSON_NEEDED = "ATP:0000109"
AUTHOR_PERSON_COMPLETE = "ATP:0000378"
COMMUNITY_CURATION = "ATP:0000235"
COMMUNITY_CURATION_STATES = ("ATP:0000231", "ATP:0000232", "ATP:0000233", "ATP:0000234", "ATP:0000342",
                             "ATP:0000394")
FIRST_PASS_CURATION = "ATP:0000329"
FIRST_PASS_CURATION_STATES = ("ATP:0000330", "ATP:0000331", "ATP:0000332", "ATP:0000333", "ATP:0000371")
AUTHOR_PERSON_STATES = ("ATP:0000109", "ATP:0000377", "ATP:0000376", "ATP:0000378")
AUTHOR_REVIEW_STATES = ("ATP:0000389", "ATP:0000390", "ATP:0000391", "ATP:0000392")
ZFIN_TOP = "ATP:0000177"
ZFIN_INDEXING_PRIORITY = "ATP:0000306"
ZFIN_MOLECULAR_PROBE = "ATP:0000380"


class Row(NamedTuple):
    mods: Tuple[str, ...]
    transition_from: str
    transition_to: str
    transition_type: str = "any"
    condition: Optional[str] = None


def _rows() -> List[Row]:
    rows: List[Row] = []
    # file upload: the first state (corpus entry, TET, file upload), every MOD including AGR
    for state in (FILE_NEEDED, FILE_UPLOAD_IN_PROGRESS, FILES_UPLOADED):
        rows.append(Row(ALL_MODS + ("AGR",), FILE_UPLOAD, state))
    # the UI tracker's "file unavailable" for a paper still at "file needed"
    rows.append(Row(ALL_MODS, FILE_NEEDED, FILE_UNAVAILABLE))
    # text conversion needed on a paper with no text conversion tag (proceed_on_value actions)
    rows.append(Row(ALL_MODS, TEXT_CONVERSION, TEXT_CONVERSION_NEEDED))
    # manual indexing: first state from the process term, and removal back to it
    for state in MANUAL_INDEXING_STATES:
        rows.append(Row(ALL_MODS, MANUAL_INDEXING, state))
        rows.append(Row(ALL_MODS, state, MANUAL_INDEXING))
    # "won't manually index" can go back to needed / 336
    rows.append(Row(ALL_MODS, WONT_MANUALLY_INDEX, MANUAL_INDEXING_NEEDED))
    rows.append(Row(ALL_MODS, WONT_MANUALLY_INDEX, "ATP:0000336"))
    # email extraction is a job on "needed": the job connector, and the job's start
    rows.append(Row(EMAIL_MODS, EMAIL_EXTRACTION, EMAIL_EXTRACTION_NEEDED, "action", "email_extraction_job"))
    rows.append(Row(EMAIL_MODS, EMAIL_EXTRACTION_NEEDED, EMAIL_EXTRACTION_IN_PROGRESS, "any", "on_start"))
    # WB author review: the PubMed update (automated) starts it at "needed"
    rows.append(Row(("WB",), AUTHOR_REVIEW, AUTHOR_REVIEW_NEEDED))
    # WB author-person curation: the sort page checkbox starts it at "needed" or "complete"
    rows.append(Row(("WB",), AUTHOR_PERSON_CURATION, AUTHOR_PERSON_NEEDED))
    rows.append(Row(("WB",), AUTHOR_PERSON_CURATION, AUTHOR_PERSON_COMPLETE))
    # the WF editor clears a tag with a manual transition to its process term (removal), for the
    # processes it shows besides manual indexing
    for mods, process, states in ((("FB", "WB"), COMMUNITY_CURATION, COMMUNITY_CURATION_STATES),
                                  (("FB",), FIRST_PASS_CURATION, FIRST_PASS_CURATION_STATES),
                                  (("WB",), AUTHOR_PERSON_CURATION, AUTHOR_PERSON_STATES),
                                  (("WB",), AUTHOR_REVIEW, AUTHOR_REVIEW_STATES)):
        for state in states:
            rows.append(Row(mods, state, process, "manual_only"))
    return rows


# existing rows whose transition_type changes: (mods, from, to, new type)
TYPE_CHANGES: List[Tuple[Tuple[str, ...], str, str, str]] = [
    # the PubMed update moves author review between needed and complete (automated)
    (("WB",), AUTHOR_REVIEW_NEEDED, AUTHOR_REVIEW_COMPLETE, "any"),
    (("WB",), AUTHOR_REVIEW_COMPLETE, AUTHOR_REVIEW_NEEDED, "any"),
    # ZFIN's corpus-entry job connectors are never fired
    (("ZFIN",), ZFIN_TOP, ZFIN_INDEXING_PRIORITY, "action"),
    (("ZFIN",), ZFIN_TOP, ZFIN_MOLECULAR_PROBE, "action"),
]


def populate(db_session, apply: bool = False) -> Dict[str, int]:
    """Add the missing rows and make the type changes; returns counts. Commits only with apply."""
    mod_ids = {abbreviation: mod_id for abbreviation, mod_id in
               db_session.query(ModModel.abbreviation, ModModel.mod_id).all()}
    existing = {(t.mod_id, t.transition_from, t.transition_to): t
                for t in db_session.query(WorkflowTransitionModel).all()}
    counts = {"added": 0, "type_changed": 0, "present": 0, "missing_mod": 0}
    for row in _rows():
        for mod in row.mods:
            mod_id = mod_ids.get(mod)
            if mod_id is None:
                counts["missing_mod"] += 1
                log.info(f"skip {mod} {row.transition_from} -> {row.transition_to}: no such mod")
                continue
            if (mod_id, row.transition_from, row.transition_to) in existing:
                counts["present"] += 1
                continue
            log.info(f"add {mod} {row.transition_from} -> {row.transition_to} "
                     f"type={row.transition_type} condition={row.condition}")
            db_session.add(WorkflowTransitionModel(mod_id=mod_id, transition_from=row.transition_from,
                                                   transition_to=row.transition_to,
                                                   transition_type=row.transition_type,
                                                   condition=row.condition, actions=[]))
            counts["added"] += 1
    for mods, transition_from, transition_to, new_type in TYPE_CHANGES:
        for mod in mods:
            transition = existing.get((mod_ids.get(mod), transition_from, transition_to))
            if transition is None or transition.transition_type == new_type:
                continue
            log.info(f"type {mod} {transition_from} -> {transition_to}: "
                     f"{transition.transition_type} -> {new_type}")
            transition.transition_type = new_type
            counts["type_changed"] += 1
    if apply:
        db_session.commit()
    else:
        db_session.rollback()
    log.info(f"{'Applied' if apply else 'Dry run'}: {counts}")
    return counts


def main():  # pragma: no cover
    from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="write the changes (default: dry run)")
    args = parser.parse_args()
    db_session = create_postgres_session(False)
    try:
        populate(db_session, apply=args.apply)
    finally:
        db_session.close()


if __name__ == "__main__":  # pragma: no cover
    main()
