"""
first_pass_curation.py

Workflow-transition action for the FlyBase "first pass curation" workflow (SCRUM-5478).

`set_first_pass_curation_tbd` seeds the "first pass curation TBD" (ATP:0000371) workflow
tag for a reference+MOD once all three prerequisite processes are complete:
  - entity extraction complete            (ATP:0000174)
  - reference / topic classification complete (ATP:0000169)
  - curation classification complete      (ATP:0000312)

Entity extraction is the last step of the pre-curation pipeline, and its "complete" state
is reached by the subtask roll-up in ``sub_task_complete`` (subtask_process.py), which
sets the main tag directly rather than via a transition. So this check is invoked from
that roll-up point (not as a transition action) when entity extraction becomes complete.
It re-checks all three prerequisites and, when all are present, inserts the TBD tag.

First pass curation is FlyBase-only, so this is a no-op for any other MOD. It mirrors
``proceed_on_value`` in that it only adds a tag and leaves the commit to the caller.
"""
from agr_literature_service.api.models import WorkflowTagModel

FIRST_PASS_CURATION_MOD = "FB"
FIRST_PASS_CURATION_PROCESS_ATP = "ATP:0000329"
FIRST_PASS_CURATION_TBD_ATP = "ATP:0000371"

# Every tag in the first pass curation process (parent + the five status states). Used
# by the idempotency guard. Kept as an explicit constant rather than a live ontology
# lookup so the guard is deterministic and cannot "fail open" (and wrongly re-seed) if
# the ATP cache is unavailable.
FIRST_PASS_CURATION_ALL_ATPS = (
    FIRST_PASS_CURATION_PROCESS_ATP,  # ATP:0000329 first pass curation (process)
    "ATP:0000331",                    # first pass curation needed
    "ATP:0000332",                    # first pass curation in progress
    "ATP:0000333",                    # first pass curation blocked
    FIRST_PASS_CURATION_TBD_ATP,      # ATP:0000371 first pass curation TBD
    "ATP:0000330",                    # first pass curation finished
)

# The three "complete" states that together gate first pass curation TBD.
ENTITY_EXTRACTION_COMPLETE_ATP = "ATP:0000174"
REFERENCE_CLASSIFICATION_COMPLETE_ATP = "ATP:0000169"
CURATION_CLASSIFICATION_COMPLETE_ATP = "ATP:0000312"

REQUIRED_COMPLETE_ATPS = (
    ENTITY_EXTRACTION_COMPLETE_ATP,
    REFERENCE_CLASSIFICATION_COMPLETE_ATP,
    CURATION_CLASSIFICATION_COMPLETE_ATP,
)


def set_first_pass_curation_tbd(db, current_workflow_tag_db_obj, args):
    """
    Seed "first pass curation TBD" (ATP:0000371) once all three prerequisite
    processes are complete for the reference+MOD.

    Idempotent: does nothing if the reference+MOD already has any workflow tag in the
    first pass curation process (ATP:0000329 subtree).

    Note: commit is handled by the caller (workflow_tag_crud.py / subtask_process.py).
    """
    # First pass curation is FlyBase-only; no-op for any other MOD.
    mod = current_workflow_tag_db_obj.mod
    if mod is None or mod.abbreviation != FIRST_PASS_CURATION_MOD:
        return

    reference_id = current_workflow_tag_db_obj.reference_id
    mod_id = current_workflow_tag_db_obj.mod_id

    # The transition that triggered this action sets the landing tag on
    # current_workflow_tag_db_obj before running actions, but the DB row may not be
    # committed/flushed yet, so count the triggering tag explicitly.
    present = {
        row.workflow_tag_id
        for row in db.query(WorkflowTagModel.workflow_tag_id).filter(
            WorkflowTagModel.reference_id == reference_id,
            WorkflowTagModel.mod_id == mod_id,
            WorkflowTagModel.workflow_tag_id.in_(REQUIRED_COMPLETE_ATPS),
        ).all()
    }
    present.add(current_workflow_tag_db_obj.workflow_tag_id)

    if not all(atp in present for atp in REQUIRED_COMPLETE_ATPS):
        return

    # Idempotency guard: skip if the reference+MOD already has any first pass curation
    # tag (so a curator's manual state is never overwritten, and we never insert a
    # duplicate TBD).
    already_present = db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == reference_id,
        WorkflowTagModel.mod_id == mod_id,
        WorkflowTagModel.workflow_tag_id.in_(FIRST_PASS_CURATION_ALL_ATPS),
    ).first()
    if already_present:
        return

    db.add(WorkflowTagModel(
        reference=current_workflow_tag_db_obj.reference,
        mod=current_workflow_tag_db_obj.mod,
        workflow_tag_id=FIRST_PASS_CURATION_TBD_ATP,
    ))
