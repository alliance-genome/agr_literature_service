"""
first_pass_curation.py

Workflow-transition action for the FlyBase "first pass curation" workflow (SCRUM-5478).

`set_first_pass_curation_tbd` seeds the "first pass curation TBD" (ATP:0000371) workflow
tag for a reference+MOD once all three prerequisite processes are complete:
  - entity extraction complete            (ATP:0000174)
  - reference / topic classification complete (ATP:0000169)
  - curation classification complete      (ATP:0000312)

Each of those three prerequisite processes reaches its "complete" state via the subtask
roll-up in ``sub_task_complete`` (subtask_process.py), which sets the main tag directly
rather than via a transition. So this check is invoked from that roll-up point (not as a
transition action) whenever ANY of the three prerequisites rolls up to complete -- it is
order-independent, so whichever process finishes last triggers the seeding. It re-checks
all three prerequisites and, when all are present, inserts the TBD tag.

ATP ids are resolved from the A-Team ontology by name via ``get_atp_id_by_name`` with the
hardcoded ids kept only as fallbacks, matching the rest of workflow_tag_crud.py. The
fallback keeps the idempotency guard deterministic even if the ontology cache is
unavailable.

First pass curation is FlyBase-only, so this is a no-op for any other MOD. It mirrors
``proceed_on_value`` in that it only adds a tag and leaves the commit to the caller.
"""
from agr_literature_service.api.models import WorkflowTagModel

FIRST_PASS_CURATION_MOD = "FB"

# The three "complete" states that together gate first pass curation TBD.
# name -> fallback ATP id (resolved by name at call time).
REQUIRED_COMPLETE_NAME_TO_FALLBACK = {
    'entity extraction complete': 'ATP:0000174',
    'reference classification complete': 'ATP:0000169',
    'curation classification complete': 'ATP:0000312',
}

# Every tag in the first pass curation process (parent + the five status states). Used by
# the idempotency guard. name -> fallback ATP id.
FIRST_PASS_CURATION_NAME_TO_FALLBACK = {
    'first pass curation': 'ATP:0000329',            # process parent
    'first pass curation needed': 'ATP:0000331',
    'first pass curation in progress': 'ATP:0000332',
    'first pass curation blocked': 'ATP:0000333',
    'first pass curation TBD': 'ATP:0000371',
    'first pass curation finished': 'ATP:0000330',
}
FIRST_PASS_CURATION_TBD_NAME = 'first pass curation TBD'
FIRST_PASS_CURATION_TBD_FALLBACK = 'ATP:0000371'

# The sub_task_complete checktypes for the three prerequisite processes. The roll-up
# invokes the TBD check when any of these completes, so the trigger is order-independent.
# "email extraction" is intentionally excluded -- it is not a first pass curation
# prerequisite.
FIRST_PASS_CURATION_PREREQUISITE_CHECKTYPES = frozenset({
    'entity extraction',
    'reference classification',
    'curation classification',
})


def set_first_pass_curation_tbd(db, current_workflow_tag_db_obj, args):
    """
    Seed "first pass curation TBD" (ATP:0000371) once all three prerequisite
    processes are complete for the reference+MOD.

    Idempotent: does nothing if the reference+MOD already has any workflow tag in the
    first pass curation process (ATP:0000329 subtree).

    Note: commit is handled by the caller (workflow_tag_crud.py / subtask_process.py).
    """
    # Local import avoids a circular import (workflow_tag_crud imports the actions package).
    from agr_literature_service.api.crud.workflow_tag_crud import get_atp_id_by_name

    # First pass curation is FlyBase-only; no-op for any other MOD.
    mod = current_workflow_tag_db_obj.mod
    if mod is None or mod.abbreviation != FIRST_PASS_CURATION_MOD:
        return

    reference_id = current_workflow_tag_db_obj.reference_id
    mod_id = current_workflow_tag_db_obj.mod_id

    required_complete_atps = [
        get_atp_id_by_name(name, fallback=fallback)
        for name, fallback in REQUIRED_COMPLETE_NAME_TO_FALLBACK.items()
    ]

    # The transition/roll-up that triggered this action sets the landing tag on
    # current_workflow_tag_db_obj before running actions, but the DB row may not be
    # committed/flushed yet, so count the triggering tag explicitly.
    present = {
        row.workflow_tag_id
        for row in db.query(WorkflowTagModel.workflow_tag_id).filter(
            WorkflowTagModel.reference_id == reference_id,
            WorkflowTagModel.mod_id == mod_id,
            WorkflowTagModel.workflow_tag_id.in_(required_complete_atps),
        ).all()
    }
    present.add(current_workflow_tag_db_obj.workflow_tag_id)

    if not all(atp in present for atp in required_complete_atps):
        return

    # Idempotency guard: skip if the reference+MOD already has any first pass curation
    # tag (so a curator's manual state is never overwritten, and we never insert a
    # duplicate TBD).
    first_pass_curation_atps = [
        get_atp_id_by_name(name, fallback=fallback)
        for name, fallback in FIRST_PASS_CURATION_NAME_TO_FALLBACK.items()
    ]
    already_present = db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == reference_id,
        WorkflowTagModel.mod_id == mod_id,
        WorkflowTagModel.workflow_tag_id.in_(first_pass_curation_atps),
    ).first()
    if already_present:
        return

    tbd_atp = get_atp_id_by_name(FIRST_PASS_CURATION_TBD_NAME, fallback=FIRST_PASS_CURATION_TBD_FALLBACK)
    db.add(WorkflowTagModel(
        reference=current_workflow_tag_db_obj.reference,
        mod=current_workflow_tag_db_obj.mod,
        workflow_tag_id=tbd_atp,
    ))
