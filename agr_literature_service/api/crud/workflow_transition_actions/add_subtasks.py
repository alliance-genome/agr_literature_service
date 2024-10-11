"""
Workflow Transition Actions change workflow status.
So in the column conditions in the table workflow_transition
if the condition is "add_subtask" then set this to the workflow.

Example.
If we have in the transition table
transition_from           transition_to                  condition                action
XXXX                      entity_extract_needed                                   {add_subtasks}
entity_extract_needed     anti_body_extract_needed       add_subtask,extract_job
entity_extract_needed     gene_extract_needed            add_subtask,extract_job

NOTE: Here i am using the labels to make it clearer but these will be ATP values.

So when transitioning from XXXX to entity_extract_needed the action will be activated
which will run the method add_subtasks.
This method will look for transition_from = 'entity_extract_needed' and condition 'add_subtask'
and add the workflow tags for those. So here anti_body_extract_needed and gene_extract_needed
would be added.

"job" is used to allow easy fetching of jobs rather than from the "needed" part of the label.
So in a api call we can get a list of jobs that need running from joining the transition_to field
and the workflow tag.
"""
from agr_literature_service.api.models import WorkflowTagModel, WorkflowTransitionModel
from sqlalchemy.orm import Session
from fastapi import HTTPException, status


def add_subtasks(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, args: list):
    """
    args: will be an empty list. But the general actions caller always adds this.
    """
    if args:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail="add_subtasks does not take any args but {args} was passed")

    transitions = db.query(WorkflowTransitionModel).filter(
        WorkflowTransitionModel.transition_from == current_workflow_tag_db_obj.workflow_tag_id,
        WorkflowTransitionModel.condition.contains("add_subtask"))

    trans_count = 0
    for transition in transitions:
        trans_count += 1
        new_workflow_tag = WorkflowTagModel(
            reference=current_workflow_tag_db_obj.reference,
            mod=current_workflow_tag_db_obj.mod,
            workflow_tag_id=transition.transition_to
        )
        db.add(new_workflow_tag)
    if trans_count == 0:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail="add_subtasks method did not add any subtasks. Please check the transition table.")
    db.commit()
