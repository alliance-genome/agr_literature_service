from agr_literature_service.api.models import WorkflowTagModel
from sqlalchemy.orm import Session
from fastapi import HTTPException, status


def proceed_on_value(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, checktype, check_value, new_atp):
    """
    args: [0] should be what to check to see if the ATP should be added.
              category: for reference.category or
              reference_type: for reference.mod_referencetypes
    args: [1] should be the value we expect it to be.
    args: [2] should be the new ATP value for the new workflow tag if test passes.
              e.g. "ATP:0000162"  :text conversion needed (ATP:0000162)
    So in the transition table we would have in the actions column
    mod_id for wormbase on transition to files uploaded (ATP:0000134)
    we would have action of proceed_on_value::reference_type::experimental::ATP:0000162
    for other organisms it would be
    proceed_on_value::category::Research_Article::ATP:0000162
    """
    print(f"Inside proceed_on_value {checktype, checktype, new_atp}")
    call_process = False
    if checktype == "category":  # Check reference category is "Research article"
        if current_workflow_tag_db_obj.reference.category == check_value:
            call_process = True
    elif checktype == "reference_type":
        if check_value in current_workflow_tag_db_obj.reference.mod_referencetypes:
            call_process = True
    else:  # Problem currently ONLY category and reference_type allowed
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail="Method {method} with first arg {checktype} not supported")

    if call_process:
        # sanity check, should start with ATP
        if not new_atp.startswith("ATP:"):
            raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                                detail="Method proceed_on_value with second arg {new_atp} must start with ATP:")
        #  Add new wft for this ref and mod
        WorkflowTagModel(reference=current_workflow_tag_db_obj.reference,
                         mod=current_workflow_tag_db_obj.mod,
                         workflow_tag_id=new_atp)
        db.commit()