from agr_literature_service.api.crud.workflow_transition_actions import *  # noqa
from fastapi import HTTPException, status


def process_action(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, action: str):
    """
    Actions have the form method_name::arg1::arg2 etc.
    """
    args = action.split("::")
    method = args.pop(0)
    if method in ADMISSIBLE_WORKFLOW_TRANSITION_ACTION_FUNCTIONS:
        try:
            locals()[method](db, current_workflow_tag_db_obj, args)
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Problem running method {method} which raises exception {e}")
    else:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail="Method {method} not supported")
