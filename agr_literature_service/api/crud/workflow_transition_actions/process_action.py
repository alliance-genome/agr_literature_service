from agr_literature_service.api.crud.workflow_transition_actions.proceed_on_value import proceed_on_value  # noqa
from agr_literature_service.api.crud.workflow_transition_actions import ADMISSIBLE_WORKFLOW_TRANSITION_ACTION_FUNCTIONS
from fastapi import HTTPException, status
from agr_literature_service.api.models import WorkflowTagModel
from sqlalchemy.orm import Session


def process_action(db: Session, current_workflow_tag_db_obj: WorkflowTagModel, action: str):
    """
    Actions have the form method_name::arg1::arg2 etc.
    """
    args = action.split("::")
    method = args.pop(0)
    checks = False
    if method in ADMISSIBLE_WORKFLOW_TRANSITION_ACTION_FUNCTIONS:
        try:
            print(f"Calling {method} with args {args}")
            print(f"AWTAF: {ADMISSIBLE_WORKFLOW_TRANSITION_ACTION_FUNCTIONS}")
            print(f"locals: {locals()}")
            ADMISSIBLE_WORKFLOW_TRANSITION_ACTION_FUNCTIONS[method](db, current_workflow_tag_db_obj, args[0], args[1], args[2])
            # checks = locals()[method](db, current_workflow_tag_db_obj, args[0], args[1], args[2])
        except Exception as e:
            print(f"Exception {e} {type(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Problem running method {method} which raises exception {e}. {checks}")
    else:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail="Method {method} not supported")