from agr_literature_service.api.crud.workflow_transition_actions.proceed_on_value import proceed_on_value
from agr_literature_service.api.crud.workflow_transition_actions.subtask_process import (
    sub_task_in_progress,
    sub_task_complete,
    sub_task_failed,
    sub_task_retry)

ADMISSIBLE_WORKFLOW_TRANSITION_ACTION_FUNCTIONS = {"proceed_on_value": proceed_on_value,
                                                   "sub_task_retry": sub_task_retry,
                                                   "sub_task_in_progress": sub_task_in_progress,
                                                   "sub_task_complete": sub_task_complete,
                                                   "sub_task_failed": sub_task_failed}
# ADMISSIBLE_WORKFLOW_TRANSITION_ACTION_FUNCTIONS = {
#    "proceed_on_value",     # add workflow tag if conditions met
#    "workflow_tag_remove",  # remove workflow tag
#    "add_subtasks"          # Add subtasks for a workflow
# }
