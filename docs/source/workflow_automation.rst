Workflow Tag Automation
===================

About this Doc.
---------------

This is a general introduction to workflow tags and their automation and how the workflow_transition table works.
We will try to keep this doc up to date but beware this table may change over time.

Table structure.
^^^^^^^^^^^^^^^^

    .. code-block:: python

    ======                 ====                 ========  =======
    Column                 Type                 Nullable  Default
    ======                 ====                 ========  =======
    date_created           timestamp            False     None
    date_updated           timestamp            True      None
    workflow_transition_id integer              False     nextval(...)
    mod_id                 integer              False     None
    transition_from        character varying    False     None
    transition_to          character varying    False     None
    created_by             character varying    False     None
    updated_by             character varying    True      None
    requirements           character varying[]  True      None
    transition_type        character varying    False     'any'
    actions                character varying[]  True      None
    condition              character varying    True      None
    ======                 =====               ========   =======

   - transition_from:
       ATP string from which we are starting.
    - transition_to:
       ATP string to which we want to move too.
    - requirements:
       Array of strings, which list functions that must pass for this transition to be allowed.
    - transition_type:
       Type of transitions that are allowed. These are entries like
       any, automated, manual which specify when this transition is allowed.
       i.e. if it is 'manual' and an automated process tries to do it then
       it will not be allowed.
    - actions
       Array of strings which relate to functions that will be applied after the transition is done.
       i.e. When we move to 'file uploaded' then if certain criteria are met then we will add 'file conversion needed'
    - condition
       These control/help with jobs. A string, comma separated for multiple conditions.

Requirements overview.
^^^^^^^^^^^^^^^^^^^^^^

    This is an array of strings that define methods that must pass before the transition is allowed.
    Mostly these will be Null but for certain transitions we want to make sure that certain criteria are met
    before this is allowed.
     Example: ATP:0000135     | ATP:0000139   | {referencefiles_present}
              So if we want to move from "file unavailable" (135) to "file upload in progress" (139)
              Then we need to check that there are reference files present now.

Actions overview.
^^^^^^^^^^^^^^^^^
    This is an array of strings that define methods and arguments that are processed when we transition.
    An example would be to add a workflow_tag 'text conversion needed' for WormBase files of reference type
    'experimental' when transitioning from "file upload in progress" to "files uploaded".
    The following shows the section from the data load file for this.
    (See "Adding data to the workflow_transition Table." for info on the data files.)


    .. code-block:: python

     {
            'mod': "WB",
            'from': "file upload in progress",
            'to': "files uploaded",
            'condition': 'on_success',
            'actions': [f"proceed_on_value::reference_type::experimental::{name_to_atp['text conversion needed']}"]
     },


Condition overview.
^^^^^^^^^^^^^^^^^^^
    Conditions are used for automated job controls. It is a string that is comma seperated for multiple values.
    Code that uses these, links the current workflow tag to the transition_to column with the values in the condition.
    This alleviates hard coding values in scripts etc and has the human readable conditions listed in one place.
    This is explained more fully in the automated job section.


Viewing data in the workflow_transition Table.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    Because we store the workflow tags as the A-Teams ATP values these are very human readable.
    Under the directory lit_processing/oneoff_scripts/workflow there some helper scripts.
    table_to_human_readable_transitions.py will translate the data to present a more readable version.

Adding data to the workflow_transition Table.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    Under the same directory there is a sub directory data that should be used to add the data that needs to be added/changed.
    The script transitions_add.py should be used to process these files.
    If you add new data files then the python script will need to be altered to find this data.
    Alterations include adding an import of the new file and adding another elif statement to run it.

Automated jobs.
^^^^^^^^^^^^^^^
    In the condition part of the table we list jobs that can be found, started, completed or failed.

    To find the jobs ready for processing there is a api end point and method get_jobs in workflow_tag_crud.py.
    This method links the transition_to too current workflow_tags and looks for conditions which contain a string which
    is specified. So if we have the following:-

    An example of classifications that are needed, lets assume we have already loaded:-
    (see classification.py in data directory)

    .. code-block:: python

      for entry in ('catalytic activity', 'disease', 'expression', 'interaction'):
        item = {
            'mod': 'ALL',
            'from': 'reference classification needed',
            'to': f'{entry} classification needed',
            'condition': f'{entry}_classification_job'}
        test_data.append(item)
        item = {
            'mod': 'ALL',
            'from': f'{entry} classification needed',
            'to': f'{entry} classification in progress',
            'condition': 'on_start'}
        test_data.append(item)
        item = {
            'mod': 'ALL',
            'from': f'{entry} classification in progress',
            'to': f'{entry} classification failed',
            'condition': 'on_failed'}
        test_data.append(item)
        item = {
            'mod': 'ALL',
            'from': f'{entry} classification in progress',
            'to': f'{entry} classification complete',
            'condition': 'on_success'}
        test_data.append(item)




    So if we call get_jobs(db, 'interaction_classification_job') it will return all the jobs that need to run.
    This returns an array of dicts which has the info needed (including the reference_workflow_tag_id).

    Conditions of 'on_start', 'on_success' and 'on_failed' are then used to update that reference_workflow_tag_id
    object with the new tag values as it proceeds through the automation.

    Just before we start the job we need to set the workflow_tag to "interaction classification in progress".
    We do this by calling the method job_condition_on_start_process() which uses the current workflow_tag
    and the condition 'on_start' to find the new workflow_tag_id and replace the existing one.

    At the end of the job we call job_change_atp_code() with a string of either "on_success" or
    "on_failed" depending on how the job went. This will replace the workflow_tag from "interaction classification in progress"
    to "interaction classification complete" or "interaction classification failed" based on this.


When to use requirements, condition or action.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    In short:-
     - requirements are used to only allow transitions if certain requirements are met.
     - conditions are used in job control.
     - actions are used to add new workflow_tags or perform task after transition.


