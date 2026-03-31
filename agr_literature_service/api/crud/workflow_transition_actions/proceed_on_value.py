from agr_literature_service.api.models import WorkflowTagModel
from fastapi import HTTPException, status
from agr_literature_service.api.crud.ateam_db_helpers import get_workflow_tags_for_mod


def proceed_on_value(db, current_workflow_tag_db_obj, args):
    """
    args: [0] should be what to check to see if the ATP should be added.
              category: for reference.category or
              reference_type: for reference.mod_referencetypes
    args: [1] should be the value we expect it to be.
    args: [2] should be the new ATP value for the new workflow tag if test passes.
              e.g. "ATP:0000162"  :text conversion needed (ATP:0000162)
    So in the transition table we would have in the actions column
    mod_id for WormBase on transition to files uploaded (ATP:0000134)
    we would have action of proceed_on_value::reference_type::experimental::ATP:0000162
    for other organisms it would be
    proceed_on_value::category::Research_Article::ATP:0000162
    """
    from sqlalchemy import text

    checktype = args[0]
    check_value = args[1]
    new_atp = args[2] if len(args) > 2 else args[1]

    call_process = False
    if checktype == "category":
        if current_workflow_tag_db_obj.reference.category == check_value:
            call_process = True
    elif checktype == "all":
        call_process = True
    elif checktype == "reference_type":
        select = f"""
        select rt.label
          from referencetype rt, mod_referencetype mrt, reference_mod_referencetype rmrt
          where rt.referencetype_id = mrt.referencetype_id and
                mrt.mod_id = {current_workflow_tag_db_obj.mod_id} and
                rmrt.reference_id = {current_workflow_tag_db_obj.reference_id} and
                mrt.mod_referencetype_id = rmrt.mod_referencetype_id
        """
        rows = db.execute(text(select)).fetchall()
        for row in rows:
            if check_value == row[0]:
                call_process = True
                continue
    else:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=f"Method {checktype} not supported")

    if call_process:
        mod_abbr = current_workflow_tag_db_obj.mod.abbreviation
        for atp in get_workflow_tags_for_mod(new_atp, mod_abbr):
            wtm = WorkflowTagModel(
                reference=current_workflow_tag_db_obj.reference,
                mod=current_workflow_tag_db_obj.mod,
                workflow_tag_id=atp
            )
            db.add(wtm)
        # Note: commit is handled by the caller (workflow_tag_crud.py)
