"""
Load new retry transitions.
i.e.

from: 'antibody classification failed'
to:   'antibody classification needed'
condition: 'on_retry'
actions: ['sub_task_retry:reference classification']
transition_type: "any"

"""
import logging
from agr_literature_service.api.crud.workflow_tag_crud import get_workflow_tags_from_process
from agr_literature_service.api.models import WorkflowTransitionModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.ateam_db_helpers import get_name_to_atp_for_descendants


logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def generate_transitions(db_session, parent_atp):
    needed = {}
    failed = {}
    in_progress = {}
    mods = {}
    name_to_atp, atp_to_name = get_name_to_atp_for_descendants(parent_atp)

    tags = get_workflow_tags_from_process(parent_atp)
    wfts = db_session.query(WorkflowTransitionModel).filter(
        WorkflowTransitionModel.condition.contains('on_start'),
        WorkflowTransitionModel.transition_to.in_(tags)).all()
    for wft in wfts:
        fro = atp_to_name[wft.transition_from]
        to = atp_to_name[wft.transition_to]
        print(f"{fro} -> {to}")
        name = fro[:-7]
        if name not in mods:
            mods[name] = [wft.mod_id]
        else:
            mods[name].append(wft.mod_id)
        needed[name] = wft.transition_from
        in_progress[name] = wft.transition_to

    wfts = db_session.query(WorkflowTransitionModel).filter(
        WorkflowTransitionModel.condition.contains('on_failed'),
        WorkflowTransitionModel.transition_to.in_(tags)).all()

    for wft in wfts:
        fro = atp_to_name[wft.transition_from]
        to = atp_to_name[wft.transition_to]
        print(f"{fro} -> {to}")
        name = to[:-7]
        failed[name] = wft.transition_to

    for name in needed.keys():
        if name not in failed:
            print(f"ERROR: {name} not in failed")
            print(f"{failed.keys()}")

    for name in needed.keys():
        for mod in mods[name]:
            print(f"{name} {mod}")
            print(f"{name} {mod}: {atp_to_name[failed[name]]} -> {atp_to_name[needed[name]]} condition:'on_retry' ")
            wft = WorkflowTransitionModel(
                mod_id=mod,
                condition='on_retry',
                actions=['sub_task_retry::reference classification'],
                transition_from=failed[name],
                transition_to=needed[name])
            db_session.add(wft)
            print(f"{name} {mod}: {atp_to_name[in_progress[name]]} -> {atp_to_name[needed[name]]} condition:'on_retry' ")
            wft = WorkflowTransitionModel(
                mod_id=mod,
                condition='on_retry',
                actions=['sub_task_retry::reference classification'],
                transition_from=in_progress[name],
                transition_to=needed[name])
            db_session.add(wft)


def main():
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO)
    db_session = create_postgres_session(False)
    for parent_atp in ("ATP:0000165", "ATP:0000172"):
        generate_transitions(db_session, parent_atp)
    db_session.commit()


if __name__ == "__main__":
    main()
