
import datetime
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
import pytz

## from in_progress
# 'FB', 'ATP:0000357', 'ATP:0000356',  'None', '['sub_task_failed::email extraction']', 'on_failed'
# 'FB', 'ATP:0000357', 'ATP:0000355',  'None', '['sub_task_complete::email extraction']', 'on_success'
## from needed
# 'FB', 'ATP:0000358', 'ATP:0000356',  'None', '['sub_task_failed::email extraction']', 'on_failed'
# 'FB', 'ATP:0000358', 'ATP:0000355',  'None', '['sub_task_complete:email extraction']', 'on_success'
## from failed
# 'FB', 'ATP:0000356', 'ATP:0000358',  'None', '['sub_task_retry::email extraction']', 'on_retry'

## on start ?
# 'FB', 'ATP:0000354', 'ATP:0000357',  'None', '['sub_task_in_progress::email extraction']', 'on_start'
# After tei conversion “email extraction needed” wf tags added to references  for FB, WB, ZFIN, SGD, XB


def do_it(session):
    mod_to_id = {'FB': 1, 'WB': 2, 'ZFIN': 3, 'SGD': 4, 'XB': 7}

    trans = [
        # from in_progress
        ['ATP:0000357', 'ATP:0000356', ['sub_task_failed::email extraction'], 'on_failed'],
        ['ATP:0000357', 'ATP:0000355', ['sub_task_complete::email extraction'], 'on_success'],
        # from needed
        ['ATP:0000358', 'ATP:0000356', ['sub_task_failed::email extraction'], 'on_failed'],
        ['ATP:0000358', 'ATP:0000355', ['sub_task_complete:email extraction'], 'on_success'],
        # from failed
        ['ATP:0000356', 'ATP:0000358', ['sub_task_retry::email extraction'], 'on_retry'],
        # on start
        ['ATP:0000354', 'ATP:0000357', ['sub_task_in_progress::email extraction'], 'on_start']

    ]
    for mod_id in mod_to_id.values():
        for tran in trans:
            cmd = f"""INSERT INTO workflow_transition
                 (mod_id, transition_from, transition_to, actions, transition_type, condition, date_created)
                VALUES ({mod_id}, '{tran[0]}', '{tran[1]}', {tran[2]}, 'any', '{tran[3]}', '{datetime.datetime.now(tz=pytz.timezone('UTC'))}')"""
            print(cmd)
            # db_session.execute(text(cmd))
    mod_to_id = {'ZFIN': 3, 'SGD': 4, 'XB': 7}
    trans = [
        ['ATP:0000162', 'ATP:0000163', ['proceed_on_value::all::email extraction'],
         'on_success'],
        ['ATP:0000198', 'ATP:0000163', ['proceed_on_value::all::email extraction'],
         'on_success']
    ]
    for mod_id in mod_to_id.values():
        for tran in trans:
            cmd = f"""INSERT INTO workflow_transition
                 (mod_id, transition_from, transition_to, actions, transition_type, condition, date_created)
                VALUES ({mod_id}, '{tran[0]}', '{tran[1]}', {tran[2]}, 'any', '{tran[3]}', '{datetime.datetime.now(tz=pytz.timezone('UTC'))}')"""
            print(cmd)
            # db_session.execute(text(cmd))

    # BUT we want to update these ones and add new 'proceed_on_value::all::email extraction' to actions list.
    # [ 'ATP:0000162', 'ATP:0000163',  ['proceed_on_value::category::research_article::reference classification', 'proceed_on_value::category::research_article::curation classification', 'proceed_on_value::category::research_article::entity extraction'], 'on_success'],
    # [ 'ATP:0000198', 'ATP:0000163',  ['proceed_on_value::category::research_article::reference classification', 'proceed_on_value::category::research_article::curation classification', 'proceed_on_value::category::research_article::entity extraction'], 'on_success']


if __name__ == "__main__":
    db_session: Session = create_postgres_session(False)
    do_it(db_session)
