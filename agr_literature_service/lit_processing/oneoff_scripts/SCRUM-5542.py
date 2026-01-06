
import datetime
from sqlalchemy.orm import Session
import pytz
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

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
testing = False


def do_it(session):
    mod_to_id = {'FB': 1, 'WB': 2, 'ZFIN': 3, 'SGD': 4, 'XB': 7}

    trans = [
        # from in_progress
        ['ATP:0000357', 'ATP:0000356', 'sub_task_failed::email extraction', 'on_failed'],
        ['ATP:0000357', 'ATP:0000355', 'sub_task_complete::email extraction', 'on_success'],
        # from needed
        ['ATP:0000358', 'ATP:0000356', 'sub_task_failed::email extraction', 'on_failed'],
        ['ATP:0000358', 'ATP:0000355', 'sub_task_complete::email extraction', 'on_success'],
        # from failed
        ['ATP:0000356', 'ATP:0000358', 'sub_task_retry::email extraction', 'on_retry'],
        # on start
        ['ATP:0000354', 'ATP:0000357', 'sub_task_in_progress::email extraction', 'on_start'],
        #
        ['ATP:0000162', 'ATP:0000163', 'proceed_on_value::all::email extraction', 'on_success'],
        ['ATP:0000198', 'ATP:0000163', 'proceed_on_value::all::email extraction', 'on_success']
    ]
    for mod_id in mod_to_id.values():
        for tran in trans:
            select_sql = f"""SELECT workflow_transition_id FROM workflow_transition
                               WHERE mod_id = {mod_id} AND
                                  transition_from = '{tran[0]}' AND
                                  transition_to = '{tran[1]}' AND
                                  condition = '{tran[3]}'
                                  """
            ids = db_session.execute(text(select_sql)).all()
            print(f"mod {mod_id} tran {tran} ids {ids}")
            if len(ids):
                for wt_id in ids:
                    cmd = f"""UPDATE workflow_transition
                              SET actions  = array_append(actions, '{tran[2]}')
                              WHERE workflow_transition_id = {wt_id[0]} """
                    print(cmd)
                    if not testing:
                        db_session.execute(text(cmd))
            else:
                cmd = f"""INSERT INTO workflow_transition
                     (mod_id, transition_from, transition_to, actions, transition_type, condition, date_created)
                    VALUES ({mod_id}, '{tran[0]}', '{tran[1]}', ARRAY['{tran[2]}'], 'any', '{tran[3]}', '{datetime.datetime.now(tz=pytz.timezone('UTC'))}')"""
                print(cmd)
                if not testing:
                    db_session.execute(text(cmd))

if __name__ == "__main__":
    db_session: Session = create_postgres_session(False)
    do_it(db_session)
    if not testing:
        db_session.commit()
    db_session.close()
