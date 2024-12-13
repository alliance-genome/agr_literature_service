"""
This script fixes in progress status of workflow_tags

---------------------------------------------------------
WRT: SCRUM-4532
Job that cleans up “in progress” paper. Runs every week
For file conversion set back to conversion needed
For file conversion if it has transitioned 5 times between needed and in progress set to conversion failed.

For file classification  set back to classification needed

for classification if it has transitioned 5 times between needed and in progress set to * classification to failed.  Send slack message

For file extraction  set back to extraction needed

for entity extraction if it has transitioned 5 times between needed and in progress set to * entity extraction to failed. Send slack message
---------------------------------------------------------

ATP:0000198 -> text conversion in progress.
ATP:0000164 -> failed
ATP:0000162 -> needed

SELECT count(*) as count, reference_id, mod_id
  FROM workflow_tag_version
    WHERE workflow_tag_id = 'ATP:0000198' AND
          workflow_tag_id_mod is TRUE AND
          reference_id in (SELECT reference_id
                             FROM workflow_tag
                               WHERE workflow_tag_id = 'ATP:0000164')
      GROUP BY reference_id, mod_id
      HAVING count(*) > 1 and count(*) < 5;

SELECT count(*) as count, reference_id, mod_id
  FROM workflow_tag_version
    WHERE workflow_tag_id in ('ATP:0000198', 'ATP:0000164') AND
          workflow_tag_id_mod is TRUE AND
          reference_id in (SELECT reference_id
                             FROM workflow_tag
                               WHERE workflow_tag_id = 'ATP:0000164')
      GROUP BY reference_id, mod_id
      HAVING count(*) > 1 and count(*) < 5;

text conversion (ATP:0000161)
file converted to text (ATP:0000163)
file to text conversion failed (ATP:0000164)
text conversion in progress (ATP:0000198)
text conversion needed (ATP:0000162)

file uploaded (ATP:0000134)
file needed (ATP:0000141)

select operation_type, workflow_tag_id, mod_id, workflow_tag_id_mod, date_updated, created_by
  from workflow_tag_version
    where reference_id = 637206 order by date_updated;

 operation_type | workflow_tag_id | mod_id | workflow_tag_id_mod |        date_updated        |           created_by
----------------+-----------------+--------+---------------------+----------------------------+---------------------------------
              0 | ATP:0000103     |      2 | t                   | 2024-03-07 08:23:37.378885 | sort_dqm_json_reference_updates
              0 | ATP:0000141     |      2 | t                   | 2024-07-15 22:25:58.855822 | sort_dqm_json_reference_updates
              1 | ATP:0000134     |      2 | t                   | 2024-07-16 05:08:21.684672 | sort_dqm_json_reference_updates
              2 | ATP:0000134     |      2 | t                   | 2024-07-16 05:08:21.684672 | sort_dqm_json_reference_updates
              0 | ATP:0000134     |      2 | t                   | 2024-07-17 00:52:07.340037 | transition_WFT_to_file_uploaded
              2 | ATP:0000134     |      2 | t                   | 2024-07-17 00:52:07.340037 | transition_WFT_to_file_uploaded
              0 | ATP:0000141     |      2 | t                   | 2024-07-19 23:22:02.435325 | transition_WFT_to_file_needed
              2 | ATP:0000134     |      2 | t                   | 2024-07-20 00:55:52.980017 | transition_WFT_to_file_needed
              1 | ATP:0000134     |      2 | t                   | 2024-07-20 00:55:52.980017 | transition_WFT_to_file_needed
              0 | ATP:0000141     |      2 | t                   | 2024-07-22 01:03:17.495871 | transition_WFT_to_file_needed
              1 | ATP:0000134     |      2 | t                   | 2024-07-22 01:12:41.398536 | transition_WFT_to_file_needed
              1 | ATP:0000164     |      2 | t                   | 2024-09-05 01:16:54.36493  | default_user

Get list of problems:-
    134 is uploaded so we want to ignore anything where that is older then 6 weeks. (tried 5 times). Cannot use (164) failed as
    this will may fail every week for 5 weeks so we need to look back to when it ws uploaded (134).


"""
from os import environ, path
from sqlalchemy import text
from datetime import date, timedelta
import argparse
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.lit_processing.utils.report_utils import send_report


def get_date_weeks_ago(weeks):
    return date.today() - timedelta(weeks=weeks)


def send_report_to_slack(mod, rows_to_report):

    email_subject = f"Report on stuck {mod} Papers in workflows"
    base_dir = environ.get("LOG_PATH", "./")
    if not base_dir:
        file_path = './'
    else:
        file_path = path.join(base_dir, 'data_check/')
    log_url = environ['LOG_URL'] + "data_check/"
    log_file = file_path + f"{mod}_check_workflow_problems.log"

    with open(log_file, "w") as fw:
        for row in rows_to_report:
            fw.write(f"{row}\n")

    # email_message = "<table></tbody>" + html_rows + "</tbody></table>"
    email_message = "<p>The log file is available at " + "<a href=" + log_url + ">" + log_url + "</a><p>"
    send_report(email_subject, email_message)


def get_mod_abbreviations(db_session, debug):
    mod_abbreviations = {}
    sql = text("SELECT mod_id, abbreviation FROM mod")
    rows = db_session.execute(sql).fetchall()
    for row in rows:
        mod_abbreviations[row[0]] = row[1]
        if debug:
            print(f"Mods abbreviation: {row[0]} {row[1]}")
    return mod_abbreviations


def check_wft_in_progress(db_session, debug=True):
    in_progress = [{'current wft': ['ATP:0000198', 'ATP:0000164'],  # failed or in progress
                    'start of progress': 'ATP0000134',             # file uploaded, sets start of process
                    'set to failed': 'ATP:0000164',                # what to do on failed
                    'set to try again': 'ATP:0000162',             # what to set to if okay to try again
                    'time limit in weeks': 6,                      # if older than this, ignore
                    'slack message': True                          # if true notify slack
                    }]
    slack_messages = {}
    for phase in in_progress:
        start_date = get_date_weeks_ago(phase['time limit in weeks'])
        # get those that failed or in progress
        # dbugging code, uncomment if needed
        # if debug:  # Try via sql too as backup and testing, REMOVE later
        #    wft_str = "'" + "', '".join(phase['current wft']) + "'"
        #    sql = text(f"select reference_id, reference_workflow_tag_id"
        #               f" from workflow_tag where workflow_tag_id in ({wft_str}) and date_updated >= '{start_date}' ")
        #    print(sql)
        #    rows = db_session.execute(sql).fetchall()
        #    for row in rows:
        #        print(f"row:{row}")

        wfts = db_session.query(WorkflowTagModel).filter(WorkflowTagModel.workflow_tag_id.in_(phase['current wft']),
                                                         WorkflowTagModel.date_updated > start_date).all()

        for wft in wfts:
            # So this reference failed or is on progress so check when it was 'started'
            sql = text(f"SELECT COUNT(1) FROM workflow_tag_version "
                       f"  WHERE reference_id = {wft.reference_id} AND"
                       f"        workflow_tag_id = '{phase['start of progress']}' AND"
                       f"        date_created > '{start_date}'")
            count = db_session.execute(sql).fetchall()
            if debug:
                print(f"SQL:{sql}\tcount:{count}")
            if count:  # need to set back to try again
                if not debug:
                    if phase['slack message']:
                        if wft.mod_id not in slack_messages:
                            slack_messages[wft.mod_id] = []
                        slack_messages[wft.mod_id].append(f"Setting {wft.reference_id} to needed from {wft.workflow_tag_id}")
                    wft.workflow_tag_id = phase['set to try again']
                else:
                    print(f"Setting to try again for {wft}")
            else:      # need to set to failed
                if not debug:
                    if phase['slack message']:
                        if wft.mod_id not in slack_messages:
                            slack_messages[wft.mod_id] = []
                        slack_messages[wft.mod_id].append(f"Setting {wft.reference_id} to failed from {wft.workflow_tag_id}")
                    wft.workflow_tag_id = phase['set to failed']
                else:
                    print(f"Setting to failed for {wft}")
    db_session.commit()

    mod_abbr = {}
    for mod_id in slack_messages.keys():
        if mod_id not in mod_abbr:
            mod_abbr = get_mod_abbreviations(db_session, debug)
            send_report_to_slack(mod_abbr[mod_id], slack_messages[mod_id])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-d', '--debug', help='run in debug mode, just print', type=bool, required=False, default=False)
    args = parser.parse_args()
    db = create_postgres_session(False)
    check_wft_in_progress(db, debug=args.debug)
