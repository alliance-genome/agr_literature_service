import logging
from sqlalchemy import text
from os import environ, path

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.topic_entity_tag_utils import check_atp_ids_validity
from agr_literature_service.lit_processing.utils.report_utils import send_report

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

maxret = 1000


def check_data():

    db_session = create_postgres_session(False)

    distinct_values = db_session.execute(text("SELECT DISTINCT topic FROM topic_entity_tag "
                                              "UNION "
                                              "SELECT DISTINCT entity_type FROM topic_entity_tag "
                                              "UNION "
                                              "SELECT DISTINCT display_tag FROM topic_entity_tag")).fetchall()
    all_distinct_curies = [row[0] for row in distinct_values if row[0] and row[0].startswith('ATP:')]

    logger.info(f"Total {len(all_distinct_curies)} unique ATP IDs are in topic_entity_tag table.")

    (valid_curies, atp_to_name) = check_atp_ids_validity(all_distinct_curies)
    obsolete_disappeared_curies = set(all_distinct_curies) - valid_curies

    logger.info(f"{len(obsolete_disappeared_curies)} out of {len(all_distinct_curies)} ATP ID(s) are obsolete or disappeared from A-team ATP table")

    mod_to_report = {}
    for atp_curie in obsolete_disappeared_curies:
        rows = db_session.execute(text(f"SELECT r.curie, m.abbreviation, tet.topic, tet.entity_type, tet.display_tag  "
                                       f"FROM reference r, mod m, mod_corpus_association mca, topic_entity_tag tet "
                                       f"WHERE (tet.topic = '{atp_curie}' or tet.entity_type = '{atp_curie}' "
                                       f"       or tet.display_tag = '{atp_curie}') "
                                       f"AND tet.reference_id = r.reference_id "
                                       f"AND tet.reference_id = mca.reference_id "
                                       f"AND mca.corpus is True "
                                       f"AND mca.mod_id = m.mod_id")).fetchall()

        cols_to_check = ['topic', 'entity_type']
        for row in rows:
            col_name = "display_tag"
            for col in cols_to_check:
                if row[col] == atp_curie:
                    col_name = col
                    break
            mod = row['abbreviation']
            report_rows = mod_to_report.get(mod, [])
            report_rows.append((row['curie'], atp_curie, col_name,
                                row['topic'], row['entity_type']))
            mod_to_report[mod] = report_rows
    db_session.close()

    if mod_to_report:
        for mod in mod_to_report:
            logger.info(f"Sending report for {mod}...")
            send_report_to_slack(mod, mod_to_report[mod], atp_to_name)


def send_report_to_slack(mod, rows_to_report, atp_to_name):

    email_subject = f"Report on Obsolete or Disappeared ATP IDs for {mod} Papers from the topic_entity_tag Table"

    file_path = path.join(environ['LOG_PATH'], 'data_check/')
    log_url = environ['LOG_URL'] + "data_check/"
    log_file = file_path + f"{mod}_check_atp.log"

    with open(log_file, "w") as fw:
        fw.write("Ref_curie\tTopic\tEntity_type\tInvalid_ATP\tCol with invalid ATP\n")
        html_rows = ""
        for count, row_data in enumerate(rows_to_report, start=1):
            ref_curie, invalid_atp, col_with_invalid_atp, topic, entity_type = row_data
            topic_name = atp_to_name.get(topic)
            entity_type_name = atp_to_name.get(entity_type)

            fw.write(f"{ref_curie}\t{topic_name}\t{entity_type_name}\t{invalid_atp}\t{col_with_invalid_atp}\n")

            if count <= 10:
                if count == 1:
                    html_rows += ("<tr><th style='text-align:left' width=200>Reference_curie</th>"
                                  "<th style='text-align:left' width=100>Topic</th>"
                                  "<th style='text-align:left' width=100>Entity_type</th>"
                                  "<th style='text-align:left' width=135>Invalid_atp_curie</th>"
                                  "<th style='text-align:left'>Col_with_invalid_atp</th></tr>")
                html_rows += f"<tr><td>{ref_curie}</td><td>{topic_name or 'null'}</td><td>{entity_type_name or 'null'}</td><td>{invalid_atp}</td><td>{col_with_invalid_atp}</td></tr>"

    email_message = "<table></tbody>" + html_rows + "</tbody></table>"
    email_message = email_message + "<p>The log file is available at " + "<a href=" + log_url + ">" + log_url + "</a><p>"
    send_report(email_subject, email_message)


if __name__ == "__main__":

    check_data()
