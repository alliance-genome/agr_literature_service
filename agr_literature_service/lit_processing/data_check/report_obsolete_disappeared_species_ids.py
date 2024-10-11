import logging
from sqlalchemy import text
from os import environ, path

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.topic_entity_tag_utils import get_map_ateam_curies_to_names
from agr_literature_service.lit_processing.utils.report_utils import send_report

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

maxret = 1000


def check_data():

    db_session = create_postgres_session(False)

    distinct_values = db_session.execute(text("SELECT DISTINCT species FROM topic_entity_tag")).fetchall()
    all_distinct_curies = [row[0] for row in distinct_values if row[0] and row[0].startswith('NCBITaxon:')]

    logger.info(f"Total {len(all_distinct_curies)} unique species are in topic_entity_tag table.")

    valid_curies = get_map_ateam_curies_to_names("ncbitaxonterm", all_distinct_curies)
    obsolete_disappeared_curies = set(all_distinct_curies) - set(valid_curies)
    logger.info(f"{len(obsolete_disappeared_curies)} out of {len(all_distinct_curies)} NCBITaxon ID(s) are obsolete or disappeared from A-team ATP table")

    mod_to_report = {}
    for atp_curie in obsolete_disappeared_curies:
        rows = db_session.execute(text(f"SELECT r.curie, m.abbreviation "
                                       f"FROM reference r, mod m, mod_corpus_association mca, topic_entity_tag tet "
                                       f"WHERE tet.species = '{atp_curie}' "
                                       f"AND tet.reference_id = r.reference_id "
                                       f"AND tet.reference_id = mca.reference_id "
                                       f"AND mca.corpus is True "
                                       f"AND mca.mod_id = m.mod_id")).fetchall()
        for row in rows:
            mod = row['abbreviation']
            report_rows = mod_to_report.get(mod, [])
            report_rows.append((row['curie'], atp_curie))
            mod_to_report[mod] = report_rows
    db_session.close()

    # logger.info(f"MODS:-\n{mod_to_report}")
    for mod in mod_to_report:
        logger.info(f"Sending report for {mod}...")
        send_report_to_slack(mod, mod_to_report[mod])


def send_report_to_slack(mod, rows_to_report):

    email_subject = f"Report on Obsolete or Disappeared species for {mod} Papers from the topic_entity_tag Table"

    file_path = path.join(environ['LOG_PATH'], 'data_check/')
    log_url = environ['LOG_URL'] + "data_check/"
    log_file = file_path + f"{mod}_check_species.log"

    with open(log_file, "w") as fw:
        fw.write("Ref_curie\tspecies\n")
        html_rows = ""
        for count, row_data in enumerate(rows_to_report, start=1):
            ref_curie, species = row_data
            fw.write(f"{ref_curie}\t{species}\n")

            if count <= 10:
                if count == 1:
                    html_rows += ("<tr><th style='text-align:left' width=200>Reference_curie</th>"
                                  "<th style='text-align:left' width=100>Species</th></tr>\n")
                html_rows += f"<tr><td>{ref_curie}</td><td>{species or 'null'}</td></tr>"

    email_message = "<table></tbody>" + html_rows + "</tbody></table>"
    email_message = email_message + "<p>The log file is available at " + "<a href=" + log_url + ">" + log_url + "</a><p>"
    send_report(email_subject, email_message)


if __name__ == "__main__":

    check_data()
