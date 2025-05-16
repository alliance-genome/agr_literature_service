import logging
from datetime import date
from sqlalchemy import text
from os import environ, path
from shutil import copy

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def check_data():

    db = create_postgres_session(False)

    data_to_report = []
    try:
        sql_query = """ SELECT DISTINCT curie, mod.abbreviation as mod
        FROM topic_entity_tag as A, topic_entity_tag_source as B, reference, mod_corpus_association as C, mod
        WHERE A.topic_entity_tag_source_id = B.topic_entity_tag_source_id
        AND A.reference_id = reference.reference_id
        AND source_evidence_assertion = 'ATP:0000036'
        AND C.mod_id = mod.mod_id
        AND C.reference_id = reference.reference_id
        AND reference.category = 'Retraction';
        """

        rows = db.execute(text(sql_query)).mappings().fetchall()
        for row in rows:
            data_to_report.append((row.curie,row.mod))

    except Exception as e:
        logger.info(f"An error occurred when getting the data for deleted/obsolete entities. Error={e}")
        db.close()
        return
    db.close()
    write_report(data_to_report)


def write_report(data_to_report):

    log_path = environ.get('LOG_PATH', '.')
    log_file = path.join(log_path, "QC/redacted_references_with_tags.log")
    datestamp = str(date.today()).replace("-", "")
    log_file_with_datestamp = path.join(log_path, f"QC/redacted_references_with_tags_{datestamp}.log")
    with open(log_file, "w") as f:
        f.write(f"#!date-produced: {datestamp}\n")
        for curie, mod in data_to_report:
            f.write(f"{curie}\t{mod}\tRetracted\n")
    copy(log_file, log_file_with_datestamp)

if __name__ == "__main__":

    check_data()
