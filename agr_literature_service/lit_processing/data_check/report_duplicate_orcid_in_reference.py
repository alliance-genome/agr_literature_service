import logging
from sqlalchemy import text
from os import environ, path
from shutil import copy
from datetime import date

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def check_data():
    data_to_report = []
    for mod_abbr in get_mod_abbreviations():
        logger.info(f"Getting pmids for {mod_abbr}.")
        db = create_postgres_session(False)
        db_rows = get_all_author_duplicate_orcid_reference_for_mod(db, mod_abbr)
        db.close()
        for row in db_rows:
            curie, orcid, author_names = row
            data_to_report.append((mod_abbr, curie, orcid, author_names))
    write_report(data_to_report)


def get_all_author_duplicate_orcid_reference_for_mod(db, mod_abbr):
    sql = text("""
        SELECT
            r.curie,
            a.orcid,
            STRING_AGG(a.name, ' | ' ORDER BY a."order") AS author_names
        FROM public.author a
        JOIN public.reference r ON a.reference_id = r.reference_id
        JOIN public.mod_corpus_association mca ON a.reference_id = mca.reference_id
        JOIN public.mod m ON mca.mod_id = m.mod_id
        WHERE m.abbreviation = :mod_abbr
          AND mca.corpus = TRUE
          AND a.orcid IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM public.author b
              WHERE b.orcid = a.orcid
                AND b.reference_id = a.reference_id
                AND b.author_id <> a.author_id
          )
        GROUP BY r.curie, a.orcid
        ORDER BY r.curie, a.orcid;
    """)
    rows = db.execute(sql, {"mod_abbr": mod_abbr}).fetchall()
    return rows


def write_report(data_to_report):
    log_path = environ.get('LOG_PATH', '.')
    log_file = path.join(log_path, "QC/duplicate_orcid_report.log")
    print("log_file=", log_file)
    datestamp = str(date.today()).replace("-", "")
    log_file_with_datestamp = path.join(log_path, f"QC/duplicate_orcid_report_{datestamp}.log")
    with open(log_file, "w") as f:
        f.write(f"#!date-produced: {datestamp}\n")
        for row in data_to_report:
            mod_abbr, curie, orcid, author_names = row
            f.write(f"{mod_abbr}\t{curie}\t{orcid}\t{author_names}\n")
    copy(log_file, log_file_with_datestamp)


if __name__ == "__main__":

    check_data()
