import logging
from sqlalchemy import text
from os import environ, path
import requests
from shutil import copy
import time
from datetime import date

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
SLEEP_TIME = 0.2


def check_and_fix_data():

    data_to_report = []
    all_invalid_pmids = set()
    for mod_abbr in get_mod_abbreviations():
        logger.info(f"Getting pmids for {mod_abbr}.")
        db = create_postgres_session(False)
        all_pmids = get_all_pmids_for_mod(db, mod_abbr)
        db.close()

        logger.info(f"Finding invalid pmids for {mod_abbr}.")
        invalid_pmids = find_invalid_pmids(all_pmids, mod_abbr)
        logger.info(f"invalid_pmids for {mod_abbr}: {invalid_pmids}")

        for pmid in invalid_pmids:
            data_to_report.append((mod_abbr, f"PMID:{pmid}"))
            all_invalid_pmids.add(f"PMID:{pmid}")

    logger.info("Updating is_obsolete for invalid PMIDs")
    update_cross_reference(all_invalid_pmids)

    logger.info("Writing report")
    write_report(data_to_report)


def write_report(data_to_report):

    log_path = environ.get('LOG_PATH', '.')
    log_file = path.join(log_path, "QC/obsolete_pmid_report.log")
    print("log_file=", log_file)
    datestamp = str(date.today()).replace("-", "")
    log_file_with_datestamp = path.join(log_path, f"QC/obsolete_pmid_report_{datestamp}.log")
    with open(log_file, "w") as f:
        f.write(f"#!date-produced: {datestamp}\n")
        for (mod_abbr, pmid) in data_to_report:
            f.write(f"{mod_abbr}\t{pmid}\n")
    copy(log_file, log_file_with_datestamp)


def update_cross_reference(all_invalid_pmids):
    db = create_postgres_session(False)
    for pmid in all_invalid_pmids:
        sql = text("""
            UPDATE cross_reference
            SET is_obsolete = True
            WHERE curie = :pmid
        """)
        db.execute(sql, {"pmid": pmid})
        logger.info(f"Making {pmid} invalid")
    db.rollback()
    # db.commit()
    db.close()


def get_all_pmids_for_mod(db, mod_abbr):

    sql = text("""
        SELECT cr.curie
        FROM cross_reference AS cr
        JOIN mod_corpus_association AS mca ON cr.reference_id = mca.reference_id
        JOIN mod AS m ON mca.mod_id = m.mod_id
        WHERE cr.curie_prefix = 'PMID'
          AND cr.is_obsolete   = FALSE
          AND mca.corpus       = TRUE
          AND m.abbreviation   = :mod_abbr
    """)
    rows = db.execute(sql, {"mod_abbr": mod_abbr}).fetchall()
    return [curie.replace('PMID:', '') for (curie,) in rows]


def find_invalid_pmids(pmids, mod_abbr):
    """
    Query NCBI’s ESummary in chunks and return a sorted list of PMIDs that are invalid—
    whose summary dict contains an 'error' key.
    """
    invalid = set()

    for i in range(0, len(pmids), 200):
        chunk = pmids[i : i + 200]
        logger.info(f"{i+1}-{min(i+200, len(pmids))}: checking {mod_abbr} PMIDs…")

        params = {
            "db": "pubmed",
            "id": ",".join(chunk),
            "retmode": "json",
            "api_key": environ.get("NCBI_API_KEY", "")
        }
        resp = requests.get(BASE, params=params)

        # skip this chunk on HTTP error
        if resp.status_code != 200:
            logger.info(f"ESummary HTTP {resp.status_code} for IDs {chunk[:3]}…")
            continue

        # parse JSON
        try:
            data = resp.json()
        except ValueError:
            logger.info("Invalid JSON response:", resp.text[:200])
            continue

        result = data.get("result")
        if not isinstance(result, dict) or "uids" not in result:
            logger.info("Unexpected JSON shape (no result.uids):", data.keys())
            continue

        uids = result["uids"] or []

        # any uid whose summary contains an 'error' field is invalid
        for uid in uids:
            entry = result.get(uid, {})
            if "error" in entry:
                invalid.add(uid)
        time.sleep(SLEEP_TIME)

    return sorted(invalid)


if __name__ == "__main__":

    check_and_fix_data()
