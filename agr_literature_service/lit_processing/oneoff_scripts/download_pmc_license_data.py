import logging
import time
import requests
from os import environ, path
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

rootUrl = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id="
pmcRootUrl = 'https://ftp.ncbi.nlm.nih.gov/pub/pmc/'

license_file = "data/pmc_license_searched.txt"
searched_url_file = "data/searched_urls.lst"


def search_pmc_licenses():

    logger.info("Retrieving PMCID list from database...")

    # test list: pmcids = ['PMC9727813', 'PMC6798786', 'PMC4878611',
    #                      'PMC4161515', 'PMC6701924', 'PMC3901671']
    pmcids = get_pmcids()

    logger.info("Reading PMCID list with license info searched...")

    pmcids_searched = set()
    fw = None
    if path.exists(license_file):
        f = open(license_file)
        for line in f:
            pmcid = line.split("\t")[0]
            pmcids_searched.add(pmcid)
        f.close()
        fw = open(license_file, "a")
    else:
        fw = open(license_file, "w")

    logger.info("Searching PMC license...")

    count = 0
    for pmcid in pmcids:
        if pmcid not in pmcids_searched:
            count += 1
            if count % 5000 == 0:
                time.sleep(30)
            license = search_pmc_and_extract_license_info(pmcid)
            if license:
                fw.write(pmcid + "\t" + license + "\n")
    fw.close()


def get_pmcids():

    db_session = create_postgres_session(False)

    pmcids = []

    limit = 5000
    loop_count = 200000
    for index in range(loop_count):
        offset = index * limit
        logger.info(f"offset={offset} Retrieving pmcids...")
        rows = db_session.execute(f"SELECT cr.curie "
                                  f"FROM cross_reference cr, mod_corpus_association mca "
                                  f"WHERE cr.curie_prefix = 'PMCID' "
                                  f"AND cr.is_obsolete is False "
                                  f"AND cr.reference_id = mca.reference_id "
                                  f"AND mca.corpus is True "
                                  f"order by cr.reference_id "
                                  f"limit {limit} "
                                  f"offset {offset}").fetchall()
        if len(rows) == 0:
            break

        for x in rows:
            pmcid = x["curie"].replace("PMCID:", "")
            if pmcid not in pmcids:
                pmcids.append(pmcid)

    db_session.close()

    return pmcids


def search_pmc_and_extract_license_info(pmcid):

    url = rootUrl + pmcid

    if environ.get('NCBI_API_KEY'):
        url = url + "&api_key=" + environ['NCBI_API_KEY']

    logger.info("Search " + url)

    response = requests.get(url)
    content = str(response.content)
    license = None
    if "license=" in content:
        license = content.split('license=')[1].split('"')[1]
    return license


if __name__ == "__main__":

    search_pmc_licenses()
