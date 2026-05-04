import argparse
import logging.config
import re
import time
import urllib
from datetime import datetime, timedelta
from os import environ, makedirs, path
from typing import Set

import requests
from dotenv import load_dotenv

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import sanitize_pubmed_json_list
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    check_handle_duplicate, add_mca_to_existing_references, mark_false_positive_papers_as_out_of_corpus, \
    process_retracted_papers
from agr_literature_service.lit_processing.utils.db_read_utils import \
    set_pmid_list, get_pmid_association_to_mod_via_reference, get_mod_abbreviations
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_resources_nlm import \
    update_resource_pubmed_nlm
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import save_database_md5data
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import (
    get_pmids_from_exclude_list,
    ExcludeListUnavailableError,
)
from agr_literature_service.api.database.main import get_db
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import sqlalchemy_load_ref_xref
from agr_literature_service.lit_processing.utils.report_utils import send_pubmed_search_report
from agr_literature_service.api.user import set_global_user_id

load_dotenv()


# pipenv run python query_pubmed_mod_updates.py

# Takes 11 minutes to process 1865 from ZFIN search between 2021 11 04 and 2022 04 18

# query pubmed for each MOD's search preferences, add PMID results into queue to download from pubmed if they've not already
# been processed by Alliance.  when ready, should output to inputs/new_results_<date>

# eutils instructions
# https://www.ncbi.nlm.nih.gov/books/NBK25499/

# SGD
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=yeast+OR+cerevisiae&retmax=100000000
# 301122
# cerevisiae	135446	yeast	300327
# sgd actually searches every 7 days what got entered into pubmed in the last 14 days
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=yeast+OR+cerevisiae&retmax=10000&reldate=14

# WB
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=elegans&retmax=100000000
# 37618

# FB
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=drosophil*[ALL]+OR+melanogaster[ALL]+NOT+pubstatusaheadofprint&retmax=100000000
# 113497
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=drosophil*+OR+melanogaster&retmax=100000000
# 113755
# drosophil*	113347	melanogaster	58772
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=%27drosophil*[ALL]%20OR%20melanogaster[ALL]%20AND%202020/07/21:2021/07/21[EDAT]%20NOT%20pubstatusaheadofprint%27&retmax=100000000


# ZFIN
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=zebrafish[Title/Abstract]&retmax=100000000
# 39363
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=zebrafish[Mesh+Terms]&retmax=100000000
# 33346
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=zebrafish[keyword]&retmax=100000000
# 9463
#
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=zebrafish[Title/Abstract]+OR+zebra+fish[Title/Abstract]+OR+danio[Title/Abstract]&retmax=100000000
# 40571
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=zebrafish[keyword]+OR+zebra+fish[keyword]+OR+danio[keyword]&retmax=100000000
# 10007
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=zebrafish[Mesh+Terms]+OR+zebra+fish[Mesh+Terms]+OR+danio[Mesh+Terms]&retmax=33346
#
# Use just this query
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=zebrafish[Title/Abstract]+OR+zebra+fish[Title/Abstract]+OR+danio[Title/Abstract]+OR+zebrafish[keyword]+OR+zebra+fish[keyword]+OR+danio[keyword]+OR+zebrafish[Mesh+Terms]+OR+zebra+fish[Mesh+Terms]+OR+danio[Mesh+Terms]&retmax=100000000
# 43304

# MGI
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("Aging+cell"[Journal])+AND+(mice)&retmax=100000000
# 952
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("Cell death and differentiation"[Journal])+AND+(mice)&retmax=100000000
# 1866
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("Cilia"[Journal])+AND+(mice)&retmax=100000000
# 9
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("Disease models mechanisms"[Journal])+AND+(mice)&retmax=100000000
# 766
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("Journal of lipid research"[Journal])+AND+(mice)&retmax=100000000
# 2431
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("Nucleic acids research"[Journal])+AND+(mice)&retmax=100000000
# 5458
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("PLoS computational biology"[Journal])+AND+(mice)&retmax=100000000
# 615
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("PLoS genetics"[Journal])+AND+(mice)&retmax=100000000
# 1700
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("PloS one"[Journal])+AND+(mice)&retmax=100000000
# 33779
#
# Use just this query
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=(("Aging+cell"[Journal])+OR+("Cell+death+and+differentiation"[Journal])+OR+("Cilia"[Journal])+OR+("Disease+models+mechanisms"[Journal])+OR+("Journal+of+lipid+research"[Journal])+OR+("Nucleic+acids+research"[Journal])+OR+("PLoS+computational+biology"[Journal])+OR+("PLoS+genetics"[Journal])+OR+("PloS+one"[Journal]))+AND+(mice)&retmax=100000000
# 47576
#
# No.  Query by PMC e.g. https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&term=("PLoS+Biology"[Journal])+AND+(mice)&retmax=100000000 get PMCIDs and pass in sets of 200 to API to map to PMID https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?tool=my_tool&idtype=pmcid&ids=8270425,8262914 then process the PMID and output list of any that don't map.  This search needs to be amended to restrict the type to "research article" because PubMed does not have abstracts, uncorrected proofs, etc. while PMC does (not sure how to add that filter yet).
#
#  XML at https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=8270425&retmode=xml  (for a PMCID) which says "research-article" and that corresponds to PMID https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=34181639&retmode=xml  which says "In-Data-Review" in PubMed.
# MGI wanted article types from https://github.com/mgijax/pdfdownload/blob/master/backPopulate.py
#     articleTypes = {'research-article' 'review-article' 'other' 'correction' 'editorial' 'article-commentary' 'brief-report' 'case-report' 'letter' 'discussion' 'retraction' 'oration' 'reply' 'news' 'expression-of-concern' }
#
# First query PMC and get mapping to PMIDs.  Then if some don't map to PMID, see if their article-type are not in the set above.  Then possibly grab all the PMC xml like https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=8270425&retmode=xml and see if the article-type in <article> matches the whitelist of articleTypes above.  Then pass results on to Monica and see if things make sense.


# SGD 301122
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=yeast+OR+cerevisiae&retmax=100000000

# WB 37618
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=elegans&retmax=100000000

# FB 113497
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=drosophil*[ALL]+OR+melanogaster[ALL]+NOT+pubstatusaheadofprint&retmax=100000000

# MGI 47576 - No clarify PMC thing
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=(("Aging+cell"[Journal])+OR+("Cell+death+and+differentiation"[Journal])+OR+("Cilia"[Journal])+OR+("Disease+models+mechanisms"[Journal])+OR+("Journal+of+lipid+research"[Journal])+OR+("Nucleic+acids+research"[Journal])+OR+("PLoS+computational+biology"[Journal])+OR+("PLoS+genetics"[Journal])+OR+("PloS+one"[Journal]))+AND+(mice)&retmax=100000000

# ZFIN 43304
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=zebrafish[Title/Abstract]+OR+zebra+fish[Title/Abstract]+OR+danio[Title/Abstract]+OR+zebrafish[keyword]+OR+zebra+fish[keyword]+OR+danio[keyword]+OR+zebrafish[Mesh+Terms]+OR+zebra+fish[Mesh+Terms]+OR+danio[Mesh+Terms]&retmax=100000000


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger(__name__)

base_path = environ.get('XML_PATH', "")
search_path = path.dirname(path.abspath(__file__)) + "/data_for_pubmed_processing/"
search_outfile_path = search_path + 'search_new_mods/'
pmc_process_path = search_path + 'pmc_processing/'
pmc_storage_path = search_path + 'pmc_processing/pmc_xml/'

if not path.exists(search_path):
    makedirs(search_path)
if not path.exists(search_outfile_path):
    makedirs(search_outfile_path)
if not path.exists(pmc_process_path):
    makedirs(pmc_process_path)
if not path.exists(pmc_storage_path):
    makedirs(pmc_storage_path)

# PubMed ESearch API limit: max 10,000 results per query
PUBMED_MAX_RESULTS = 10000
ESEARCH_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"


def build_esearch_url(term: str, retmax: int = PUBMED_MAX_RESULTS,
                      mindate: str = None, maxdate: str = None,
                      api_key: str = None) -> str:
    """
    Build a PubMed ESearch URL with the given parameters.

    Args:
        term: The search term/query
        retmax: Maximum number of results to return (default 10000, the API limit)
        mindate: Start date in YYYY/MM/DD format
        maxdate: End date in YYYY/MM/DD format
        api_key: NCBI API key for higher rate limits

    Returns:
        Complete ESearch URL
    """
    params = {
        'db': 'pubmed',
        'retmax': retmax,
        'term': term
    }
    if mindate and maxdate:
        params['mindate'] = mindate
        params['maxdate'] = maxdate
        params['datetype'] = 'edat'
    if api_key:
        params['api_key'] = api_key

    query_string = urllib.parse.urlencode(params, safe='+*[]()":/')
    return f"{ESEARCH_BASE_URL}?{query_string}"


def get_esearch_count(term: str, mindate: str = None, maxdate: str = None,
                      api_key: str = None, max_retries: int = 3) -> int:
    """
    Query PubMed ESearch to get the count of results for a search term.

    Args:
        term: The search term/query
        mindate: Start date in YYYY/MM/DD format
        maxdate: End date in YYYY/MM/DD format
        api_key: NCBI API key
        max_retries: Number of retries on network error

    Returns:
        Count of matching records

    Raises:
        Exception: If all retries fail
    """
    url = build_esearch_url(term, retmax=0, mindate=mindate, maxdate=maxdate, api_key=api_key)
    last_error = None

    for attempt in range(max_retries):
        try:
            f = urllib.request.urlopen(url)
            xml_response = f.read().decode('utf-8')
            count_match = re.search(r"<Count>(\d+)</Count>", xml_response)
            if count_match:
                return int(count_match.group(1))
            return 0
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                sleep_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                logger.warning(f"Error getting count from PubMed (attempt {attempt + 1}/{max_retries}): {e}. "
                               f"Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Error getting count from PubMed after {max_retries} attempts: {e}")

    raise Exception(f"Failed to get PubMed count after {max_retries} attempts: {last_error}")


def query_pubmed_with_date_partitioning(term: str, start_date: datetime,
                                        end_date: datetime, api_key: str = None,
                                        sleep_delay: float = 0.5,
                                        initial_count: int = None) -> Set[str]:
    """
    Query PubMed ESearch with automatic date range partitioning to handle
    the 10,000 result limit. If a query returns >= 10,000 results, the date
    range is split in half and each half is queried recursively.

    Args:
        term: The PubMed search term/query
        start_date: Start date for the search range
        end_date: End date for the search range
        api_key: NCBI API key for higher rate limits
        sleep_delay: Delay between API calls to respect rate limits
        initial_count: Optional pre-fetched count to avoid redundant API call

    Returns:
        Set of PMIDs (as strings without 'PMID:' prefix)
    """
    pmids: Set[str] = set()
    mindate_str = start_date.strftime('%Y/%m/%d')
    maxdate_str = end_date.strftime('%Y/%m/%d')

    # Use pre-fetched count if provided, otherwise fetch it
    if initial_count is not None:
        count = initial_count
    else:
        time.sleep(sleep_delay)
        count = get_esearch_count(term, mindate=mindate_str, maxdate=maxdate_str, api_key=api_key)

    logger.info(f"Date range {mindate_str} to {maxdate_str}: {count} results")

    if count == 0:
        return pmids

    if count >= PUBMED_MAX_RESULTS:
        date_diff = end_date - start_date
        if date_diff.days <= 1:
            logger.warning(f"Date range {mindate_str} to {maxdate_str} has {count} results "
                           f"but cannot split further. Retrieving first {PUBMED_MAX_RESULTS}.")
            pmids.update(_fetch_pmids_from_pubmed(term, mindate_str, maxdate_str, api_key, sleep_delay))
        else:
            mid_date = start_date + timedelta(days=date_diff.days // 2)
            logger.info(f"Splitting date range at {mid_date.strftime('%Y/%m/%d')}")

            pmids.update(query_pubmed_with_date_partitioning(
                term, start_date, mid_date, api_key, sleep_delay))
            pmids.update(query_pubmed_with_date_partitioning(
                term, mid_date + timedelta(days=1), end_date, api_key, sleep_delay))
    else:
        pmids.update(_fetch_pmids_from_pubmed(term, mindate_str, maxdate_str, api_key, sleep_delay))

    return pmids


def _fetch_pmids_from_pubmed(term: str, mindate: str, maxdate: str,
                             api_key: str = None, sleep_delay: float = 0.5,
                             max_retries: int = 3) -> Set[str]:
    """
    Fetch PMIDs from PubMed ESearch for a given term and date range.

    Args:
        term: The PubMed search term/query
        mindate: Start date in YYYY/MM/DD format
        maxdate: End date in YYYY/MM/DD format
        api_key: NCBI API key
        sleep_delay: Delay between API calls
        max_retries: Number of retries on network error

    Returns:
        Set of PMIDs (as strings without 'PMID:' prefix)

    Raises:
        Exception: If all retries fail
    """
    pmids: Set[str] = set()
    time.sleep(sleep_delay)

    url = build_esearch_url(term, retmax=PUBMED_MAX_RESULTS, mindate=mindate,
                            maxdate=maxdate, api_key=api_key)
    last_error = None

    for attempt in range(max_retries):
        try:
            f = urllib.request.urlopen(url)
            xml_response = f.read().decode('utf-8')

            pmid_matches = re.findall(r"<Id>(\d+)</Id>", xml_response)
            pmids.update(pmid_matches)
            logger.debug(f"Fetched {len(pmid_matches)} PMIDs for {mindate} to {maxdate}")
            return pmids
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                retry_sleep = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                logger.warning(f"Error fetching PMIDs from PubMed (attempt {attempt + 1}/{max_retries}): {e}. "
                               f"Retrying in {retry_sleep}s...")
                time.sleep(retry_sleep)
            else:
                logger.error(f"Error fetching PMIDs from PubMed after {max_retries} attempts: {e}")

    raise Exception(f"Failed to fetch PMIDs from PubMed after {max_retries} attempts: {last_error}")


def query_pubmed_for_mod(mod: str, term: str, reldate_days: int,
                         api_key: str = None) -> Set[str]:
    """
    Query PubMed for a MOD's search term, handling the 10,000 result limit
    by using date partitioning when necessary.

    Args:
        mod: MOD abbreviation (e.g., 'SGD', 'WB')
        term: The PubMed search term for this MOD
        reldate_days: Number of days back to search
        api_key: NCBI API key

    Returns:
        Set of PMIDs (as strings without 'PMID:' prefix)
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=reldate_days)
    mindate_str = start_date.strftime('%Y/%m/%d')
    maxdate_str = end_date.strftime('%Y/%m/%d')

    logger.info(f"Querying PubMed for {mod}: {mindate_str} to {maxdate_str} ({reldate_days} days)")

    # Get expected total count from PubMed before partitioning
    expected_count = get_esearch_count(term, mindate=mindate_str, maxdate=maxdate_str, api_key=api_key)
    logger.info(f"{mod}: PubMed reports {expected_count} total results for date range")

    # Retrieve all PMIDs using date partitioning, passing the count to avoid redundant API call
    pmids = query_pubmed_with_date_partitioning(term, start_date, end_date, api_key,
                                                initial_count=expected_count)

    # Log summary and verify we got all results
    retrieved_count = len(pmids)
    logger.info(f"{mod}: Retrieved {retrieved_count} PMIDs via date partitioning")
    if retrieved_count < expected_count:
        logger.warning(f"{mod}: Retrieved {retrieved_count} but expected {expected_count} - "
                       f"missing {expected_count - retrieved_count} PMIDs")
    elif retrieved_count > expected_count:
        logger.info(f"{mod}: Retrieved {retrieved_count} (expected {expected_count}) - "
                    f"slight variance is normal due to timing")

    return pmids


def query_pubmed_mod_updates(input_mod, reldate):
    """

    :return:
    """

    # query_pmc_mgi()			# find pmc articles for mice and 9 journals, get pmid mappings and list of pmc without pmid
    # download_pmc_without_pmid_mgi()     # download pmc xml for pmc without pmid and find their article type
    query_mods(input_mod, reldate)			# query pubmed for mod references


def query_mods(input_mod, reldate):  # noqa: C901
    """

    :return:
    """

    # to pull in new journal info from pubmed
    update_resource_pubmed_nlm()

    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&reldate=200&term=yeast+OR+cerevisiae+NOT+preprint[pt] => return 4712
    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&reldate=200&term=yeast+OR+cerevisiae => return 4745

    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&re[…]anogaster[ALL]+NOT+pubstatusaheadofprint+NOT+preprint[pt] => return 2946
    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&reldate=360&term=drosophil*[ALL]+OR+melanogaster[ALL]+NOT+pubstatusaheadofprint => return 2984

    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=zebrafish[Title/Abstract]+OR+zebra+fish[Title/Abstract]+OR+danio[Title/Abstract]+OR+zebrafish[keyword]+OR+zebra+fish[keyword]+OR+danio[keyword]+OR+zebrafish[Mesh+Terms]+OR+zebra+fish[Mesh+Terms]+OR+danio[Mesh+Terms]+NOT+preprint[pt]' => return 7967
    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=zebrafish[Title/Abstract]+OR+zebra+fish[Title/Abstract]+OR+danio[Title/Abstract]+OR+zebrafish[keyword]+OR+zebra+fish[keyword]+OR+danio[keyword]+OR+zebrafish[Mesh+Terms]+OR+zebra+fish[Mesh+Terms]+OR+danio[Mesh+Terms]+NOT+preprint[pt]' => return 7997

    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&reldate=1825&term=elegans+NOT+preprint[pt] => return 9644
    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&reldate=1825&term=elegans => return 9670

    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=(Xenopus+OR+Silurana)+AND+%22Journal+Article%E2%80%9D+NOT+preprint[pt] => return 576
    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=(Xenopus+OR+Silurana)+AND+%22Journal+Article%E2%80%9D => return 576

    # example FB URL: https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=drosophil*[ALL]+OR+melanogaster[ALL]+AND+2024/04/02:2025/04/02[EDAT]+NOT+pubstatusaheadofprint+NOT+preprint[Publication+Type]&retmax=100000000

    # today = datetime.today()
    # one_year_ago = today - timedelta(days=365)
    # date_string = f"{one_year_ago.strftime('%Y/%m/%d')}:{today.strftime('%Y/%m/%d')}"

    # 'FB': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=drosophil*[ALL]+OR+melanogaster[ALL]+AND+NOT+pubstatusaheadofprint+NOT+preprint[pt]&retmax=100000000'

    # Search terms for each MOD (without URL structure - used with date partitioning)
    mod_search_terms = {
        'FB': '(drosophil*[ALL] OR melanogaster[ALL]) NOT pubstatusaheadofprint NOT preprint[pt] NOT "epub ahead of print"[Publication Type]',
        'ZFIN': 'zebrafish[Title/Abstract] OR zebra fish[Title/Abstract] OR danio[Title/Abstract] OR zebrafish[keyword] OR zebra fish[keyword] OR danio[keyword] OR zebrafish[Mesh Terms] OR zebra fish[Mesh Terms] OR danio[Mesh Terms] NOT preprint[pt]',
        'SGD': 'yeast OR cerevisiae NOT preprint[pt]',
        'WB': 'elegans NOT preprint[pt]',
        'XB': '(Xenopus OR Silurana) AND "Journal Article" NOT preprint[pt]'
    }
    # Default date ranges in days for each MOD
    mod_default_reldate = {
        'FB': 3650,
        'ZFIN': 730,
        'SGD': 3650,
        'WB': 1825,
        'XB': 365
    }
    # retrieve all cross_reference info from database
    xref_ref, ref_xref_valid, ref_xref_obsolete = sqlalchemy_load_ref_xref('reference')
    db_session = next(get_db())
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)
    mods_to_query = get_mod_abbreviations()
    if input_mod in mods_to_query:
        mods_to_query = [input_mod]
    pmids_posted = set()     # type: Set
    logger.info("Starting query mods")
    not_loaded_pmids4mod = {}
    pmids4mod = {}
    pmids4mod['all'] = set()

    exclude_pmids = get_pmids_from_exclude_list()

    for mod in [mod for mod in mods_to_query if mod in mod_search_terms]:
        pmids4mod[mod] = set()
        logger.info(f"Processing {mod}")
        try:
            fp_pmids = get_pmids_from_exclude_list(mod)
        except ExcludeListUnavailableError as e:
            logger.error("Fatal: required MOD exclude list unavailable; aborting PubMed search run")
            send_pubmed_search_report(
                pmids4mod,
                mods_to_query,
                log_path=None,
                log_url=None,
                not_loaded_pmids4mod=not_loaded_pmids4mod,
                bad_date_published=[],
                fatal_error=str(e),
            )
            db_session.close()
            return

        # Determine date range for query
        api_key = environ.get('NCBI_API_KEY')
        if reldate:
            reldate_days = int(reldate)
        elif mod in mod_default_reldate:
            reldate_days = mod_default_reldate[mod]
        else:
            reldate_days = 365  # Default to 1 year if not specified

        # Query PubMed with date partitioning to handle >10K results
        term = mod_search_terms[mod]
        try:
            pmid_group = query_pubmed_for_mod(mod, term, reldate_days, api_key)
        except Exception as e:
            logger.error(f"Failed to query PubMed for {mod}: {e}. Skipping this MOD.")
            continue
        logger.info(f"Total PMIDs retrieved for {mod}: {len(pmid_group)}")

        pmids_to_create = []
        agr_curies_to_corpus = []
        if pmid_group:
            whitelist_pmids = [pmid for pmid in pmid_group
                               if pmid not in fp_pmids and pmid not in exclude_pmids]

            pmids_wanted = list(map(lambda x: 'PMID:' + x, whitelist_pmids))

            # Process PMIDs in chunks to avoid overwhelming the database with large IN clauses
            # The old code never passed this many PMIDs due to the 10K API limit
            CHUNK_SIZE = 5000
            pmid_curie_mod_dict = {}
            for i in range(0, len(pmids_wanted), CHUNK_SIZE):
                chunk = pmids_wanted[i:i + CHUNK_SIZE]
                logger.info(f"Checking database for PMIDs {i + 1} to {min(i + CHUNK_SIZE, len(pmids_wanted))} of {len(pmids_wanted)}")
                chunk_result = get_pmid_association_to_mod_via_reference(db_session, chunk, mod)

                # Debug: count results for this chunk
                chunk_has_ref = sum(1 for v in chunk_result.values() if v[0] is not None)
                chunk_has_mod = sum(1 for v in chunk_result.values() if v[1] is not None)
                chunk_no_ref = sum(1 for v in chunk_result.values() if v[0] is None)
                logger.info(f"  Chunk results: {chunk_has_ref} have reference, {chunk_has_mod} have {mod} MCA, {chunk_no_ref} not in DB")

                # Debug: sample some PMIDs that appear to not be in DB
                if chunk_no_ref > 0:
                    sample_missing = [k for k, v in chunk_result.items() if v[0] is None][:5]
                    logger.info(f"  Sample PMIDs marked as 'not in DB': {sample_missing}")

                pmid_curie_mod_dict.update(chunk_result)

            # Debug: summarize totals before processing
            total_has_ref = sum(1 for v in pmid_curie_mod_dict.values() if v[0] is not None)
            total_has_mod = sum(1 for v in pmid_curie_mod_dict.values() if v[1] is not None)
            total_no_ref = sum(1 for v in pmid_curie_mod_dict.values() if v[0] is None)
            logger.info(f"{mod} DB lookup summary: {total_has_ref} have reference, {total_has_mod} have {mod} MCA, {total_no_ref} not in DB")

            for pmid in pmids_wanted:
                if pmid in pmids4mod['all']:
                    # the same paper already added during search for other mod papers
                    pmids4mod[mod].add(pmid)
                if pmid in pmid_curie_mod_dict:
                    agr_curie = pmid_curie_mod_dict[pmid][0]
                    in_corpus = pmid_curie_mod_dict[pmid][1]
                    if agr_curie is None:
                        pmids_to_create.append(pmid.replace('PMID:', ''))
                    elif in_corpus is None:
                        agr_curies_to_corpus.append(agr_curie)
        logger.info(f"pmids_to_create: {len(pmids_to_create)}")

        # pmids_joined = (',').join(sorted(pmids_to_create))
        # logger.info(pmids_joined)
        logger.info(f"agr_curies_to_corpus: {len(agr_curies_to_corpus)}")
        # pmids_joined = (',').join(sorted(agr_curies_to_corpus))
        # logger.info(pmids_joined)

        # connect mod pmid from search to existing abc references
        add_mca_to_existing_references(db_session, agr_curies_to_corpus, mod, logger)

        pmids_to_process = sorted(pmids_to_create)
        # pmids_to_process = sorted(pmids_to_create)[0:2]   # smaller set to test
        logger.info(pmids_to_process)
        download_pubmed_xml(pmids_to_process)
        generate_json(pmids_to_process, [])

        (log_path, log_url, not_loaded_pmids) = check_handle_duplicate(db_session, mod,
                                                                       pmids_to_process,
                                                                       xref_ref,
                                                                       ref_xref_valid,
                                                                       logger)

        not_loaded_pmids4mod[mod] = not_loaded_pmids
        for pmid in pmids_to_process:
            pmids_posted.add(pmid)

        inject_object = {}
        mod_corpus_associations = [{"modAbbreviation": mod, "modCorpusSortSource": "mod_pubmed_search", "corpus": None}]
        inject_object['modCorpusAssociations'] = mod_corpus_associations

        # pmids_to_process = ['34849855']	# test a single pmid

        # generate json to post for these pmids and inject data not from pubmed
        bad_date_published = sanitize_pubmed_json_list(pmids_to_process, [inject_object])

        # load new papers into database
        json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
        # process_results = post_references(json_filepath, 'no_file_check')
        # logger.info(process_results)
        post_references(json_filepath)
        # upload each processed json file to s3
        for pmid in pmids_to_process:
            # logger.info(f"upload {pmid} to s3")
            upload_xml_file_to_s3(pmid)

        add_md5sum_to_database(db_session, mod, pmids_to_process)

        set_pmid_list(db_session, mod, pmids4mod, json_filepath)

        mark_false_positive_papers_as_out_of_corpus(db_session, mod, fp_pmids, logger)

    process_retracted_papers(db_session, logger)
    logger.info("Sending Report")
    send_pubmed_search_report(pmids4mod, mods_to_query, log_path, log_url, not_loaded_pmids4mod,
                              bad_date_published)

    # do not need to recursively process downloading errata and corrections,
    # but if they exist, connect them.
    # take list of pmids that were posted to the database, look at their .json for
    # corrections and connect to existing abc references.
    # logger.info("pmids process comments corrections")
    # logger.info(pmids_posted)
    # post_comments_corrections(list(pmids_posted))
    db_session.close()
    logger.info("end query_mods")


def add_md5sum_to_database(db_session, mod, pmids_to_process):  # pragma: no cover

    file = base_path + "pubmed_json/md5sum"
    pmid_to_md5sum = {}
    if path.exists(file):
        f = open(file)
        for line in f:
            pieces = line.strip().split("\t")
            pmid_to_md5sum["PMID:" + pieces[0]] = pieces[1]
    md5dict = {"PMID": pmid_to_md5sum}
    save_database_md5data(md5dict)


# find pmc articles for mice and 9 journals, get pmid mappings and list of pmc without pmid
def query_pmc_mgi():
    """

    :return:
    """

    logger.info("Starting query pmc mgi")

    idconv_slice_size = 200

    pmcid_to_pmid = ''
    pmc_without_pmid = set()

    terms = ['("Aging+cell"[Journal])+AND+(mice)', '("Cell death and differentiation"[Journal])+AND+(mice)', '("Cilia"[Journal])+AND+(mice)', '("Disease models mechanisms"[Journal])+AND+(mice)', '("Journal of lipid research"[Journal])+AND+(mice)', '("Nucleic acids research"[Journal])+AND+(mice)', '("PLoS computational biology"[Journal])+AND+(mice)', '("PLoS genetics"[Journal])+AND+(mice)', '("PloS one"[Journal])+AND+(mice)']

    # efetch to get records by id, esearch to find by terms
    pmc_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

    for term in terms:
        # parameters = {'db': 'pmc', 'retmode': 'xml', 'retmax': 10000000, 'term': '("Cilia"[Journal])+AND+(mice)'}
        parameters = {'db': 'pmc', 'retmode': 'xml', 'retmax': 10000000, 'term': term}
        r = requests.post(pmc_url, data=parameters)
        pmc_xml_all = r.text
        # logger.info(pmc_xml_all)

        if re.findall(r"<Id>(\d+)</Id>", pmc_xml_all):
            pmc_group = re.findall(r"<Id>(\d+)</Id>", pmc_xml_all)
            # print(pmc_group)
            for index in range(0, len(pmc_group), idconv_slice_size):
                pmcs_slice = pmc_group[index:index + idconv_slice_size]
                pmcs_joined = (',').join(pmcs_slice)
                logger.debug("processing PMIDs %s", pmcs_joined)

                url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?tool=my_tool&idtype=pmcid&ids=" + pmcs_joined
                # print(url)
                f = urllib.request.urlopen(url)
                xml_all = f.read().decode('utf-8')
                # logger.info(xml_all)
                if re.findall(r"<record (.*?)>", xml_all):
                    record_group = re.findall(r"<record (.*?)>", xml_all)
                    for record in record_group:
                        # print(record)
                        pmid = ''
                        pmcid = ''
                        if re.search(r"requested-id=\"(\d+)\"", record):
                            pmid_group = re.search(r"requested-id=\"(\d+)\"", record)
                            pmcid = pmid_group.group(1)
                        if re.search(r"pmid=\"(\d+)\"", record):
                            pmid_group = re.search(r"pmid=\"(\d+)\"", record)
                            pmid = pmid_group.group(1)
                        # print(pmcid + "\t" + pmid)
                        if pmid == '':
                            pmc_without_pmid.add(pmcid)
                        else:
                            pmcid_to_pmid += pmcid + "\t" + pmid + "\n"

    # print(pmcid_to_pmid)
    pmcid_to_pmid_file = pmc_process_path + 'pmcid_to_pmid'
    logger.info("Writing pmcid to pmid mappings to %s", pmcid_to_pmid_file)
    with open(pmcid_to_pmid_file, "w") as pmcid_to_pmid_file_fh:
        pmcid_to_pmid_file_fh.write(pmcid_to_pmid)

    pmcid_without_pmid_file = pmc_process_path + 'pmcid_without_pmid'
    logger.info("Writing pmc without pmid mappings to %s", pmcid_without_pmid_file)
    with open(pmcid_without_pmid_file, "w") as pmcid_without_pmid_file_fh:
        for pmc in sorted(pmc_without_pmid):
            pmcid_without_pmid_file_fh.write("%s\n" % (pmc))


# pipenv run python query_pubmed_mod_updates.py > pmc_processing/pmcid_without_pmid_article_type
# download pmc xml for pmc without pmid and find their article type
def download_pmc_without_pmid_mgi():
    """

    :return:
    """

    sleep_delay = 1
    articleTypes = {"research-article", "review-article", "other", "correction", "editorial", "article-commentary", "brief-report", "case-report", "letter", "discussion", "retraction", "oration", "reply", "news", "expression-of-concern"}

    pmcid_without_pmid_file = pmc_process_path + 'pmcid_without_pmid'
    with open(pmcid_without_pmid_file, "r") as pmcid_without_pmid_file_fh:
        for line in pmcid_without_pmid_file_fh:
            pmcid = line.rstrip()
            pmc_xml_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&retmode=xml&id=PMC" + pmcid
            time.sleep(sleep_delay)
            f_pmc_xml_url = urllib.request.urlopen(pmc_xml_url)
            xml_pmc_xml_url = f_pmc_xml_url.read().decode('utf-8')
            article_type = ''
            if re.search(r"article-type=\"(.*?)\"", xml_pmc_xml_url):
                article_type_group = re.search(r"article-type=\"(.*?)\"", xml_pmc_xml_url)
                article_type = article_type_group.group(1)
                print(pmcid + "\t" + article_type)
                if article_type in articleTypes:
                    print("ERROR " + article_type + " in " + pmcid)


# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("Cilia"[Journal])+AND+(mice)&retmax=100000000

# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=("Aging+cell"[Journal])+AND+(mice)&retmax=100000000
# No.  Query by PMC e.g. https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&term=("PLoS+Biology"[Journal])+AND+(mice)&retmax=100000000 get PMCIDs and pass in sets of 200 to API to map to PMID https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?tool=my_tool&idtype=pmcid&ids=8270425,8262914 then process the PMID and output list of any that don't map.  This search needs to be amended to restrict the type to "research article" because PubMed does not have abstracts, uncorrected proofs, etc. while PMC does (not sure how to add that filter yet).
#
#  XML at https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=8270425&retmode=xml  (for a PMCID) which
#  says "research-article" and that corresponds to PMID https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=34181639&retmode=xml
#  which says "In-Data-Review" in PubMed.
# 952

# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=PMC3555923&retmode=xml


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')
    parser.add_argument('-d', '--database', action='store_true', help='take input from database query')
    parser.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')
    parser.add_argument('-s', '--sample', action='store_true', help='test sample input from hardcoded entries')
    parser.add_argument('-u', '--url', action='store', help='take input from entries in file at url')
    ##########
    parser.add_argument('-m', '--mods', action='store', help='which mod, use all or leave blank for all', nargs='+',
                        default=['all'])
    parser.add_argument('-b', '--reldate', action='store', help='how far back to search pubmed in days for each MOD')
    args = vars(parser.parse_args())
    # pmids_wanted = []
    ## usage: query_pubmed_mod_updates.py -m SGD -b 7
    ## usage: query_pubmed_mod_updates.py -m WB -b 14
    ## usage: query_pubmed_mod_updates.py -m FB -b 14
    ## usage: query_pubmed_mod_updates.py -m ZFIN -b 14
    ## usage: query_pubmed_mod_updates.py -b 14
    ## usage: query_pubmed_mod_updates.py

    reldate = args['reldate'] if args['reldate'] else None

    for mod in args['mods']:
        query_pubmed_mod_updates(mod, reldate)
