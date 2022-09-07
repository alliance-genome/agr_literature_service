import argparse
import logging.config
import re
import time
import urllib
from os import environ, makedirs, path
from typing import List, Set, Dict, Tuple, Union
import json

import requests
from dotenv import load_dotenv

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml.get_pubmed_xml import download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml.xml_to_json import generate_json
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import sanitize_pubmed_json_list
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_resources_nlm import update_resource_pubmed_nlm
from agr_literature_service.api.database.main import get_db
from agr_literature_service.api.models import ReferenceModel, CrossReferenceModel,\
    ModCorpusAssociationModel, ModModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import sqlalchemy_load_ref_xref
from agr_literature_service.lit_processing.utils.email_utils import send_email
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
logger = logging.getLogger('literature logger')

base_path = environ.get('XML_PATH', "")
search_path = base_path.replace("tests/", "") + 'pubmed_searches/'
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


def get_pmid_association_to_mod_via_reference(pmids: List[str], mod_abbreviation: str):
    db_session = next(get_db())
    query = db_session.query(
        CrossReferenceModel.curie,
        ReferenceModel.curie,
        ModModel.abbreviation
    ).join(
        ReferenceModel.cross_reference
    ).filter(
        CrossReferenceModel.curie.in_(pmids)
    ).outerjoin(
        ReferenceModel.mod_corpus_association
    ).outerjoin(
        ModCorpusAssociationModel.mod
    )
    results = query.all()
    pmid_curie_mod_dict: Dict[str, Tuple[Union[str, None], Union[str, None]]] = {}
    ## example: pmid_curie_mod_dict['PMID:35510023'] = ('AGR:AGR-Reference-0000862607', 'SGD')
    for result in results:
        if result[0] not in pmid_curie_mod_dict or pmid_curie_mod_dict[result[0]][1] is None:
            pmid_curie_mod_dict[result[0]] = (result[1], result[2] if result[2] == mod_abbreviation else None)
    for pmid in pmids:
        if pmid not in pmid_curie_mod_dict:
            pmid_curie_mod_dict[pmid] = (None, None)
    return pmid_curie_mod_dict


def query_pubmed_mod_updates(input_mod, reldate):
    """

    :return:
    """

    # query_pmc_mgi()			# find pmc articles for mice and 9 journals, get pmid mappings and list of pmc without pmid
    # download_pmc_without_pmid_mgi()     # download pmc xml for pmc without pmid and find their article type
    query_mods(input_mod, reldate)			# query pubmed for mod references


def query_mods(input_mod, reldate):
    """

    :return:
    """

    # to pull in new journal info from pubmed
    update_resource_pubmed_nlm()

    mod_esearch_url = {
        'FB': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=drosophil*[ALL]+OR+melanogaster[ALL]+NOT+pubstatusaheadofprint',
        'ZFIN': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=zebrafish[Title/Abstract]+OR+zebra+fish[Title/Abstract]+OR+danio[Title/Abstract]+OR+zebrafish[keyword]+OR+zebra+fish[keyword]+OR+danio[keyword]+OR+zebrafish[Mesh+Terms]+OR+zebra+fish[Mesh+Terms]+OR+danio[Mesh+Terms]',
        'SGD': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=yeast+OR+cerevisiae',
        'WB': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=elegans',
        'XB': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=(Xenopus+OR+Silurana)+AND+%22Journal+Article%E2%80%9D'
    }
    mod_daterange = {
        'FB': '&reldate=365',
        'ZFIN': '&reldate=730',
        'SGD': '&reldate=200',
        'WB': '&reldate=1825',
        'XB': '&reldate=365'
    }
    mod_false_positive_file = {
        'FB': 'FB_false_positive_pmids.txt',
        'WB': 'WB_false_positive_pmids.txt',
        'SGD': 'SGD_false_positive_pmids.txt',
        'XB': 'XB_false_positive_pmids.txt'
    }
    # source of WB FP file: https://tazendra.caltech.edu/~postgres/agr/lit/WB_false_positive_pmids

    # retrieve all cross_reference info from database
    xref_ref, ref_xref_valid, ref_xref_obsolete = sqlalchemy_load_ref_xref('reference')
    db_session = next(get_db())
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)
    mods_to_query = ['ZFIN', 'WB', 'FB', 'SGD', 'XB']
    if input_mod in mods_to_query:
        mods_to_query = [input_mod]
    pmids_posted = set()     # type: Set
    logger.info("Starting query mods")
    sleep_delay = 1
    not_loaded_pmids4mod = {}
    pmids4mod = {}
    pmids4mod['all'] = set()
    for mod in mods_to_query:
        pmids4mod[mod] = set()
        logger.info(f"Processing {mod}")
        fp_pmids = set()     # type: Set
        if mod in mod_false_positive_file:
            infile = search_path + mod_false_positive_file[mod]
            with open(infile, "r") as infile_fh:
                for line in infile_fh:
                    pmid = line.rstrip()
                    pmid = pmid.replace('PMID:', '')
                    fp_pmids.add(pmid)
        time.sleep(sleep_delay)
        url = mod_esearch_url[mod]
        if reldate:
            url = url + "&reldate=" + str(reldate)
        elif mod in mod_daterange:
            url = url + mod_daterange[mod]
        # print (" url for " + mod + "=" + url)
        f = urllib.request.urlopen(url)
        xml_all = f.read().decode('utf-8')
        pmids_to_create = []
        agr_curies_to_corpus = []
        if re.findall(r"<Id>(\d+)</Id>", xml_all):
            pmid_group = re.findall(r"<Id>(\d+)</Id>", xml_all)

            whitelist_pmids = []  # remove this later with removed block below
            for pmid in pmid_group:
                if pmid not in fp_pmids:
                    whitelist_pmids.append(pmid)
            pmids_wanted = list(map(lambda x: 'PMID:' + x, whitelist_pmids))
            pmid_curie_mod_dict = get_pmid_association_to_mod_via_reference(pmids_wanted, mod)
            # to debug
            # json_data = json.dumps(pmid_curie_mod_dict, indent=4, sort_keys=True)
            # print(mod)
            # print(json_data)
            # pmids_joined = (',').join(sorted(pmids_wanted))
            # logger.info(pmids_joined)
            # logger.info(len(pmids_wanted))
            for pmid in pmids_wanted:
                if pmid in pmids4mod['all']:
                    # the same paper already added during seacrh for other mod papers
                    pmids4mod[mod].add(pmid)
                if pmid in pmid_curie_mod_dict:
                    agr_curie = pmid_curie_mod_dict[pmid][0]
                    in_corpus = pmid_curie_mod_dict[pmid][1]
                    # to debug
                    # print(f"{pmid}\t{agr_curie}\t{in_corpus}")
                    if agr_curie is None:
                        pmids_to_create.append(pmid.replace('PMID:', ''))
                    elif in_corpus is None:
                        # print(f"add {mod} mca to {pmid} is {agr_curie}")
                        agr_curies_to_corpus.append(agr_curie)
        logger.info(f"pmids_to_create: {len(pmids_to_create)}")

        # pmids_joined = (',').join(sorted(pmids_to_create))
        # logger.info(pmids_joined)
        logger.info(f"agr_curies_to_corpus: {len(agr_curies_to_corpus)}")
        # pmids_joined = (',').join(sorted(agr_curies_to_corpus))
        # logger.info(pmids_joined)

        # connect mod pmid from search to existing abc references
        post_mca_to_existing_references(db_session, agr_curies_to_corpus, mod)

        pmids_to_process = sorted(pmids_to_create)
        # pmids_to_process = sorted(pmids_to_create)[0:2]   # smaller set to test
        logger.info(pmids_to_process)
        download_pubmed_xml(pmids_to_process)
        generate_json(pmids_to_process, [])

        (log_path, log_url, not_loaded_pmids) = check_handle_duplicate(db_session, mod,
                                                                       pmids_to_process,
                                                                       xref_ref,
                                                                       ref_xref_valid,
                                                                       ref_xref_obsolete)

        not_loaded_pmids4mod[mod] = not_loaded_pmids
        for pmid in pmids_to_process:
            pmids_posted.add(pmid)

        inject_object = {}
        mod_corpus_associations = [{"modAbbreviation": mod, "modCorpusSortSource": "mod_pubmed_search", "corpus": None}]
        inject_object['modCorpusAssociations'] = mod_corpus_associations

        # pmids_to_process = ['34849855']	# test a single pmid

        # generate json to post for these pmids and inject data not from pubmed
        sanitize_pubmed_json_list(pmids_to_process, [inject_object])

        # load new papers into database
        json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
        # process_results = post_references(json_filepath, 'no_file_check')
        # logger.info(process_results)
        post_references(json_filepath)
        # upload each processed json file to s3
        for pmid in pmids_to_process:
            # logger.info(f"upload {pmid} to s3")
            upload_xml_file_to_s3(pmid)

        set_pmid_list(db_session, mod, pmids4mod, json_filepath)

    logger.info("Sending Report")
    send_loading_report(pmids4mod, mods_to_query, log_path, log_url, not_loaded_pmids4mod)

    # do not need to recursively process downloading errata and corrections,
    # but if they exist, connect them.
    # take list of pmids that were posted to the database, look at their .json for
    # corrections and connect to existing abc references.
    # logger.info("pmids process comments corrections")
    # logger.info(pmids_posted)
    # post_comments_corrections(list(pmids_posted))
    db_session.close()
    logger.info("end query_mods")


def set_pmid_list(db_session, mod, pmids4mod, json_file):

    f = open(json_file)
    json_data = json.load(f)
    f.close()

    for x in json_data:
        if x.get('crossReferences'):
            for c in x['crossReferences']:
                if c['id'].startswith('PMID:'):
                    row = db_session.query(CrossReferenceModel).filter_by(curie=c['id']).one_or_none()
                    if row:
                        pmid = c['id'].replace('PMID:', '')
                        pmids4mod['all'].add(pmid)
                        pmids4mod[mod].add(pmid)


def send_loading_report(pmids4mod, mods, log_path, log_url, not_loaded_pmids4mod):

    email_recipients = None
    if environ.get('CRONTAB_EMAIL'):
        email_recipients = environ['CRONTAB_EMAIL']
    sender_email = None
    if environ.get('SENDER_EMAIL'):
        sender_email = environ['SENDER_EMAIL']
    sender_password = None
    if environ.get('SENDER_PASSWORD'):
        sender_password = environ['SENDER_PASSWORD']
    reply_to = sender_email
    if environ.get('REPLY_TO'):
        reply_to = environ['REPLY_TO']
    if email_recipients is None or sender_email is None:
        return

    all_pmids = pmids4mod.get('all')
    if all_pmids is None:
        return

    email_subject = "PubMed Paper Search Report"
    email_message = ""

    if len(all_pmids) == 0:

        email_message = "No new papers from PubMed Search"

    else:

        log_file = log_path + "new_papers.log"
        fw = open(log_file, "w")
        message = "Total " + str(len(all_pmids)) + " new PubMed paper(s) have been added into database"
        fw.write(message + "\n\n")
        email_message = "<h3>" + message + "</h3>"

        rows = ""
        for mod in mods:
            pmids = pmids4mod.get(mod)
            if pmids is None:
                continue
            # email_message = email_message + "<strong>" + mod + "</strong>" + " : " + str(len(pmids)) + "<br>"
            rows = rows + "<tr><th width='80'>" + mod + ":</th><td>" + str(len(pmids)) + "</td></tr>"
            fw.write(mod + ": " + str(len(pmids)) + "\n")

        email_message = email_message + "<table></tbody>" + rows + "</tbody></table>"

        rows = ''
        for mod in mods:
            if mod not in not_loaded_pmids4mod:
                continue
            not_loaded_pmids = not_loaded_pmids4mod[mod]
            for not_loaded_pmid_row in not_loaded_pmids:
                (pmid_new, doi, pmid_in_db) = not_loaded_pmid_row
                rows = rows + "<tr><th width='80'>" + mod + ":</th><td><b>PMID:" + pmid_new + "</b> was not added since its DOI:" + doi + " already exists. This DOI is associated with PMID:" + pmid_in_db + " in the database.</td></tr>"
        if rows != '':
            email_message = email_message + "<p><strong>Following new PMID(s) were not added to ABC from PubMed Search</strong><p>"
            email_message = email_message + "<table></tbody>" + rows + "</tbody></table>"

        if log_url:
            email_message = email_message + "<p>Log file(s) are available at " + "<a href=" + log_url + ">" + log_url + "</a><p>"
        else:
            email_message = email_message + "<p>Log file(s) are available at " + log_path

        fw.write("\n")
        for mod in mods:
            pmids = pmids4mod.get(mod)
            if pmids is None:
                continue
            pmids_to_report = list(map(lambda x: 'PMID:' + x, pmids))
            fw.write("New papers for " + mod + ":\n")
            fw.write("\n".join(pmids_to_report) + "\n\n")

    (status, message) = send_email(email_subject, email_recipients,
                                   email_message, sender_email, sender_password, reply_to)
    if status == 'error':
        logger.info("Failed sending email to slack: " + message + "\n")


def check_handle_duplicate(db_session, mod, pmids, xref_ref, ref_xref_valid, ref_xref_obsolete):

    # check for papers with same doi in the database
    # print ("ref_xref_valid=", str(ref_xref_valid['AGR:AGR-Reference-0000167781']))
    # ref_xref_valid= {'DOI': '10.1111/j.1440-1711.2005.01311.x', 'MGI': '3573820', 'PMID': '15748210'}
    # print ("xref_ref['DOI'][doi]=", str(xref_ref['DOI']['10.1111/j.1440-1711.2005.01311.x']))
    # xref_ref['DOI'][doi]= AGR:AGR-Reference-0000167781

    from datetime import datetime
    json_path = base_path + "pubmed_json/"
    log_path = base_path + 'pubmed_search_logs/'
    log_url = None
    if environ.get('LOG_PATH'):
        log_path = path.join(environ['LOG_PATH'], 'pubmed_search/')
        if environ.get('LOG_URL'):
            log_url = path.join(environ['LOG_URL'], 'pubmed_search/')
    if not path.exists(log_path):
        makedirs(log_path)
    log_file = log_path + "duplicate_rows_" + mod + ".log"
    fw = None
    if path.exists(log_file):
        fw = open(log_file, "a")
    else:
        fw = open(log_file, "w")
    not_loaded_pmids = []
    for pmid in pmids:
        json_file = json_path + pmid + ".json"
        f = open(json_file)
        json_data = json.load(f)
        f.close()
        cross_references = json_data['crossReferences']
        doi = None
        for c in cross_references:
            if c['id'].startswith('DOI:'):
                doi = c['id'].replace('DOI:', '')
        if doi and doi in xref_ref['DOI']:
            ## the doi for the new paper is in the database
            agr = xref_ref['DOI'][doi]
            all_ref_xref = ref_xref_valid[agr] if agr in ref_xref_valid else {}
            if agr in ref_xref_obsolete:
                # merge two dictionaries
                all_ref_xref.update(ref_xref_obsolete[agr])
            found_pmids_for_this_doi = []
            for prefix in all_ref_xref:
                if prefix == 'PMID':
                    found_pmids_for_this_doi.append(all_ref_xref[prefix])
            if len(found_pmids_for_this_doi) == 0:
                reference_id = get_reference_id_by_curie(db_session, agr)
                if reference_id is None:
                    logger.info("The reference curie: " + agr + " is not in the database.")
                try:
                    cross_ref = CrossReferenceModel(curie="PMID:" + pmid, reference_id=reference_id)
                    db_session.add(cross_ref)
                    fw.write(str(datetime.now()) + ": adding PMID:" + pmid + " to the row with doi = " + doi + " in the database\n")
                except Exception as e:
                    logger.info(str(datetime.now()) + ": adding " + pmid + " to the row with " + doi + " is failed: " + str(e) + "\n")
            else:
                fw.write(str(datetime.now()) + ": " + doi + " for PMID:" + pmid + " is associated with PMID(s) in the database: " + ",".join(found_pmids_for_this_doi) + "\n")
                not_loaded_pmids.append((pmid, doi, ",".join(found_pmids_for_this_doi)))
            pmids.remove(pmid)
    fw.close()

    return (log_path, log_url, not_loaded_pmids)


def get_reference_id_by_curie(db_session, agr):

    x = db_session.query(ReferenceModel).filter_by(curie=agr).one_or_none()
    if x:
        return x.reference_id
    return None


def post_mca_to_existing_references(db_session, agr_curies_to_corpus, mod):
    """
    :param agr_curies_to_corpus: agr reference curies to add an mca_value to
    :param mod: mod to associate
    :return:
    """

    m = db_session.query(ModModel).filter_by(abbreviation=mod).one_or_none()
    mod_id = m.mod_id

    curie_to_reference_id = {}
    for x in db_session.query(ReferenceModel).filter(
            ReferenceModel.curie.in_(agr_curies_to_corpus)).all():
        curie_to_reference_id[x.curie] = x.reference_id

    for curie in agr_curies_to_corpus:
        try:
            reference_id = curie_to_reference_id[x.curie]
            mca = ModCorpusAssociationModel(reference_id=reference_id,
                                            mod_id=mod_id,
                                            mod_corpus_sort_source='mod_pubmed_search',
                                            corpus=None)
            db_session.add(mca)
            logger.info("The mod_corpus_association row has been added into database for mod = " + mod + ", reference_curie = " + curie)
        except Exception as e:
            logger.info("An error occurred when adding mod_corpus_association for mod = " + mod + ", reference_curie = " + curie + ". error = " + str(e))

    db_session.commit()


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
