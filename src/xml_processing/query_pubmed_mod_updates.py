import argparse
import logging
import logging.config
import re
import time
import urllib
# import glob
# import hashlib
from datetime import datetime
# import os
from os import environ, makedirs, path
from typing import List, Set, Dict, Tuple, Union

import requests
from dotenv import load_dotenv

from helper_file_processing import (generate_cross_references_file,
                                    load_ref_xref)

from literature.database.main import get_db
from literature.models import ReferenceModel, CrossReferenceModel, ModCorpusAssociationModel, ModModel


load_dotenv()


# pipenv run python query_pubmed_mod_updates.py

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


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


base_path = environ.get('XML_PATH', "")
search_path = base_path + 'pubmed_searches/'
search_outfile_path = base_path + 'pubmed_searches/search_new_mods/'
pmc_process_path = base_path + 'pubmed_searches/pmc_processing/'
pmc_storage_path = base_path + 'pubmed_searches/pmc_processing/pmc_xml/'

if not path.exists(search_path):
    makedirs(search_path)
if not path.exists(search_outfile_path):
    makedirs(search_outfile_path)
if not path.exists(pmc_process_path):
    makedirs(pmc_process_path)
if not path.exists(pmc_storage_path):
    makedirs(pmc_storage_path)


#     'FB': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=%27drosophil*[ALL]%20OR%20melanogaster[ALL]%20AND%202020/07/21:2021/07/21[EDAT]%20NOT%20pubstatusaheadofprint%27&retmax=100000000',
#     'FB': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=drosophil*[ALL]+OR+melanogaster[ALL]+NOT+pubstatusaheadofprint&retmax=100000000',

# alliance_pmids = set()     # type: Set


def get_pmid_association_to_mod_via_reference(pmids: List[str], mod_abbreviation: str):
    db_session = next(get_db())
    query = db_session.query(
        CrossReferenceModel.curie,
        ReferenceModel.curie,
        ModModel.abbreviation
    ).join(
        ReferenceModel.cross_references
    ).filter(
        CrossReferenceModel.curie.in_(pmids)
    ).outerjoin(
        ReferenceModel.mod_corpus_association
    ).outerjoin(
        ModCorpusAssociationModel.mod
    )
    results = query.all()
    pmid_curie_mod_dict: Dict[str, Tuple[Union[str, None], Union[str, None]]] = {}
    for result in results:
        if result[0] not in pmid_curie_mod_dict or pmid_curie_mod_dict[result[0]][1] is None:
            pmid_curie_mod_dict[result[0]] = (result[1], result[2] if result[2] == mod_abbreviation else None)
    for pmid in pmids:
        if pmid not in pmid_curie_mod_dict:
            pmid_curie_mod_dict[pmid] = (None, None)
    return pmid_curie_mod_dict


def query_pubmed_mod_updates():
    """

    :return:
    """

    # query_pmc_mgi()			# find pmc articles for mice and 9 journals, get pmid mappings and list of pmc without pmid
    # download_pmc_without_pmid_mgi()     # download pmc xml for pmc without pmid and find their article type
    query_mods()			# query pubmed for mod references


def query_mods():
    """

    :return:
    """

    mod_esearch_url = {
        'FB': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=drosophil*[ALL]+OR+melanogaster[ALL]+NOT+pubstatusaheadofprint',
        'ZFIN': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=zebrafish[Title/Abstract]+OR+zebra+fish[Title/Abstract]+OR+danio[Title/Abstract]+OR+zebrafish[keyword]+OR+zebra+fish[keyword]+OR+danio[keyword]+OR+zebrafish[Mesh+Terms]+OR+zebra+fish[Mesh+Terms]+OR+danio[Mesh+Terms]',
        'SGD': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=yeast+OR+cerevisiae',
        'WB': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=100000000&term=elegans'
    }
    # how far back to search pubmed in days for each MOD
    mod_daterange = {
        'FB': '&reldate=365',
        'ZFIN': '&reldate=730',
        'SGD': '&reldate=14',
        'WB': '&reldate=1825'
    }
    mod_false_positive_file = {
        'FB': 'FB_fp_PMIDs_20210728.txt',
        'WB': 'WB_false_positive_pmids',
        'SGD': 'SGD_referencedeletedpmids_20210803.csv'
    }
    mods_to_query = ['FB', 'SGD', 'WB', 'ZFIN']

    alliance_pmids = populate_alliance_pmids()

    logger.info("Starting query mods")
    search_output = ''
    sleep_delay = 1
    for mod in mods_to_query:
        fp_pmids = set()
        if mod in mod_false_positive_file:
            infile = search_path + mod_false_positive_file[mod]
            with open(infile, "r") as infile_fh:
                for line in infile_fh:
                    pmid = line.rstrip()
                    pmid = pmid.replace('PMID:', '')
                    fp_pmids.add(pmid)
        time.sleep(sleep_delay)
        url = mod_esearch_url[mod]
        if mod in mod_daterange:
            url = url + mod_daterange[mod]
        f = urllib.request.urlopen(url)
        xml_all = f.read().decode('utf-8')
        if re.findall(r"<Id>(\d+)</Id>", xml_all):
            pmid_group = re.findall(r"<Id>(\d+)</Id>", xml_all)
            new_pmids = []
            for pmid in pmid_group:
                if pmid not in alliance_pmids and pmid not in fp_pmids:
                    new_pmids.append(pmid)
                # new_pmids.append(pmid)
            logger.info("%s search pmids not in alliance count : %s", mod, len(new_pmids))
            search_output += mod + " search pmids not in alliance count : " + str(len(new_pmids)) + "\n"
            pmids_joined = (',').join(sorted(new_pmids))
            # logger.info(pmids_joined)
            search_output += pmids_joined + "\n"
    now = datetime.now()
    date = now.strftime("%Y%m%d")
    search_output_file = search_outfile_path + 'search_new_mods_' + date
    with open(search_output_file, "w") as search_output_file_fh:
        search_output_file_fh.write(search_output)


def populate_alliance_pmids():
    """

    :return:
    """

    alliance_pmids = set()     # type: Set

    # old way using flatfile from original population
    # infile = base_path + 'inputs/alliance_pmids'
    # with open(infile, "r") as infile_fh:
    #     for line in infile_fh:
    #         pmid = line.rstrip()
    #         alliance_pmids.add(pmid)

    generate_cross_references_file('reference')   # this updates from references in the database, and takes 88 seconds. if updating this script, comment it out after running it once
    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('reference')
    for pmid in xref_ref['PMID']:
        alliance_pmids.add(pmid)
    return alliance_pmids


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

# def download_pubmed_xml(pmids_wanted):
#     # 4.5 minutes to download 28994 wormbase records in 10000 chunks
#     # 61 minutes to download 429899 alliance records in 10000 chunks
#     # 127 minutes to download 646714 alliance records in 5000 chunks, failed on 280
#     pmids_slice_size = 5000
#
#     if not path.exists(storage_path):
#         makedirs(storage_path)
#
#     # comparing through a set instead of a list takes 2.6 seconds instead of 4256
#     pmids_found = set()
#
#     # this section reads pubmed xml files already acquired to skip downloading them.
#     # to get full set, clear out storage_path, or comment out this section
#     logger.info("Reading PubMed XML previously acquired")
#     full_path_pmid_xml = glob.glob(storage_path + "*.xml")
#     pmids_wanted_set = set(pmids_wanted)
#     for elem in full_path_pmid_xml:
#         elem = elem.replace(storage_path, '')
#         elem = elem.replace('.xml', '')
#         if elem in pmids_wanted_set:
#             pmids_wanted_set.remove(elem)
#     pmids_wanted = sorted(list(pmids_wanted_set))
#
# #     for pmid in pmids_wanted:
# #         print(pmid)
#
#     logger.info("Starting download of new PubMed XML")
#
#     md5dict = {}
#     md5file = storage_path + 'md5sum'
#     if path.exists(md5file):
#         logger.info("Reading previous md5sum mappings from %s", md5file)
#         with open(md5file, "r") as md5file_fh:
#             for line in md5file_fh:
#                 line_data = line.split("\t")
#                 if line_data[0]:
#                     md5dict[line_data[0]] = line_data[1].rstrip()
#
#     for index in range(0, len(pmids_wanted), pmids_slice_size):
#         pmids_slice = pmids_wanted[index:index + pmids_slice_size]
#         pmids_joined = (',').join(pmids_slice)
#         logger.debug("processing PMIDs %s", pmids_joined)
#
# #         https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=10074449&retmode=xml
#
# #         default way without a library, using get
# #         url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + pmids_joined + "&retmode=xml"
# #         print url
# #         f = urllib.urlopen(url)
# #         xml_all = f.read()
#
# #         using post with requests library, works well for 10000 pmids
#         url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
#         parameters = {'db': 'pubmed', 'retmode': 'xml', 'id': pmids_joined}
#         r = requests.post(url, data=parameters)
#         xml_all = r.text
# #         xml_all = r.text.encode('utf-8').strip()		# python2
#         xml_split = xml_all.split("\n<Pubmed")		# some types are not PubmedArticle, like PubmedBookArticle, e.g. 32644453
#
#         header = xml_split.pop(0)
#         header = header + "\n<Pubmed" + xml_split.pop(0)
#         footer = "\n\n</PubmedArticleSet>"
#
#         for n in range(len(xml_split)):
#             xml_split[n] = header + "\n<Pubmed" + xml_split[n]
#             xml_split[n] = os.linesep.join([s for s in xml_split[n].splitlines() if s])
#
#         for n in range(len(xml_split) - 1):
#             xml_split[n] += footer
#
#         for xml in xml_split:
#             if re.search(r"<PMID[^>]*?>(\d+)</PMID>", xml):
#                 pmid_group = re.search(r"<PMID[^>]*?>(\d+)</PMID>", xml)
#                 pmid = pmid_group.group(1)
#                 pmids_found.add(pmid)
#                 filename = storage_path + pmid + '.xml'
#                 f = open(filename, "w")
#                 f.write(xml)
#                 f.close()
#                 md5sum = hashlib.md5(xml.encode('utf-8')).hexdigest()
#                 md5dict[pmid] = md5sum
#                 # md5data += pmid + "\t" + md5sum + "\n"
#
#         if len(pmids_slice) == pmids_slice_size:
#             logger.info("waiting to process more pmids")
#             time.sleep(5)
#
#     # md5file = storage_path + 'md5sum'
#     logger.info("Writing md5sum mappings to %s", md5file)
#     with open(md5file, "w") as md5file_fh:
#         # md5file_fh.write(md5data)
#         for key in sorted(md5dict.keys(), key=int):
#             md5file_fh.write("%s\t%s\n" % (key, md5dict[key]))
#
#     logger.info("Writing log of pmids_not_found")
#     output_pmids_not_found_file = base_path + 'pmids_not_found'
#     with open(output_pmids_not_found_file, "w") as pmids_not_found_file:
#         for pmid in pmids_wanted:
#             if pmid not in pmids_found:
#                 pmids_not_found_file.write("%s\n" % (pmid))
#                 logger.info("PMID %s not found in pubmed query", pmid)
#         pmids_not_found_file.close()
#
#     logger.info("Getting PubMed XML complete")
#
#
# # to process one by one
# #   for pmid in pmids_wanted:
# # #    add some validation here
# #     url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + pmid + "&retmode=xml"
# #     filename = storage_path + pmid + '.xml'
# # #     print url
# # #     print filename
# #     logger.info("Downloading %s into %s", url, filename)
# #     urllib.urlretrieve(url, filename)
# #     time.sleep( 5 )


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')
    parser.add_argument('-d', '--database', action='store_true', help='take input from database query')
    parser.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')
    parser.add_argument('-r', '--restapi', action='store', help='take input from rest api')
    parser.add_argument('-s', '--sample', action='store_true', help='test sample input from hardcoded entries')
    parser.add_argument('-u', '--url', action='store', help='take input from entries in file at url')

    args = vars(parser.parse_args())

    pmids_wanted = []     # type: List

    query_pubmed_mod_updates()

# #    python query_pubmed_mod_updates.py -d
#     if args['database']:
#         logger.info("Processing database entries")
#
#     elif args['restapi']:
#         logger.info("Processing rest api entries")
#
# #     python query_pubmed_mod_updates.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/pmid_file.txt
#     elif args['file']:
#         logger.info("Processing file input from %s", args['file'])
#         with open(args['file'], 'r') as fp:
#             pmid = fp.readline()
#             while pmid:
#                 pmids_wanted.append(pmid.rstrip())
#                 pmid = fp.readline()
#
# #     python query_pubmed_mod_updates.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
#     elif args['url']:
#         logger.info("Processing url input from %s", args['url'])
#         req = urllib.request.urlopen(args['url'])
#         data = req.read()
#         lines = data.splitlines()
#         for pmid in lines:
#             pmids_wanted.append(str(int(pmid)))
#
# #    python query_pubmed_mod_updates.py -c 1234 4576 1828
#     elif args['commandline']:
#         logger.info("Processing commandline input")
#         for pmid in args['commandline']:
#             pmids_wanted.append(pmid)
#
# #    python query_pubmed_mod_updates.py -s
#     elif args['sample']:
#         logger.info("Processing hardcoded sample input")
#         pmid = '12345678'
#         pmids_wanted.append(pmid)
#         pmid = '12345679'
#         pmids_wanted.append(pmid)
#         pmid = '12345680'
#         pmids_wanted.append(pmid)
#
#     else:
#         logger.info("Processing database entries")
#
#     download_pubmed_xml(pmids_wanted)
