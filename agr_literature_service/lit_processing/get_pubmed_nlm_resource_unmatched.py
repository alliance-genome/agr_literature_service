
import logging
import logging.config
import re
import time
from os import environ, path
from typing import List, Set

import requests
from dotenv import load_dotenv


load_dotenv()


# pipenv run python get_pubmed_nlm_resource_unmatched.py

# for cleanup, see which dqm resourceAbbreviations don't match NLM data from J_Medline.txt
# and query pubmed from Kimberly's query to try to find info.

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


base_path = environ.get('XML_PATH', "")
storage_path = base_path + 'resource_xml/'


pmids = []              # type: List
pmids_found = set()     # type: Set


def download_pubmed_unmatched_resource_xml():
    """

    :return:
    """

    resource_abbreviation_not_found_filename = storage_path + 'resource_abbreviation_not_matched'
    resource_abbreviations = []
    logger.info("Processing file input from %s", resource_abbreviation_not_found_filename)
    with open(resource_abbreviation_not_found_filename, 'r') as fp:
        line = fp.readline()
        while line:
            resource_abbreviations.append(line.rstrip())
            line = fp.readline()
        fp.close()
    counter = 0
    max_count = 3000
    for resource_abbreviation in resource_abbreviations:
        counter += 1
        if counter > max_count:
            break
        logger.info(resource_abbreviation)
#         url = https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=nlmcatalog&term=Revue%20de%20Nematologie%5BAll%20Fields%5D&cmd=DetailsSearch
        url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=nlmcatalog&term=' \
              + resource_abbreviation + '%5BAll%20Fields%5D&cmd=DetailsSearch'
        r = requests.post(url)
        xml_all = r.text
        print(xml_all)
        filename = storage_path + simplify_text(resource_abbreviation) + '.xml'
        f = open(filename, "w")
        f.write(xml_all)
        f.close()
        time.sleep(5)


def simplify_text(text):
    """

    :param text:
    :return:
    """

    no_html = re.sub('<[^<]+?>', '', text)
    stripped = re.sub("[^a-zA-Z]+", "", no_html)
    clean = stripped.lower()

    return clean


if __name__ == "__main__":
    """
    call main start function
    """

    download_pubmed_unmatched_resource_xml()
