# Paulo Nuin Oct 2021

"""
manage.py
=========
"""

import logging
import os
import sys
import urllib

import click
import coloredlogs
from lib import get_dqm_data, get_pubmed_xml, xml_to_json, parse_dqm_json_reference

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")


@click.command()
@click.option("-c","--commandline", "cli", multiple=True, help="take input from command line flag", required=False)
@click.option("-d", "--database", "db", help="take input from database query", required=False)
@click.option("-f", "--file", "ffile", help="take input from entries in file with full path", required=False)
@click.option("-r", "--restapi", "api", help="take input from rest api", required=False)
@click.option("-s", "--sample", "sample", help="test sample input from hardcoded entries", required=False,
              default=False, is_flag=True)
@click.option("-u", "--url", "url", help="take input from entries in file at url", required=False)
@click.option("-D", "--dqm", "dqm", help="get the DQM data", required=False, is_flag=True)
@click.option("-x", "--xml", "xml", help="convert XML files to JSON", required=False)
def run_pipeline(cli, db, ffile, api, sample, url, dqm, xml):
    """

    :param cli:
    :param db:
    :param ffile:
    :param api:
    :param sample:
    :param url:
    :return:
    """

    # set storage location
    if len(os.environ.get("XML_PATH")) == 0:
        logger.warning("XML_PATH not set")
        base_path = os.getcwd()
    else:
        base_path = os.environ.get("XML_PATH")

    storage_path = base_path + "/pubmed_xml/"
    logger.info(f"Base path is at {base_path}")
    logger.info(f"XMLs will be saved on {storage_path}")

    pmids = []  # list that will contain the PMIDs to be downloaded

    # checking parameters
    if db:
        # python get_pubmed_xml.py -d
        logger.info("Processing database entries")
    elif api:
        # python get_pubmed_xml.py -r
        logger.info("Processing rest api entries")
    elif ffile:
        # python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/pmid_file.txt
        logger.info("Processing file input from " + ffile)
        # this requires a well structured input
        pmids = open(ffile).read().splitlines()
    elif url:
        # python get_pubmed_xml.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
        logger.info("Processing url input from %s", url)
        req = urllib.request.urlopen(url)
        lines = req.read().splitlines()
        for pmid in lines:
            pmids.append(str(int(pmid)))
    elif cli:
        # python get_pubmed_xml.py -c 1234 4576 1828
        logger.info("Processing commandline input")
        for pmid in cli:
            pmids.append(pmid)
    elif sample:
        # python get_pubmed_xml.py -s
        logger.info("Processing hardcoded sample input")
        pmids = ["12345678", "12345679", "12345680", "21290765", "33054145",
                 "21413221", "28304499", "28308877", "9684897", "27899353",
                 "30979869", "30003105", "30002370", "2993907"]
    elif dqm:
        logger.info("Getting DQM data")
        get_dqm_data.download_dqm_json(base_path)
        logger.info("Getting the PMIDs")
        parse_dqm_json_reference.generate_pmid_data(base_path, os.path.join(base_path, "dqm_data/REFERENCE/output"))
    elif xml:
        logger.info("Converting XML to JSON")
        xml_to_json.process_tasks(ffile)
        sys.exit()

    if len(pmids) > 0:
        logger.info("Starting XML download for %d PMIDs", len(pmids))
        # get_pubmed_xml.download_pubmed_xml(pmids, storage_path, base_path)
        # xml_to_json.generate_json(pmids, base_path)
    else:
        logger.error("No PMIDs to be downloaded")


if __name__ == "__main__":

    run_pipeline()
