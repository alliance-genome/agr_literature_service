"""
process_many_pmids_to_json.py
=============================

module that converts XMLs to JSON files

pipenv run python process_many_pmids_to_json.py -f inputs/alliance_pmids

to force skip of downloading xml
pipenv run python process_many_pmids_to_json.py -s -f inputs/alliance_pmids

enter a file with a list of pmids as an argument, download xml, convert to json, find new pmids in commentsCorrections,
recurse, output list of pubmed-based (as opposed to MOD-DQM-based) pmids to  inputs/pubmed_only_pmids

"""


import logging
import os
import time

import click
import coloredlogs

from get_pubmed_xml import download_pubmed_xml
from xml_to_json import generate_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")


def download_and_convert_pmids(pmids_wanted, skip_download_flag):
    """

    :param pmids_wanted:
    :return:
    """

    pmids_original = pmids_wanted
    pmids_additional = []
    pmids_new_list = pmids_wanted
    pmids_additional = recursively_process_pmids(pmids_original, pmids_additional, pmids_new_list, skip_download_flag)

    base_path = os.environ.get("XML_PATH")
    inputs_path = base_path + "inputs/"
    if not os.path.exists(inputs_path):
        os.makedirs(inputs_path)
    pubmed_only_filepath = base_path + "inputs/pubmed_only_pmids"
    pmids_additional.sort(key=int)
    # for pmid in pmids_additional:
    #     logger.info("new_pmid %s", pmid)
    #     print("pubmed additional %s" % (pmid))
    pmids_additional_string = "\n".join(pmids_additional)
    with open(pubmed_only_filepath, "w") as pubmed_only_fh:
        pubmed_only_fh.write(pmids_additional_string)

    pubmed_all_filepath = base_path + "inputs/all_pmids"
    pmids_all_list = pmids_wanted + pmids_additional
    pmids_all_list.sort(key=int)
    pmids_all_string = "\n".join(pmids_all_list)
    with open(pubmed_all_filepath, "w") as pubmed_all_fh:
        pubmed_all_fh.write(pmids_all_string)


def recursively_process_pmids(
    pmids_original, pmids_additional, pmids_new_list, skip_download_flag
):
    """

    :param pmids_original:
    :param pmids_additional:
    :param pmids_new_list:
    :return:
    """

    if not skip_download_flag:
        download_pubmed_xml(pmids_new_list)
    pmids_already_processed = pmids_original + pmids_additional
    pmids_new_list = generate_json(pmids_new_list, pmids_already_processed)
    # for pmid in pmids_new_list:
    #     logger.info("new_pmid %s", pmid)
    #     print("newly found %s" % (pmid))
    # print(pmids_new_list)
    # print(pmids_additional)
    if pmids_new_list:
        time.sleep(1)
        pmids_additional.extend(pmids_new_list)
        recursively_process_pmids(pmids_original, pmids_additional, pmids_new_list, skip_download_flag)
    return pmids_additional


@click.command()
@click.option("-c", "--commandline", "cli", multiple=True, help="take input from command line flag", required=False)
@click.option("-f", "--file", "ffile", help="take input from entries in file with full path", required=False)
@click.option("-s", "--skip-download", "skip", help="do not download PubMed XML in testing mode", required=False, default=False,)
def run_tasks(cli, ffile, skip):
    """
    skip download flag is to avoid downloading new pubmed_xml/ when running tests,
    although if the files already exist there from the repo, they won't get downloaded anyway.

    :param cli:
    :param ffile:
    :param skip:
    :return:
    """

    pmids_wanted = []

    # python process_single_pmid.py -c 1234 4576 1828
    if cli:
        logger.info("Processing commandline input")
        for pmid in cli:
            pmids_wanted.append(pmid)
    elif ffile:
        logger.info("Processing file input from " + ffile)
        pmids_wanted = open(ffile).read().splitlines()
    else:
        logger.info("Must enter a PMID through command line")

    download_and_convert_pmids(pmids_wanted, skip)

    logger.info("Done Processing")


if __name__ == "__main__":
    """
    call main start function
    """

    run_tasks()
