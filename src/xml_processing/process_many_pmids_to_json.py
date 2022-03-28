import argparse
import logging
import sys
import time
from os import environ, makedirs, path

from get_pubmed_xml import download_pubmed_xml
from xml_to_json import generate_json

# import logging.config

# pipenv run python process_many_pmids_to_json.py -f inputs/alliance_pmids
#
# to force skip of downloading xml
# pipenv run python process_many_pmids_to_json.py -s -f inputs/alliance_pmids
#
# enter a file with a list of pmids as an argument, download xml, convert to json, find new pmids in commentsCorrections, recurse, output list of pubmed-based (as opposed to MOD-DQM-based) pmids to  inputs/pubmed_only_pmids


# log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
# logging.config.fileConfig(log_file_path)
# logger = logging.getLogger('literature logger')
logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def download_and_convert_pmids(pmids_wanted, skip_download_flag):
    """

    :param pmids_wanted:
    :return:
    """

    pmids_original = pmids_wanted
    pmids_additional = []
    pmids_new_list = pmids_wanted
    pmids_additional = recursively_process_pmids(pmids_original, pmids_additional, pmids_new_list, skip_download_flag)

    base_path = environ.get('XML_PATH')
    inputs_path = base_path + 'inputs/'
    if not path.exists(inputs_path):
        makedirs(inputs_path)
    pubmed_only_filepath = base_path + 'inputs/pubmed_only_pmids'
    pmids_additional.sort(key=int)
    # for pmid in pmids_additional:
    #     logger.info("new_pmid %s", pmid)
    #     print("pubmed additional %s" % (pmid))
    pmids_additional_string = ("\n".join(pmids_additional))
    with open(pubmed_only_filepath, "w") as pubmed_only_fh:
        pubmed_only_fh.write(pmids_additional_string)

    pubmed_all_filepath = base_path + 'inputs/all_pmids'
    pmids_all_list = pmids_wanted + pmids_additional
    pmids_all_list.sort(key=int)
    pmids_all_string = ("\n".join(pmids_all_list))
    with open(pubmed_all_filepath, "w") as pubmed_all_fh:
        pubmed_all_fh.write(pmids_all_string)


def recursively_process_pmids(pmids_original, pmids_additional, pmids_new_list, skip_download_flag):
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


if __name__ == "__main__":
    """
    call main start function

    skip download flag is to avoid downloading new pubmed_xml/ when running tests,
    although if the files already exist there from the repo, they won't get downloaded anyway.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')
    parser.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')
    parser.add_argument('-s', '--skip-download', action='store_true', help='do not download PubMed XML in testing mode')

    args = vars(parser.parse_args())

    pmids_wanted = []

    skip_download_flag = False
    if args['skip_download']:
        skip_download_flag = args['skip_download']

    # python process_single_pmid.py -c 1234 4576 1828
    if args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids_wanted.append(pmid)

    elif args['file']:
        base_path = environ.get('XML_PATH')
        logger.info("Processing file input from %s", args['file'])
        with open(base_path + args['file'], 'r') as fp:
            pmid = fp.readline()
            while pmid:
                pmids_wanted.append(pmid.rstrip())
                pmid = fp.readline()

    else:
        logger.info("Must enter a PMID through command line")

    download_and_convert_pmids(pmids_wanted, skip_download_flag)

    logger.info("Done Processing")
