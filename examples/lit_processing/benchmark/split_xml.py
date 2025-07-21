# 22 minutes on dev.wormbase for 646727 documents from filesystem. 12G of xml to 6.0G of jso1
# import json
import urllib.request
# import xmltodict


# python3 split_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/alliance_pmids -x
# python3 split_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/alliance_pmids -j
#
# 1 hour 48 minutes on agr-literature-dev for 649063 xml documents from filesystem.
# 1 hour  9 minutes on agr-literature-dev for 649063 json documents from filesystem.

# split directory of files with file_type xml or json into multiple sub directories (4 deep) to try to keep directories
# between 1k and 10k files.


import argparse
# import re

from os import environ, path, makedirs, listdir
import logging
import logging.config

from shutil import copyfile

from typing import List
from dotenv import load_dotenv

load_dotenv()


# Need to set up a queue that queries postgres to get a list of pubmed id that don't have a pubmed final flag
# Need to set up an S3 bucket to store xml
# Need to set up flags to take in pmids from postgres queue, file in filesystem, file in URL, list from command line

# to get set of pmids with search term 'elegans'
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=elegans&retmax=100000000

pmids = []  # type: List


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


# todo: save this in an env variable
# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
base_path = environ.get('XML_PATH')


def make_directories(file_type):
    """

    :param file_type:
    :return:
    """

    base_destination_path = base_path + 'pubmed_' + file_type + '_split/'
    destination_path = base_destination_path + '0/'
    makedirs(destination_path, exist_ok=True)
    for w in range(1, 10):
        for x in range(0, 10):
            for y in range(0, 10):
                for z in range(0, 10):
                    destination_path = base_destination_path + str(w) + '/' + str(x) + '/' + str(y) + '/' + str(z) + '/'
                    # logger.info("Create %s if new", destination_path)
                    makedirs(destination_path, exist_ok=True)


def get_max_count_directory(file_type):
    """

    :param file_type:
    :return:
    """

    base_destination_path = base_path + 'pubmed_' + file_type + '_split/'
    destination_path = base_destination_path + '0/'
    max_count = len(listdir(destination_path))
    max_dir = destination_path
    for w in range(1, 10):
        for x in range(0, 10):
            for y in range(0, 10):
                for z in range(0, 10):
                    destination_path = base_destination_path + str(w) + '/' + str(x) + '/' + str(y) + '/' + str(z) + '/'
                    count = len(listdir(destination_path))
                    if count > max_count:
                        max_count = count
                        max_dir = destination_path
    logger.info("max_count %s from path %s", max_count, max_dir)


def get_path_from_pmid(pmid, file_type):
    """

    :param pmid:
    :param file_type:
    :return:
    """

    pmid_list = list(pmid)
    if len(pmid_list) < 4:
        destination_filepath = base_path + 'pubmed_' + file_type + '_split/0/' + pmid + '.' + file_type
    else:
        w = pmid_list.pop(0)
        x = pmid_list.pop(0)
        y = pmid_list.pop(0)
        z = pmid_list.pop(0)
        destination_filepath = base_path + 'pubmed_' + file_type + '_split/' + str(w) + '/' + str(x) + '/' + str(y) + '/' + str(z) + '/' + pmid + '.' + file_type
    # logger.info("pmid %s %s", pmid, destination_filepath)
    return destination_filepath


def transfer_file(file_type):
    """

    :param file_type:
    :return:
    """

    # open input xml file and read data in form of python dictionary using xmltodict module
    for pmid in pmids:
        storage_path = base_path + 'pubmed_' + file_type + '/'
        filename = storage_path + pmid + '.' + file_type
        if not path.exists(filename):
            continue
        destination_filepath = get_path_from_pmid(pmid, file_type)
        # logger.info("Found %s %s", file_type, filename)
        copyfile(filename, destination_filepath)


if __name__ == "__main__":
    """ call main start function """

    parser = argparse.ArgumentParser()
    parser.add_argument('-x', '--xml', action='store_true', help='process xml files')
    parser.add_argument('-j', '--json', action='store_true', help='process json files')
    parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')
    parser.add_argument('-d', '--database', action='store_true', help='take input from database query')
    parser.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')
    parser.add_argument('-r', '--restapi', action='store', help='take input from rest api')
    parser.add_argument('-s', '--sample', action='store_true', help='test sample input from hardcoded entries')
    parser.add_argument('-u', '--url', action='store', help='take input from entries in file at url')

    args = vars(parser.parse_args())

    file_type = 'xml'

    if args['xml']:
        file_type = 'xml'
        logger.info("Splitting up xml directory")

    elif args['json']:
        file_type = 'json'
        logger.info("Splitting up json directory")

#    python split_xml.py -d
    if args['database']:
        logger.info("Processing database entries")

    elif args['restapi']:
        logger.info("Processing rest api entries")

#     python split_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
    elif args['file']:
        logger.info("Processing file input from %s", args['file'])
        with open(args['file'], 'r') as fp:
            pmid = fp.readline()
            while pmid:
                pmids.append(pmid.rstrip())
                pmid = fp.readline()

#     python split_xml.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
    elif args['url']:
        logger.info("Processing url input from %s", args['url'])
        with urllib.request.urlopen(args['url']) as req:
            data = req.read()
            lines = data.splitlines()
            for pmid in lines:
                pmids.append(pmid)

#    python split_xml.py -c 1234 4576 1828
    elif args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids.append(pmid)

#    python split_xml.py -s
    elif args['sample']:
        logger.info("Processing hardcoded sample input")
        pmid = '12345678'
        pmids.append(pmid)
        pmid = '12345679'
        pmids.append(pmid)
        pmid = '12345680'
        pmids.append(pmid)

    else:
        logger.info("Processing database entries")

    make_directories(file_type)
    # UNCOMMENT to transfer files from unsplit pubmed_<file_type> to pubmed_<filetype>_split/<w>/<x>/<y>/<z>/<pmid>.<file_type>
    # transfer_file(file_type)
    # find directory with the highest count of files and report
    get_max_count_directory(file_type)
    logger.info("Done splitting up %s files into directories", file_type)
