# import json
import argparse
import hashlib
# from os import makedirs, listdir
import logging
import logging.config
import urllib
from os import environ, path
# from shutil import copyfile
from typing import List

from dotenv import load_dotenv

# pipenv run python get_md5sum.py -x -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/alliance_pmids
# pipenv run python get_md5sum.py -j -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/alliance_pmids

# generate file mapping directory's pmid files to their md5sums, taking as input xml or json directories.

# 5 minutes 5 seconds for 649073 xml

# import re

load_dotenv()

pmids = []      # type: List


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


# todo: save this in an env variable
# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
base_path = environ.get('XML_PATH', "")


def generate_md5sums(file_type):
    """

    :param file_type:
    :return:
    """

    # storage_path = base_path + 'pubmed_' + file_type + '_20210322/'
    storage_path = base_path + 'pubmed_' + file_type + '/'
    md5data = ''
    for pmid in pmids:
        filename = storage_path + pmid + '.' + file_type
        if not path.exists(filename):
            continue
        md5_hash = hashlib.md5()
        with open(filename, "rb") as f:
            # Read and update hash in chunks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        # logger.info("Found %s %s %s", file_type, md5_hash.hexdigest(), filename)
        md5data += pmid + "\t" + md5_hash.hexdigest() + "\n"
    md5file = storage_path + 'md5sum'
    with open(md5file, "w") as md5file_fh:
        md5file_fh.write(md5data)


if __name__ == "__main__":
    """
    call main start function
    """
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
        logger.info("generating md5sums of xml directory")

    elif args['json']:
        file_type = 'json'
        logger.info("generating md5sums of json directory")

#    python get_md5sum.py -d
    if args['database']:
        logger.info("Processing database entries")

    elif args['restapi']:
        logger.info("Processing rest api entries")

#     python get_md5sum.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
    elif args['file']:
        logger.info("Processing file input from %s", args['file'])
        with open(args['file'], 'r') as fp:
            pmid = fp.readline()
            while pmid:
                pmids.append(pmid.rstrip())
                pmid = fp.readline()

#     python get_md5sum.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
    elif args['url']:
        logger.info("Processing url input from %s", args['url'])
        with urllib.request.urlopen(args["url"]) as req:
            data = req.read()
            lines = data.splitlines()
            for pmid in lines:
                pmids.append(pmid)

#    python get_md5sum.py -c 1234 4576 1828
    elif args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids.append(pmid)

#    python get_md5sum.py -s
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

    generate_md5sums(file_type)
    logger.info("Done generating md5sum of %s files", file_type)
