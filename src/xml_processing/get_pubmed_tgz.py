import argparse
import logging
import logging.config
import time
import urllib
from os import environ, makedirs, path

import requests  # noqa flake8 will complain about this, but without it urllib.request will not work
from dotenv import load_dotenv

load_dotenv()

# pipenv run python get_pubmed_tgz.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/alliance_pmids

# mapping of pmc and pmid to tar.gz from
# ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt
# prepend path ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/
# take input list of pmids, get ftp from above, download and write to storage_path pubmed_tgz/ as well as file pmids_found of files that have been downloaded.
# if file already exists in pmids_found, skip it.


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


parser = argparse.ArgumentParser()
parser.add_argument('-d', '--database', action='store_true', help='take input from database query')
parser.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')
parser.add_argument('-r', '--restapi', action='store', help='take input from rest api')

args = vars(parser.parse_args())

# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
base_path = environ.get('XML_PATH')
storage_path = base_path + 'pubmed_tgz/'


def download_pubmed_tgz(pmids_wanted):
    """

    :param pmids_wanted:
    :return:
    """

    # ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt

    if not path.exists(storage_path):
        makedirs(storage_path)

    pmids_found_set = set()
    foundfile = storage_path + 'pmids_found'
    if path.exists(foundfile):
        logger.info("Reading previous foundsum mappings from %s", foundfile)
        with open(foundfile, "r") as foundfile_fh:
            for line in foundfile_fh:
                pmids_found_set.add(line.rstrip())

    pmids_wanted_set = set(pmids_wanted)

    oafile = storage_path + 'oa_file_list.txt'
    if not path.exists(oafile):
        oafile_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt'
        logger.info("Download %s", oafile_ftp)
        oa_req = urllib.request.urlopen(oafile_ftp)
        oafile_data = oa_req.read()
        with open(oafile, 'wb') as oa_fh:
            oa_fh.write(oafile_data)

    if path.exists(oafile):
        logger.info("Reading previous md5sum mappings from %s", oafile)
        with open(oafile, "r") as oafile_fh:
            prefix = 'ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/'
            for line in oafile_fh:
                line_data = line.rstrip().split("\t")
                if len(line_data) > 2:
                    # md5dict[line_data[0]] = line_data[1]
                    pmid = line_data[3]
                    pmid = pmid.replace('PMID:', '')
                    if pmid in pmids_found_set:
                        continue
                    if pmid in pmids_wanted_set:
                        ftp = prefix + line_data[0]
                        pmids_found_set.add(pmid)
                        outfile = storage_path + pmid + '.tar.gz'
                        logger.info("Download %s %s", pmid, ftp)
                        ftp_req = urllib.request.urlopen(ftp)
                        ftp_data = ftp_req.read()
                        with open(outfile, 'wb') as out_fh:
                            out_fh.write(ftp_data)
                        time.sleep(5)

    with open(foundfile, 'w') as found_fh:
        for pmid in sorted(pmids_found_set, key=int):
            found_fh.write("%s\n" % pmid)


if __name__ == "__main__":
    """
    call main start function
    """

    pmids_wanted = []

#    python get_pubmed_xml.py -d
    if args['database']:
        logger.info("Processing database entries")

    elif args['restapi']:
        logger.info("Processing rest api entries")

#     python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/pmid_file.txt
    elif args['file']:
        logger.info("Processing file input from %s", args['file'])
        with open(args['file'], 'r') as fp:
            pmid = fp.readline()
            while pmid:
                pmids_wanted.append(pmid.rstrip())
                pmid = fp.readline()

    download_pubmed_tgz(pmids_wanted)
