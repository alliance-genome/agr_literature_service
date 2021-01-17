
import time
import urllib
import argparse

import re
import requests

import os
from os import path
import logging
import logging.config


# python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
# python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/wormbase_pmids

# webenv
# https://www.ncbi.nlm.nih.gov/books/NBK25498/#chapter3.Application_3_Retrieving_large

# try using post like (works with 5000 in perl)
# https://www.ncbi.nlm.nih.gov/books/NBK25498/#chapter3.Application_4_Finding_unique_se


# Need to set up a queue that queries postgres to get a list of pubmed id that don't have a pubmed final flag
# Need to set up an S3 bucket to store xml
# Need to set up flags to take in pmids from postgres queue, file in filesystem, file in URL, list from command line

# to get set of pmids with search term 'elegans'
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=elegans&retmax=100000000


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


parser = argparse.ArgumentParser()
parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')
parser.add_argument('-d', '--database', action='store_true', help='take input from database query')
parser.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')
parser.add_argument('-u', '--url', action='store', help='take input from entries in file at url')
parser.add_argument('-s', '--sample', action='store_true', help='test sample input from hardcoded entries')

args = vars(parser.parse_args())

# todo: save this in an env variable
storage_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/pubmed_xml/'

pmids = []
pmids_found = set()

def download_pubmed_xml():
      # 4.5 minutes to download 28994 wormbase records in 10000 chunks
    pmids_slice_size = 10000
    for index in range(0, len(pmids), pmids_slice_size):
        pmids_slice = pmids[index:index + pmids_slice_size]
        pmids_joined = (',').join(pmids_slice);
        logger.info("processing PMIDs %s", pmids_joined)

#         default way without a library, using get
#         url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + pmids_joined + "&retmode=xml"
#         print url
#         f = urllib.urlopen(url)
#         xml_all = f.read()

#         using post with requests library, works well for 10000 pmids
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        parameters = {'db': 'pubmed', 'retmode': 'xml', 'id': pmids_joined}
        r = requests.post(url, data=parameters)
        xml_all = r.text.encode('utf-8').strip()

        xml_split = xml_all.split("\n<Pubmed")		# some types are not PubmedArticle, like PubmedBookArticle, e.g. 32644453
        header = xml_split.pop(0);
        header = header + "\n<Pubmed" + xml_split.pop(0);
        footer = "\n\n</PubmedArticleSet>"

        for n in range(len(xml_split)):
            xml_split[n] = header + "\n<Pubmed" + xml_split[n]
            xml_split[n] = os.linesep.join([s for s in xml_split[n].splitlines() if s])

        for n in range(len(xml_split) - 1):
            xml_split[n] += footer

        for xml in xml_split:
            if re.search("<PMID[^>]*?>(\d+)</PMID>", xml):
                pmid_group = re.search("<PMID[^>]*?>(\d+)</PMID>", xml)
                pmid = pmid_group.group(1)
                pmids_found.add(pmid)
                filename = storage_path + pmid + '.xml'
                f = open(filename, "w")
                f.write(xml)
                f.close()

        if len(pmids_slice) == pmids_slice_size:
            logger.info("waiting to process more pmids")
            time.sleep( 5 )

    for pmid in pmids:
        if pmid not in pmids_found:
            logger.info("PMID %s not found in pubmed query", pmid)

# to process one by one
#   for pmid in pmids:
# #    add some validation here
#     url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + pmid + "&retmode=xml"
#     filename = storage_path + pmid + '.xml'
# #     print url
# #     print filename
#     logger.info("Downloading %s into %s", url, filename)
#     urllib.urlretrieve(url, filename)
#     time.sleep( 5 )



if __name__ == "__main__":
    """ call main start function """

#    python get_pubmed_xml.py -d
    if args['database']:
        logger.info("Processing database entries")

#     python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/pmid_file.txt
    elif args['file']:
        logger.info("Processing file input from %s", args['file'])
        with open(args['file'], 'r') as fp:
            pmid = fp.readline()
            while pmid:
                pmids.append(pmid.rstrip())
                pmid = fp.readline()

#     python get_pubmed_xml.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
    elif args['url']:
        logger.info("Processing url input from %s", args['url'])
        req = urllib.urlopen(args['url'])
        data = req.read()
        lines = data.splitlines()
        for pmid in lines:
            pmids.append(pmid)

#    python get_pubmed_xml.py -c 1234 4576 1828
    elif args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids.append(pmid)

#    python get_pubmed_xml.py -s
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

    download_pubmed_xml()

