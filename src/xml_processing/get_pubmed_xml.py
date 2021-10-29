"""
get_pubmed_xml
==============
"""

# pipenv run python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/alliance_pmids
# pipenv run python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
# pipenv run python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/wormbase_pmids

# PubMed randomly has ("Connection broken: InvalidChunkLength(got length b'', 0 bytes read)" that
# crashes this script.  Keep running it again until it gets all the entries, then generate the md5sum file by running
# pipenv run python get_md5sum.py -x -f /home/azurebrd/git/agr_literature_service_demo/src/xml_
# processing/inputs/alliance_pmids


# pipenv run python get_pubmed_xml.py -u "http://tazendra.caltech.edu/~azurebrd/cgi-bin/forms/generic.cgi?action=ListPmids"

# 1 hour 42 minutes to copy 646721 xml files / 12 G / 12466408 to s3 with
#  aws s3 cp pubmed_xml/ s3://agr-literature/develop/reference/metadata/pubmed/xml/ --recursive

# 1 hour 0 minutes 26 seconds to skip through files already in filesystem in agr-lit-dev, vs 2 minutes at dev.wormbase

# webenv
# https://www.ncbi.nlm.nih.gov/books/NBK25498/#chapter3.Application_3_Retrieving_large

# try using post like (works with 5000 in perl)
# https://www.ncbi.nlm.nih.gov/books/NBK25498/#chapter3.Application_4_Finding_unique_se

# TODO:
# - Need to set up a queue that queries postgres to get a list of pubmed id that don't have a pubmed final flag
# = Need to set up an S3 bucket to store xml
# = Need to set up flags to take in pmids from postgres queue, file in filesystem, file in URL, list from command line

# to get set of pmids with search term 'elegans'
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=elegans&retmax=100000000

# to get a batch of pmids by pmids
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=1,10,100,1000487,1000584&retmode=xml

import sys
import time
import re
import requests
import os
import logging.config
import glob
import hashlib
# from dotenv import load_dotenv
import click
import coloredlogs
import urllib

# load_dotenv()

log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')
coloredlogs.install(level='DEBUG')


def download_pubmed_xml(pmids_wanted, storage_path, base_path):
    """
    main function that downloads PubMed XMLs from a list of IDs
    currently downloaded XMLs are checked and won't be redownloaded (provided a previously used
    base_path and storage_path are used

    Performance:
    4.5 minutes to download 28994 WormBase records in 10000 chunks
    61 minutes to download 429899 Alliance records in 10000 chunks
    127 minutes to download 646714 Alliance records in 5000 chunks, failed on 280


    :param pmids_wanted: list of PMIDs to be processed
    :param storage_path: path to the directory where XMLs will be stored
    :param base_path: path where the application will generate output
    :return:
    """

    pmids_slice_size = 5000

    logger.info('There are ' + str(len(pmids_wanted)) + ' files in the queue')

    if not os.path.exists(storage_path):
        os.makedirs(storage_path)

    # comparing through a set instead of a list takes 2.6 seconds instead of 4256
    pmids_found = set([])

    # this section reads pubmed xml files already acquired to skip downloading them.
    # to get full set, clear out storage_path, or comment out this section
    logger.info('Reading PubMed XML previously acquired')
    pmid_xml = set([os.path.basename(x).replace('.xml', '') for x in glob.glob(storage_path + '*.xml')])
    logger.info('Found ' + str(len(pmid_xml)) + ' XMLs')

    pmids_to_get = set(pmids_wanted).difference(set(pmid_xml))
    pmids_to_get = sorted(list(pmids_to_get))

    logger.info('After processing, ' + str(len(pmids_to_get)) + ' files will be downloaded')
    logger.info("Starting download of new PubMed XML")

    md5dict = {}
    md5file = storage_path + 'md5sum'
    if os.path.exists(md5file):
        logger.info('Reading previous md5sum mappings from %s', md5file)
        with open(md5file) as md5file_fh:
            for line in md5file_fh:
                line_data = line.split('\t')
                if line_data[0]:
                    md5dict[line_data[0]] = line_data[1].rstrip()
    else:
        logger.info('No MD5SUM information found')

    #
    for index in range(0, len(pmids_wanted), pmids_slice_size):
        pmids_slice = pmids_wanted[index:index + pmids_slice_size]
        pmids_joined = ', '.join(pmids_slice)
        logger.debug('Processing PMIDs %s', pmids_joined)

        # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=1,10,100,1000487,1000584&retmode=xml
        # default way without a library, using get
        # url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" \+
        # pmids_joined + "&retmode=xml"
        # print url
        # f = urllib.urlopen(url)
        # xml_all = f.read()

        # using post with requests library, works well for 10000 pmids
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        parameters = {'db': 'pubmed', 'retmode': 'xml', 'id': pmids_joined}

        # PubMed randomly has ("Connection broken: InvalidChunkLength(got length b'', 0 bytes read)"
        # that crashes this script.
        try:
            with requests.post(url, data=parameters) as r:
                xml_all = r.text
                # xml_all = r.text.encode('utf-8').strip()		  # python2
                # xml_split = xml_all.split("\n<Pubmed")	 # before 2021 08 11  xml output had linebreaks between pmids, making that easier
                xml_split = re.split('(<Pubmed[^>]*Article>)', xml_all)	  # some types are not PubmedArticle, like PubmedBookArticle, e.g. 32644453
                header = xml_split.pop(0)
                # header = header + "\n<Pubmed" + xml_split.pop(0)	  # before when splitting on linebreak without capturing was manually adding the split
                # footer = "\n\n</PubmedArticleSet>"
                footer = "</PubmedArticleSet>"

                while xml_split:
                    this_xml = header + xml_split.pop(0) + xml_split.pop(0)
                    if len(xml_split) > 0:
                        this_xml = this_xml + footer
                    clean_xml = os.linesep.join([s for s in this_xml.splitlines() if s])
                    clean_xml = clean_xml.replace('\n', ' ')
                    # logger.info(clean_xml)
                    if re.search(r"<PMID[^>]*?>(\d+)</PMID>", clean_xml):
                        pmid_group = re.search(r"<PMID[^>]*?>(\d+)</PMID>", clean_xml)
                        pmid = pmid_group.group(1)
                        pmids_found.add(pmid)
                        filename = storage_path + pmid + '.xml'
                        f = open(filename, "w")
                        f.write(clean_xml)
                        f.close()
                        md5sum = hashlib.md5(clean_xml.encode('utf-8')).hexdigest()
                        md5dict[pmid] = md5sum
                if len(pmids_slice) == pmids_slice_size:
                    logger.info('Waiting to process more pmids')
                    time.sleep(5)
        except requests.exceptions.RequestException as e:
            logger.info("requests failure with input %s %s", pmids_joined, e)
            logger.error(str(e))
            raise SystemExit(e)

    # md5file = storage_path + 'md5sum'
    logger.info('Writing md5sum mappings to %s', md5file)
    with open(md5file, 'w') as md5file_fh:
        # md5file_fh.write(md5data)
        for key in sorted(md5dict.keys(), key=int):
            md5file_fh.write("%s\t%s\n" % (key, md5dict[key]))

    logger.info('Writing log of pmids_not_found')
    output_pmids_not_found_file = base_path + ' pmids_not_found'
    with open(output_pmids_not_found_file, "a") as pmids_not_found_file:
        for pmid in pmids_wanted:
            if pmid not in pmids_found:
                pmids_not_found_file.write("%s\n" % (pmid))
                logger.info('PMID %s not found in pubmed query', pmid)
        pmids_not_found_file.close()

    logger.info('Getting PubMed XML complete')


# to process one by one
#   for pmid in pmids_wanted:
# #    add some validation here
#     url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + pmid + "&retmode=xml"
#     filename = storage_path + pmid + '.xml'
# #     print url
# #     print filename
#     logger.info("Downloading %s into %s", url, filename)
#     urllib.urlretrieve(url, filename)
#     time.sleep( 5 )

@click.command()
@click.option('-c', '--commandline', 'cli', multiple=True, help='take input from command line flag', required=False)
@click.option('-d', '--database', 'db', help='take input from database query', required=False)
@click.option('-f', '--file', 'ffile', help='take input from entries in file with full path', required=False)
@click.option('-r', '--restapi', 'api', help='take input from rest api', required=False)
@click.option('-s', '--sample', 'sample', help='test sample input from hardcoded entries', required=False, default=False, is_flag=True)
@click.option('-u', '--url', 'url', help='take input from entries in file at url', required=False)
def process_tasks(cli, db, ffile, api, sample, url):
    """
    auxiliary  function that process arguments from the command line

    :param cli: PMIDs as cli inputs
    :param db: PMIDs from DB query (not fully implemented)
    :param ffile: PMIDs from a file (one per line), path entered
    :param api: PMIDs from a REST API endpoint
    :param sample: hardcoded PMIDs
    :param url: PMIDs from a file at the end of a URL
    :return:
    """

    # set storage location
    # todo: see if environment variable check works
    # base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
    if len(os.environ.get('XML_PATH')) == 0:
        sys.exit()
    else:
        base_path = os.environ.get('XML_PATH')
        storage_path = base_path + 'pubmed_xml/'

    logger.info('Base path is at ' + base_path)
    logger.info('XMLs will be saved on ' + storage_path)

    # print(os.environ.get('XML_PATH'))

    pmids_wanted = []        # list that will contain the PMIDs to be downloaded

    # checking parameters
    if db:
        # python get_pubmed_xml.py -d
        logger.info('Processing database entries')
    elif api:
        # python get_pubmed_xml.py -r
        logger.info('Processing rest api entries')
    elif ffile:
        # python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/pmid_file.txt
        logger.info('Processing file input from ' + ffile)
        # this requires a well structured input
        pmids_wanted = open(ffile).read().splitlines()
    elif url:
        # python get_pubmed_xml.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
        logger.info('Processing url input from %s', url)
        req = urllib.request.urlopen(url)
        data = req.read()
        lines = data.splitlines()
        for pmid in lines:
            pmids_wanted.append(str(int(pmid)))
    elif cli:
        # python get_pubmed_xml.py -c 1234 4576 1828
        logger.info("Processing commandline input")
        for pmid in cli:
            pmids_wanted.append(pmid)
    elif sample:
        # python get_pubmed_xml.py -s
        logger.info("Processing hardcoded sample input")
        pmids_wanted = ['12345678', '12345679', '12345680']
    # else:
    #     logger.info("Processing database entries")

    if len(pmids_wanted) > 0:
        logger.info('Starting XML download')
        download_pubmed_xml(pmids_wanted, storage_path, base_path)
    else:
        logger.error('No PMIDs to be downloaded')


if __name__ == '__main__':
    """
    call main processing function
    """

    process_tasks()
