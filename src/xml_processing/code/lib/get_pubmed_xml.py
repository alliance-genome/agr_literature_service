"""
get_pubmed_xml
==============

pipenv run python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/alliance_pmids
pipenv run python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
pipenv run python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/wormbase_pmids

PubMed randomly has ("Connection broken: InvalidChunkLength(got length b'', 0 bytes read)" that
crashes this script.  Keep running it again until it gets all the entries, then generate the md5sum file by running
pipenv run python get_md5sum.py -x -f /home/azurebrd/git/agr_literature_service_demo/src/xml_
processing/inputs/alliance_pmids


pipenv run python get_pubmed_xml.py -u "http://tazendra.caltech.edu/~azurebrd/cgi-bin/forms/generic.cgi?action=ListPmids"

1 hour 42 minutes to copy 646721 xml files / 12 G / 12466408 to s3 with
 aws s3 cp pubmed_xml/ s3://agr-literature/develop/reference/metadata/pubmed/xml/ --recursive

1 hour 0 minutes 26 seconds to skip through files already in filesystem in agr-lit-dev, vs 2 minutes at dev.wormbase

webenv
https://www.ncbi.nlm.nih.gov/books/NBK25498/#chapter3.Application_3_Retrieving_large

try using post like (works with 5000 in perl)
https://www.ncbi.nlm.nih.gov/books/NBK25498/#chapter3.Application_4_Finding_unique_se

TODO:
- Need to set up a queue that queries postgres to get a list of pubmed id that don't have a pubmed final flag
= Need to set up an S3 bucket to store xml
= Need to set up flags to take in pmids from postgres queue, file in filesystem, file in URL, list from command line

to get set of pmids with search term 'elegans'
https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=elegans&retmax=100000000

to get a batch of pmids by pmids
https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=1,10,100,1000487,1000584&retmode=xml
"""

import glob
import hashlib
import logging
import os
import re
import time

import coloredlogs
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")


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

    logger.info("There are " + str(len(pmids_wanted)) + " files in the queue")

    if not os.path.exists(storage_path):
        logger.info("Storage path does not exist, creating it")
        os.makedirs(storage_path)
    else:
        logger.info(f"Storage path already exists at {storage_path}")

    logger.info("Checking previously downloaded XMLs")

    pmids_found = set([])

    # this section reads pubmed xml files already acquired to skip downloading them.
    # to get full set, clear out storage_path, or comment out this section
    pmid_xml = set([os.path.basename(x).replace(".xml", "") for x in glob.glob(storage_path + "/*.xml")])
    logger.info(f"Found {len(pmid_xml)} XMLs")

    pmids_to_get = set(pmids_wanted).difference(set(pmid_xml))
    pmids_to_get = sorted(list(pmids_to_get))
    logger.info(f"After processing, {len(pmids_to_get)} files will be downloaded")

    if len(pmids_to_get) > 0:
        logger.info("Starting download of new PubMed XML")
        md5dict = {}
        md5file = storage_path + "/md5sum"
        if os.path.exists(md5file):
            logger.info(f"Reading previous md5sum mappings from {md5file}")
            with open(md5file) as md5file_fh:
                for line in md5file_fh:
                    line_data = line.split("\t")
                    if line_data[0]:
                        md5dict[line_data[0]] = line_data[1].rstrip()
        else:
            logger.info("No MD5SUM information found")

        for index in range(0, len(pmids_wanted), pmids_slice_size):
            pmids_slice = pmids_wanted[index: index + pmids_slice_size]
            pmids_joined = ", ".join(pmids_slice)
            logger.debug(f"Processing PMIDs {pmids_joined}")

            # using post with requests library, works well for 10000 pmids
            url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
            parameters = {"db": "pubmed", "retmode": "xml", "id": pmids_joined}

            # PubMed randomly has ("Connection broken: InvalidChunkLength(got length b'', 0 bytes read)"
            # that crashes this script.
            try:
                with requests.post(url, data=parameters) as r:
                    xml_all = r.text
                    # some types are not PubmedArticle, like PubmedBookArticle, e.g. 32644453
                    xml_split = re.split('(<Pubmed[^>]*Article>)', xml_all)
                    header = xml_split.pop(0)
                    footer = "</PubmedArticleSet>"

                    while xml_split:
                        this_xml = header + xml_split.pop(0) + xml_split.pop(0)
                        if len(xml_split) > 0:
                            this_xml = this_xml + footer
                        clean_xml = os.linesep.join([s for s in this_xml.splitlines() if s])
                        clean_xml = clean_xml.rstrip()
                        if re.search(r"<PMID[^>]*?>(\d+)</PMID>", clean_xml):
                            pmid = re.search(r"<PMID[^>]*?>(\d+)</PMID>", clean_xml).group(1)
                            pmids_found.add(pmid)
                            filename = os.path.join(storage_path, f"{pmid}.xml")
                            f = open(filename, "w")
                            f.write(clean_xml)
                            f.close()
                            md5sum = hashlib.md5(clean_xml.encode("utf-8")).hexdigest()
                            md5dict[pmid] = md5sum
                    if len(pmids_slice) == pmids_slice_size:
                        logger.info("Waiting to process more pmids")
                        time.sleep(5)
            except requests.exceptions.RequestException as e:
                logger.info(f"requests failure with input {pmids_joined} {e}")
                logger.error(str(e))
                raise SystemExit(e)

            # md5file = storage_path + 'md5sum'
            logger.info(f"Writing md5sum mappings to {md5file}")
            with open(md5file, "w") as md5file_fh:
                for key in sorted(md5dict.keys(), key=int):
                    md5file_fh.write("%s\t%s\n" % (key, md5dict[key]))

    logger.info("Writing log of pmids_not_found")
    output_pmids_not_found_file = base_path + "/pmids_not_found"
    with open(output_pmids_not_found_file, "a") as pmids_not_found_file:
        for pmid in pmids_wanted:
            if pmid not in pmids_found:
                pmids_not_found_file.write("%s\n" % (pmid))
                logger.info("PMID %s not found in pubmed query", pmid)
        pmids_not_found_file.close()

    logger.info("Getting PubMed XML complete")


if __name__ == '__main__':
    """
    call main processing function
    """

    pmids = ["12345678", "12345679", "12345680", "21290765", "33054145", "21413221", "28304499", "28308877"]
    base_path = os.getcwd()
    storage_path = os.path.join(base_path, 'pubmed_xml')
    download_pubmed_xml(pmids, storage_path, base_path)
