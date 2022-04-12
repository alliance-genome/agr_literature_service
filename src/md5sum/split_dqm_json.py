
import json

import hashlib
import sys

from os import path
import logging.config

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def split_dqm_json(input_path):      # noqa: C901
    # mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']
    mods = ['WB']

    pmid_fields = ['authors', 'volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished',
                   'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher',
                   'meshTerms', 'plainLanguageAbstract', 'pubmedAbstractLanguages', 'publicationStatus']

    counter = 0
    for mod in mods:
        md5data = ''

        filename = input_path
        logger.info("Loading %s data from %s", mod, filename)
        dqm_data = dict()
        try:
            with open(filename, 'r') as f:
                dqm_data = json.load(f)
                f.close()
        except IOError:
            logger.info("No reference data to update from MOD %s", mod)

        for entry in dqm_data['data']:
            counter += 1
            primary_id = entry['primaryId']
            logger.info("counting %s %s %s", counter, mod, primary_id)

            # if it's a pmid, ignore fields that come from pubmed, since they'll be updated from pubmed update
            prefix, identifier, separator = split_identifier(entry['primaryId'])
            if prefix == 'PMID':
                for pmid_field in pmid_fields:
                    if pmid_field in entry:
                        del entry[pmid_field]
            # always ignore dateLastModified if a MOD sent it in, should only come from PubMed
            if 'dateLastModified' in entry:
                del entry['dateLastModified']

            # pretty-print
            json_data = json.dumps(entry, indent=4, sort_keys=True)

            md5sum = hashlib.md5(json_data.encode('utf-8')).hexdigest()
            md5data += primary_id + "\t" + md5sum + "\n"

        md5file = input_path + '.md5sum'
        logger.info("Writing md5sum mappings to %s", md5file)
        with open(md5file, "a") as md5file_fh:
            md5file_fh.write(md5data)


def split_identifier(identifier, ignore_error=False):
    """
    Split Identifier

    Does not throw exception anymore. Check return, if None returned, there was an error

    :param identifier:
    :param ignore_error:
    :return:
    """

    prefix = None
    identifier_processed = None
    separator = None

    if ':' in identifier:
        prefix, identifier_processed = identifier.split(':', 1)  # Split on the first occurrence
        separator = ':'
    elif '-' in identifier:
        prefix, identifier_processed = identifier.split('-', 1)  # Split on the first occurrence
        separator = '-'
    else:
        if not ignore_error:
            logger.critical('Identifier does not contain \':\' or \'-\' characters.')
            logger.critical('Splitting identifier is not possible.')
            logger.critical('Identifier: %s', identifier)
        prefix = identifier_processed = separator = None

    return prefix, identifier_processed, separator


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("split_dqm_json parse_dqm_json_reference.py")

    split_dqm_json(sys.argv[1])

    logger.info("split_dqm_json parse_dqm_json_reference.py")
