import argparse
import logging
import logging.config
from os import environ, path

from sanitize_pubmed_json import sanitize_pubmed_json_list

# from post_reference_to_api import post_references

# pipenv run python parse_pubmed_json_reference.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/pubmed_only_pmids
# enter a file of pmids as an argument, sanitize, post to api

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('parse_pubmed_json_reference')


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--generate-pmid-data', action='store_true', help='generate pmid outputs')
    parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')
    parser.add_argument('-m', '--mod', action='store', help='which mod, use all or leave blank for all')
    parser.add_argument('-c', '--commandline', nargs='*', action='store',
                        help='placeholder for parse_pubmed_json_reference.py')

    args = vars(parser.parse_args())

    pmids_wanted = []

    # python parse_pubmed_json_reference.py -c 1234 4576 1828
    if args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids_wanted.append(pmid)

    elif args['file']:
        logger.info("Processing file input from %s", args['file'])
        base_path = environ.get('XML_PATH')
        filename = base_path + args['file']
        try:
            with open(filename, 'r') as fp:
                pmid = fp.readline()
                while pmid:
                    pmids_wanted.append(pmid.rstrip())
                    pmid = fp.readline()
                fp.close()
        except IOError:
            logger.info("No input file at %s", filename)

    else:
        logger.info("Must enter a PMID through command line")

    sanitize_pubmed_json_list(pmids_wanted)

    # do below if wanting to post from here, instead of from post_reference_to_api.py
    # base_path = environ.get('XML_PATH')
    # json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
    # process_results = post_references(json_filepath)

    logger.info("Done Processing")
