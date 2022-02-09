
import json
import logging.config
import warnings
from os import environ, listdir, path

from dotenv import load_dotenv

from helper_file_processing import split_identifier

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()

# pipenv run python3 find_duplicate_doi.py


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
base_path = environ.get('XML_PATH')


# def split_identifier(identifier, ignore_error=False):
#     """
#
#     Split Identifier.
#
#     Does not throw exception anymore. Check return, if None returned, there was an error
#
#     :param identifier:
#     :param ignore_error:
#     :return:
#     """
#
#     prefix = None
#     identifier_processed = None
#     separator = None
#
#     if ':' in identifier:
#         prefix, identifier_processed = identifier.split(':', 1)  # Split on the first occurrence
#         separator = ':'
#     elif '-' in identifier:
#         prefix, identifier_processed = identifier.split('-', 1)  # Split on the first occurrence
#         separator = '-'
#     else:
#         if not ignore_error:
#             logger.critical('Identifier does not contain \':\' or \'-\' characters.')
#             logger.critical('Splitting identifier is not possible.')
#             logger.critical('Identifier: %s', identifier)
#         prefix = identifier_processed = separator = None
#
#     return prefix, identifier_processed, separator


def find_doi_duplicates():
    """

    :return:
    """

    json_storage_path = base_path + 'sanitized_reference_json/'

    files_to_process = []
    dir_list = listdir(json_storage_path)
    for filename in dir_list:
        # logger.info("%s", filename)
        if 'REFERENCE_' in filename and '.REFERENCE_' not in filename:
            # logger.info("%s", filename)
            files_to_process.append(json_storage_path + filename)

    duplicate_dois_file = base_path + 'duplicate_dois'
    with open(duplicate_dois_file, 'w') as duplicate_fh:
        xrefs = dict()
        primary_id_in_file = dict()
        for filepath in files_to_process:
            # only test one file for run
            # if filepath != json_storage_path + 'REFERENCE_PUBMED_ZFIN_1.json':
            #     continue
            logger.info("opening file\t%s", filepath)
            f = open(filepath)
            reference_data = json.load(f)

            filename = filepath.replace(json_storage_path, '')

            # counter = 0
            for entry in reference_data:
                # counter += 1
                # if counter > 2:
                #     break

                # output what we get from the file before converting for the API
                # json_object = json.dumps(entry, indent=4)
                # print(json_object)

                primary_id = entry['primaryId']
                # if primary_id != 'PMID:9643811':
                #     continue

                if primary_id in primary_id_in_file:
                    primary_id_in_file[primary_id].append(filename)
                else:
                    primary_id_in_file[primary_id] = []
                    primary_id_in_file[primary_id].append(filename)

                if 'crossReferences' in entry:
                    for xref in entry['crossReferences']:
                        prefix, identifier, separator = split_identifier(xref['id'])
                        if prefix == 'NLM' or prefix == 'ISSN':
                            continue

                        ident = xref['id']
                        if ident in xrefs:
                            xrefs[ident].add(primary_id)
                        else:
                            xrefs[ident] = set()
                            xrefs[ident].add(primary_id)
        for ident in xrefs:
            if len(xrefs[ident]) > 1:
                sorted_ids = sorted(xrefs[ident])
                duplicate_list = []
                for primary_id in sorted_ids:
                    files = ", ".join(primary_id_in_file[primary_id])
                    duplicate_text = primary_id + " (" + files + ")"
                    duplicate_list.append(duplicate_text)
                primary_ids = "; ".join(duplicate_list)
                duplicate_fh.write(ident + "\t" + primary_ids + "\n")
                # logger.info("ident %s\tset %s", ident, primary_ids)

        duplicate_fh.close


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("starting find_duplicate_doi.py")

    find_doi_duplicates()

# pipenv run python3 find_duplicate_doi.py

    logger.info("ending parse_dqm_json_reference.py")
