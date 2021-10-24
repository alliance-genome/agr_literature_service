from os import environ, path

import json
import requests

base_path = environ.get('XML_PATH')


def split_identifier(identifier, ignore_error=False):
    """

    Split Identifier.

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
            # not sure how to logger from imported function without breaking logger in main function
            # logger.critical('Identifier does not contain \':\' or \'-\' characters.')
            # logger.critical('Splitting identifier is not possible.')
            # logger.critical('Identifier: %s', identifier)
            print('Identifier does not contain \':\' or \'-\' characters.')
            print('Splitting identifier is not possible.')
            print('Identifier: %s' % (identifier))
        prefix = identifier_processed = separator = None

    return prefix, identifier_processed, separator


def load_ref_xref(datatype):
    """

    :param datatype:
    :return:
    """

    # 7 seconds to populate file with 2476879 rows
    ref_xref_valid = dict()
    ref_xref_obsolete = dict()
    xref_ref = dict()
    base_path = environ.get('XML_PATH')
    reference_primary_id_to_curie_file = base_path + datatype + '_curie_to_xref'
    if path.isfile(reference_primary_id_to_curie_file):
        with open(reference_primary_id_to_curie_file, 'r') as read_fh:
            for line in read_fh:
                line_data = line.rstrip().split("\t")
                agr = line_data[0]
                xref = line_data[1]
                status = line_data[2]
                prefix, identifier, separator = split_identifier(xref)
                if status == 'valid':
                    if agr not in ref_xref_valid:
                        ref_xref_valid[agr] = dict()
                    ref_xref_valid[agr][prefix] = identifier
                    # previously a reference and prefix could have multiple values
                    # if prefix not in ref_xref_valid[agr]:
                    #     ref_xref_valid[agr][prefix] = set()
                    # if identifier not in ref_xref_valid[agr][prefix]:
                    #     ref_xref_valid[agr][prefix].add(identifier)
                    if prefix not in xref_ref:
                        xref_ref[prefix] = dict()
                    if identifier not in xref_ref[prefix]:
                        xref_ref[prefix][identifier] = agr
                elif status == 'obsolete':
                    if agr not in ref_xref_obsolete:
                        ref_xref_obsolete[agr] = dict()
                    # a reference and prefix can still have multiple obsolete values
                    if prefix not in ref_xref_obsolete[agr]:
                        ref_xref_obsolete[agr][prefix] = set()
                    if identifier not in ref_xref_obsolete[agr][prefix]:
                        ref_xref_obsolete[agr][prefix].add(identifier.lower())
            read_fh.close
    return xref_ref, ref_xref_valid, ref_xref_obsolete


def load_pubmed_resource_basic():
    """

    :return:
    """

    filename = base_path + 'pubmed_resource_json/resource_pubmed_all.json'
    f = open(filename)
    resource_data = json.load(f)
    pubmed_by_nlm = dict()
    for entry in resource_data:
        # primary_id = entry['primaryId']
        nlm = entry['nlm']
        pubmed_by_nlm[nlm] = entry
    return pubmed_by_nlm


def save_pubmed_resource(json_storage_path, pubmed_by_nlm):
    """

    :param json_storage_path:
    :param pubmed_by_nlm:
    :return:
    """

    pubmed_data = dict()
    pubmed_data['data'] = []
    for nlm in pubmed_by_nlm:
        pubmed_data['data'].append(pubmed_by_nlm[nlm])
    json_filename = json_storage_path + 'RESOURCE_NLM.json'
    write_json(json_filename, pubmed_data)


def write_json(json_filename, dict_to_output):
    """

    :param json_filename:
    :param dict_to_output:
    :return:
    """

    with open(json_filename, "w") as json_file:
        # not sure how to logger from imported function without breaking logger in main function
        # logger.info("Generating JSON for %s", json_filename)
        json_data = json.dumps(dict_to_output, indent=4, sort_keys=True)
        json_file.write(json_data)
        json_file.close()
