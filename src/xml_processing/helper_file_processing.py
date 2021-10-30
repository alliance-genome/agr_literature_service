from os import environ, path

import json
import requests

from helper_post_to_api import generate_headers, update_token, get_authentication_token

import bs4
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

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
    datatype can be reference or resource, generate mappings of their curies to their cross_reference curies

    :param datatype:
    :return:
    """

    # 7 seconds to populate file with 2476879 rows
    ref_xref_valid = dict()
    ref_xref_obsolete = dict()
    xref_ref = dict()
    base_path = environ.get('XML_PATH')
    datatype_primary_id_to_curie_file = base_path + datatype + '_curie_to_xref'
    if path.isfile(datatype_primary_id_to_curie_file):
        with open(datatype_primary_id_to_curie_file, 'r') as read_fh:
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
    nlm_by_issn = dict()
    for entry in resource_data:
        # primary_id = entry['primaryId']
        nlm = entry['nlm']
        pubmed_by_nlm[nlm] = entry
        if 'printISSN' in entry:
            pissn = entry['printISSN']
            if pissn in nlm_by_issn:
                if nlm not in nlm_by_issn[pissn]:
                    nlm_by_issn[pissn].append(nlm)
            else:
                nlm_by_issn[pissn] = [nlm]
        if 'onlineISSN' in entry:
            oissn = entry['onlineISSN']
            if oissn in nlm_by_issn:
                if nlm not in nlm_by_issn[oissn]:
                    nlm_by_issn[oissn].append(nlm)
            else:
                nlm_by_issn[oissn] = [nlm]
    return pubmed_by_nlm, nlm_by_issn


def save_resource_file(json_storage_path, pubmed_by_nlm, datatype):
    """

    :param json_storage_path:
    :param pubmed_by_nlm:
    :return:
    """

    pubmed_data = dict()
    pubmed_data['data'] = []
    for nlm in pubmed_by_nlm:
        pubmed_data['data'].append(pubmed_by_nlm[nlm])
    json_filename = json_storage_path + 'RESOURCE_' + datatype + '.json'
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


def clean_up_keywords(mod, entry):
    # e.g. 9882485 25544291 24201188 31188077
    if mod == 'ZFIN':
        if 'keywords' in entry:
            if entry['keywords'][0] == '':
                entry['keywords'] = []
            else:
                # zfin has all keywords in the first array element, they cannot fix it
                zfin_value = entry['keywords'][0]
                zfin_value = str(bs4.BeautifulSoup(zfin_value, "html.parser"))
                comma_count = 0
                semicolon_count = 0
                if ", " in zfin_value:
                    comma_count = zfin_value.count(',')
                if "; " in zfin_value:
                    semicolon_count = zfin_value.count(';')
                if (comma_count == 0) and (semicolon_count == 0):
                    entry['keywords'] = [zfin_value]
                elif comma_count >= semicolon_count:
                    entry['keywords'] = zfin_value.split(", ")
                else:
                    entry['keywords'] = zfin_value.split("; ")
    else:
        keywords = []
        for mod_keyword in entry['keywords']:
            mod_keyword = str(bs4.BeautifulSoup(mod_keyword, "html.parser"))
            keywords.append(mod_keyword)
        entry['keywords'] = keywords
    return entry


def generate_cross_references_file(datatype):
    """
    This function generates bulk cross_reference data from the API and database.
    4 seconds for resource
    88 seconds for reference

    :param datatype:
    :return:
    """

    api_port = environ.get('API_PORT')
    base_path = environ.get('XML_PATH')

    token = get_authentication_token()
    headers = generate_headers(token)

    url = 'http://localhost:' + api_port + '/bulk_download/' + datatype + 's/external_ids/'
    post_return = requests.get(url, headers=headers)

    if post_return.status_code == 401:
        token = update_token()
        headers = generate_headers(token)
        post_return = requests.get(url, headers=headers)

    response_array = json.loads(post_return.text)
    mapping_output = ''
    for entry in response_array:
        curie = entry['curie']
        xref_array = entry['cross_references']
        for xref_dict in xref_array:
            if xref_dict is not None:
                flag = 'valid'
                xref_id = ''
                if 'curie' in xref_dict:
                    if xref_dict['curie']:
                        xref_id = xref_dict['curie']
                if 'is_obsolete' in xref_dict:
                    if xref_dict['is_obsolete']:
                        flag = 'obsolete'
                mapping_output += curie + '\t' + xref_id + '\t' + flag + '\n'

    ref_xref_file = base_path + datatype + '_curie_to_xref'
    with open(ref_xref_file, "w") as ref_xref_file_fh:
        ref_xref_file_fh.write(mapping_output)
