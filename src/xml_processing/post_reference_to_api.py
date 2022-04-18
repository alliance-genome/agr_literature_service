
import requests
import argparse
import json
import logging
import logging.config
import re
import sys
from os import environ, listdir
# from os path

from helper_file_processing import (generate_cross_references_file,
                                    load_ref_xref, split_identifier)
from helper_post_to_api import (generate_headers, get_authentication_token,
                                process_api_request, update_token)

# post to api data from sanitized_reference_json/
# python post_reference_to_api.py
#
# update okta_token only
# python post_reference_to_api.py -a


# log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
# logging.config.fileConfig(log_file_path)
# logger = logging.getLogger('literature logger')

logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


# keys that exist in data
# 2021-05-25 21:16:53,372 - literature logger - INFO - key abstract
# 2021-05-25 21:16:53,372 - literature logger - INFO - key citation
# 2021-05-25 21:16:53,372 - literature logger - INFO - key datePublished
# 2021-05-25 21:16:53,373 - literature logger - INFO - key dateArrivedInPubmed
# 2021-05-25 21:16:53,373 - literature logger - INFO - key dateLastModified
# 2021-05-25 21:16:53,373 - literature logger - INFO - key keywords
# 2021-05-25 21:16:53,373 - literature logger - INFO - key crossReferences
# 2021-05-25 21:16:53,373 - literature logger - INFO - key title
# 2021-05-25 21:16:53,373 - literature logger - INFO - key tags
# 2021-05-25 21:16:53,373 - literature logger - INFO - key issueName
# 2021-05-25 21:16:53,373 - literature logger - INFO - key issueDate
# 2021-05-25 21:16:53,373 - literature logger - INFO - key MODReferenceType
# 2021-05-25 21:16:53,373 - literature logger - INFO - key pubMedType
# 2021-05-25 21:16:53,373 - literature logger - INFO - key meshTerms
# 2021-05-25 21:16:53,373 - literature logger - INFO - key allianceCategory
# 2021-05-25 21:16:53,373 - literature logger - INFO - key volume
# 2021-05-25 21:16:53,373 - literature logger - INFO - key authors
# 2021-05-25 21:16:53,373 - literature logger - INFO - key pages
# 2021-05-25 21:16:53,373 - literature logger - INFO - key publisher
# 2021-05-25 21:16:53,373 - literature logger - INFO - key resource
# 2021-05-25 21:16:53,373 - literature logger - INFO - key language
# 2021-05-25 21:16:53,373 - literature logger - INFO - key modResources
# 2021-05-25 21:16:53,373 - literature logger - INFO - key MODReferenceTypes
# 2021-05-25 21:16:53,373 - literature logger - INFO - key resourceAbbreviation


def camel_to_snake(name):
    """

    :param name:
    :return:
    """

    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)

    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def post_references(input_file, check_file_flag):      # noqa: C901
    """

    :param input_file:
    :param check_file_flag:
    :return:
    """

    api_port = environ.get('API_PORT')
    # base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
    base_path = environ.get('XML_PATH')

    files_to_process = []
    if input_file == 'sanitized':
        json_storage_path = base_path + 'sanitized_reference_json/'
        dir_list = listdir(json_storage_path)
        for filename in dir_list:
            # logger.info("%s", filename)
            if 'REFERENCE_' in filename and '.REFERENCE_' not in filename:
                # logger.info("%s", filename)
                files_to_process.append(json_storage_path + filename)
    else:
        files_to_process.append(input_file)

    keys_to_remove = {'nlm', 'primaryId', 'modResources', 'resourceAbbreviation'}
    remap_keys = dict()
    remap_keys['datePublished'] = 'date_published'
    remap_keys['dateArrivedInPubmed'] = 'date_arrived_in_pubmed'
    remap_keys['dateLastModified'] = 'date_last_modified'
    remap_keys['crossReferences'] = 'cross_references'
    remap_keys['issueName'] = 'issue_name'
    remap_keys['issueDate'] = 'issue_date'
    remap_keys['pubMedType'] = 'pubmed_type'
    remap_keys['meshTerms'] = 'mesh_terms'
    remap_keys['allianceCategory'] = 'category'
    remap_keys['MODReferenceType'] = 'mod_reference_types'
    remap_keys['MODReferenceTypes'] = 'mod_reference_types'
    remap_keys['modCorpusAssociations'] = 'mod_corpus_associations'
    remap_keys['plainLanguageAbstract'] = 'plain_language_abstract'
    remap_keys['pubmedAbstractLanguages'] = 'pubmed_abstract_languages'
    remap_keys['publicationStatus'] = 'pubmed_publication_status'

    subkeys_to_remove = dict()
    remap_subkeys = dict()

    subkeys_to_remove['mesh_terms'] = {'referenceId'}
    subkeys_to_remove['tags'] = {'referenceId'}
    subkeys_to_remove['authors'] = {'referenceId', 'firstinit', 'firstInit', 'crossReferences', 'collectivename'}

    remap_subkeys['mesh_terms'] = dict()
    remap_subkeys['mesh_terms']['meshHeadingTerm'] = 'heading_term'
    remap_subkeys['mesh_terms']['meshQualfierTerm'] = 'qualifier_term'
    remap_subkeys['mesh_terms']['meshQualifierTerm'] = 'qualifier_term'

    remap_subkeys['mod_reference_types'] = dict()
    remap_subkeys['mod_reference_types']['referenceType'] = 'reference_type'

    remap_subkeys['mod_corpus_associations'] = dict()
    remap_subkeys['mod_corpus_associations']['modAbbreviation'] = 'mod_abbreviation'
    remap_subkeys['mod_corpus_associations']['modCorpusSortSource'] = 'mod_corpus_sort_source'
    remap_subkeys['mod_corpus_associations']['dqmFiles'] = 'dqm_files'

    remap_subkeys['tags'] = dict()
    remap_subkeys['tags']['tagName'] = 'tag_name'
    remap_subkeys['tags']['tagSource'] = 'tag_source'

    remap_subkeys['cross_references'] = dict()
    remap_subkeys['cross_references']['id'] = 'curie'

    remap_subkeys['authors'] = dict()
    remap_subkeys['authors']['authorRank'] = 'order'
    remap_subkeys['authors']['firstName'] = 'first_name'
    remap_subkeys['authors']['lastName'] = 'last_name'
    remap_subkeys['authors']['middleNames'] = 'middle_names'
    remap_subkeys['authors']['firstname'] = 'first_name'
    remap_subkeys['authors']['lastname'] = 'last_name'
    remap_subkeys['authors']['middlenames'] = 'middle_names'
    remap_subkeys['authors']['correspondingAuthor'] = 'corresponding_author'
    remap_subkeys['authors']['firstAuthor'] = 'first_author'

    keys_found = set()

    # token = ''
    # okta_file = base_path + 'okta_token'
    # if path.isfile(okta_file):
    #     with open(okta_file, 'r') as okta_fh:
    #         token = okta_fh.read().replace("\n", "")
    #         okta_fh.close
    # else:
    #     token = update_token()
    token = get_authentication_token()
    headers = generate_headers(token)
    api_server = environ.get('API_SERVER', 'localhost')
    url = 'http://' + api_server + ':' + api_port + '/reference/'
    reference_primary_id_to_curie_file = base_path + 'reference_primary_id_to_curie'
    errors_in_posting_reference_file = base_path + 'errors_in_posting_reference'

    # previously loading from reference_primary_id_to_curie from past run of this script
    # already_processed_primary_id = set()
    # if check_file_flag == 'yes_file_check':
    #     if path.isfile(reference_primary_id_to_curie_file):
    #         with open(reference_primary_id_to_curie_file, 'r') as read_fh:
    #             for line in read_fh:
    #                 line_data = line.split("\t")
    #                 if line_data[0]:
    #                     already_processed_primary_id.add(line_data[0].rstrip())
    #             read_fh.close

    if check_file_flag == 'no_file_check':
        xref_ref = dict()
    else:
        generate_cross_references_file('resource')   # this updates from resources in the database, and takes 4 seconds. if updating this script, comment it out after running it once
        generate_cross_references_file('reference')   # this updates from references in the database, and takes 88 seconds. if updating this script, comment it out after running it once

        xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('resource')
        resource_to_curie = dict()
        for prefix in xref_ref:
            for identifier in xref_ref[prefix]:
                xref_curie = prefix + ':' + identifier
                resource_to_curie[xref_curie] = xref_ref[prefix][identifier]
        # previously loading from resource_primary_id_to_curie from past run of post_resource_to_api
        # resource_primary_id_to_curie_file = base_path + 'resource_primary_id_to_curie'
        # if path.isfile(resource_primary_id_to_curie_file):
        #     with open(resource_primary_id_to_curie_file, 'r') as read_fh:
        #         for line in read_fh:
        #             line_data = line.rstrip().split("\t")
        #             if line_data[0]:
        #                 resource_to_curie[line_data[0]] = line_data[1]
        #         read_fh.close

        xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('reference')

    process_results = []
    with open(reference_primary_id_to_curie_file, 'a') as mapping_fh, open(errors_in_posting_reference_file, 'a') as error_fh:
        for filepath in sorted(files_to_process):
            # only test one file for run
            # if filepath != json_storage_path + 'REFERENCE_PUBMED_WB_1.json':
            #     continue
            # logger.info("opening file\t%s", filepath)
            f = open(filepath)
            reference_data = json.load(f)
            # counter = 0
            for entry in reference_data:

                # only take a couple of sample from each file for testing
                # counter += 1
                # if counter > 2:
                #     break

                # output what we get from the file before converting for the API
                # json_object = json.dumps(entry, indent=4)
                # print(json_object)

                primary_id = entry['primaryId']
                prefix, identifier, separator = split_identifier(primary_id)
                # this is only populated if check_file_flag is not "no_file_check", meaning it came from bulk processing of dqm / pubmed
                # if it is "no_file_check" it comes from processing a single pmid via lit curation ui, which validates this before coming here.
                if prefix in xref_ref:
                    if identifier in xref_ref[prefix]:
                        logger.info("%s\talready in", primary_id)
                        continue
                # previously loading from reference_primary_id_to_curie from past run of this script
                # if primary_id in already_processed_primary_id:
                #     continue

                # if primary_id != 'PMID:9643811':
                #     continue

                new_entry = dict()

                for key in entry:
                    keys_found.add(key)
                    # logger.info("key found\t%s\t%s", key, entry[key])
                    if key in remap_keys:
                        # logger.info("remap\t%s\t%s", key, remap_keys[key])
                        # this renames a key, but it can be accessed again in the for key loop, so sometimes a key is visited twice while another is skipped, so have to create a new dict to populate instead
                        # entry[remap_keys[key]] = entry.pop(key)
                        new_entry[remap_keys[key]] = entry[key]
                    elif key not in keys_to_remove:
                        new_entry[key] = entry[key]

                for key in remap_subkeys:
                    if key in new_entry:
                        # logger.info("%s\t%s\t%s", primary_id, key, new_entry[key])
                        new_list = []
                        for sub_element in new_entry[key]:
                            new_sub_element = dict()
                            for subkey in sub_element:
                                if subkey in remap_subkeys[key]:
                                    new_sub_element[remap_subkeys[key][subkey]] = sub_element[subkey]
                                    # logger.info("remap subkey\t%s\t%s", subkey, remap_subkeys[key][subkey])
                                elif key not in subkeys_to_remove or subkey not in subkeys_to_remove[key]:
                                    new_sub_element[subkey] = sub_element[subkey]
                            new_list.append(new_sub_element)
                        new_entry[key] = new_list

                # can only enter agr resource curie, if resource does not map to one, enter nothing
                if 'resource' in new_entry:
                    if check_file_flag == 'no_file_check':
                        url_get_xref = 'http://' + api_server + ':' + api_port + '/cross_reference/' + new_entry['resource']
                        logger.info("get AGR resource cross_reference info from database %s", url_get_xref)
                        get_return = requests.get(url_get_xref)
                        db_entry = json.loads(get_return.text)
                        resource_found = False
                        if 'is_obsolete' in db_entry:
                            if db_entry['is_obsolete'] is False:
                                if 'resource_curie' in db_entry:
                                    new_entry['resource'] = db_entry['resource_curie']
                                    resource_found = True
                        if resource_found is False:
                            del new_entry['resource']
                    else:
                        if new_entry['resource'] in resource_to_curie:
                            new_entry['resource'] = resource_to_curie[new_entry['resource']]
                        else:
                            del new_entry['resource']
                if 'category' in new_entry:
                    new_entry['category'] = new_entry['category'].lower().replace(" ", "_")
                if 'tags' in new_entry:
                    for sub_element in new_entry['tags']:
                        if 'tag_name' in sub_element:
                            sub_element['tag_name'] = camel_to_snake(sub_element['tag_name'])
                if 'authors' in new_entry:
                    for author in new_entry['authors']:
                        if 'orcid' in author:
                            # orcid field in json has just the identifier, need to add the prefix
                            if 'ORCID:' not in author['orcid']:
                                author['orcid'] = 'ORCID:' + author['orcid']
                if 'cross_references' in new_entry:
                    new_entry['cross_references'] = list(filter(lambda x: 'curie' in x and 'NLM:' not in x['curie'] and 'ISSN:' not in x['curie'], new_entry['cross_references']))

                # output what is sent to API after converting file data
                # json_object = json.dumps(new_entry, indent=4)
                # print(json_object)

                # get rid of this if process_api_request works on a full run
                # process_post_tuple = process_post(url, headers, new_entry, primary_id, mapping_fh, error_fh)
                # headers = process_post_tuple[0]
                # process_text = process_post_tuple[1]
                # process_status_code = process_post_tuple[2]
                # process_result = dict()
                # process_result['text'] = process_text
                # process_result['status_code'] = process_status_code
                # process_results.append(process_result)

                api_response_tuple = process_api_request('POST', url, headers, new_entry, primary_id, None, None)
                headers = api_response_tuple[0]
                response_text = api_response_tuple[1]
                response_status_code = api_response_tuple[2]
                log_info = api_response_tuple[3]

                try:
                    response_dict = json.loads(response_text)
                    process_result = dict()
                    process_result['text'] = response_text
                    process_result['status_code'] = response_status_code
                    process_results.append(process_result)

                    if log_info:
                        logger.info(log_info)

                    if (response_status_code == 201):
                        response_dict = response_dict.replace('"', '')
                        logger.info("%s\t%s", primary_id, response_dict)
                        mapping_fh.write("%s\t%s\n" % (primary_id, response_dict))
                    else:
                        logger.info("api error %s primaryId %s message %s", str(response_status_code), primary_id, response_dict['detail'])
                        error_fh.write("api error %s primaryId %s message %s\n" % (str(response_status_code), primary_id, response_dict['detail']))
                except ValueError:
                    logger.info(f"{primary_id}\tValueError")
                    error_fh.write(f"ERROR {primary_id} did not convert to json\n")

        # if wanting to output keys in data for figuring out mapping
        # for key in keys_found:
        #     logger.info("key %s", key)

        mapping_fh.close
        error_fh.close
    return process_results


# get rid of this if process_api_request works on a full run
# def process_post(url, headers, new_entry, primary_id, mapping_fh, error_fh):
#     """
#
#     output the json getting posted to the API
#     json_object = json.dumps(new_entry, indent = 4)
#     print(json_object)
#
#     :param url:
#     :param headers:
#     :param new_entry:
#     :param primary_id:
#     :param mapping_fh:
#     :param error_fh:
#     :return:
#     """
#
#     post_return = requests.post(url, headers=headers, json=new_entry)
#     process_text = str(post_return.text)
#     process_status_code = str(post_return.status_code)
#     logger.info(primary_id + ' text ' + process_text)
#     logger.info(primary_id + ' status_code ' + process_status_code)
#
#     response_dict = dict()
#     try:
#         response_dict = json.loads(post_return.text)
#     except ValueError:
#         logger.info("%s\tValueError", primary_id)
#         error_fh.write("ERROR %s primaryId did not convert to json\n" % (primary_id))
#         return headers, process_text, process_status_code
#
#     if (post_return.status_code == 201):
#         response_dict = response_dict.replace('"', '')
#         logger.info("%s\t%s", primary_id, response_dict)
#         mapping_fh.write("%s\t%s\n" % (primary_id, response_dict))
#     elif (post_return.status_code == 401):
#         logger.info("%s\texpired token", primary_id)
#         mapping_fh.write("%s\t%s\n" % (primary_id, response_dict))
#         token = update_token()
#         headers = generate_headers(token)
#         process_post_tuple = process_post(url, headers, new_entry, primary_id, mapping_fh, error_fh)
#         headers = process_post_tuple[0]
#         process_text = process_post_tuple[1]
#         process_status_code = process_post_tuple[2]
#     elif (post_return.status_code == 500):
#         logger.info("%s\tFAILURE", primary_id)
#         mapping_fh.write("%s\t%s\n" % (primary_id, response_dict))
#     # if redoing a run and want to skip errors of data having already gone in
#     # elif (post_return.status_code == 409):
#     #     continue
#     else:
#         logger.info("ERROR %s primaryId %s message %s", post_return.status_code, primary_id, response_dict['detail'])
#         error_fh.write("ERROR %s primaryId %s message %s\n" % (post_return.status_code, primary_id, response_dict['detail']))
#     return headers, process_text, process_status_code


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--authorization', action='store_true', help='update authorization token')
    parser.add_argument('-f', '--file', action='store', help='take input from input file in full path')
    parser.add_argument('-c', '--commandline', nargs='*', action='store', help='placeholder for process_single_pmid.py')
    args = vars(parser.parse_args())

    logger.info("Starting post_reference_to_api.py")

    if args['authorization']:
        update_token()

    elif args['commandline']:
        logger.info("placeholder for process_single_pmid.py")

    elif args['file']:
        logger.info("placeholder for parse_pubmed_json_reference.py")

    else:
        post_references('sanitized', 'yes_file_check')

    logger.info("ending post_reference_to_api.py")

# pipenv run python post_reference_to_api.py
