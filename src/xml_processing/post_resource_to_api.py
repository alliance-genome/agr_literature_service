import json
import requests
from os import environ, path
import logging
import logging.config


# from datetime import datetime

from helper_post_to_api import generate_headers, update_token

from dotenv import load_dotenv

load_dotenv()

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

# pipenv run python3 post_resource_to_api.py > log_post_resource_to_api

# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
base_path = environ.get('XML_PATH')

auth0_file = base_path + 'auth0_token'

# resource_fields = ['primaryId', 'nlm', 'title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
# resource_fields_from_pubmed = ['title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
resource_fields_not_in_pubmed = ['titleSynonyms', 'abbreviationSynonyms', 'isoAbbreviation', 'copyrightDate', 'publisher', 'editorsOrAuthors', 'volumes', 'pages', 'abstractOrSummary']

# keys that exist in data
# 2021-05-24 23:06:27,844 - literature logger - INFO - key publisher
# 2021-05-24 23:06:27,844 - literature logger - INFO - key isoAbbreviation
# 2021-05-24 23:06:27,844 - literature logger - INFO - key title
# 2021-05-24 23:06:27,844 - literature logger - INFO - key primaryId
# 2021-05-24 23:06:27,844 - literature logger - INFO - key medlineAbbreviation
# 2021-05-24 23:06:27,844 - literature logger - INFO - key onlineISSN
# 2021-05-24 23:06:27,844 - literature logger - INFO - key abbreviationSynonyms
# 2021-05-24 23:06:27,844 - literature logger - INFO - key volumes
# 2021-05-24 23:06:27,844 - literature logger - INFO - key crossReferences
# 2021-05-24 23:06:27,844 - literature logger - INFO - key editorsOrAuthors
# 2021-05-24 23:06:27,844 - literature logger - INFO - key nlm
# 2021-05-24 23:06:27,845 - literature logger - INFO - key pages
# 2021-05-24 23:06:27,845 - literature logger - INFO - key printISSN


def post_resources():
    json_storage_path = base_path + 'sanitized_resource_json/'
    filesets = ['NLM', 'FB', 'ZFIN']
    keys_to_remove = {'nlm', 'primaryId'}
    remap_keys = dict()
    remap_keys['isoAbbreviation'] = 'iso_abbreviation'
    remap_keys['medlineAbbreviation'] = 'medline_abbreviation'
    remap_keys['abbreviationSynonyms'] = 'title_synonyms'
    remap_keys['crossReferences'] = 'cross_references'
    remap_keys['editorsOrAuthors'] = 'editors'
    remap_keys['printISSN'] = 'print_issn'
    remap_keys['onlineISSN'] = 'online_issn'
    editor_keys_to_remove = {'referenceId'}
    remap_editor_keys = dict()
    remap_editor_keys['authorRank'] = 'order'
    remap_editor_keys['firstName'] = 'first_name'
    remap_editor_keys['lastName'] = 'last_name'
    remap_editor_keys['middleNames'] = 'middle_names'
    keys_found = set()

#     url = 'http://localhost:49161/resource/'
    url = 'http://localhost:11223/resource/'
#     headers = {
#         'Authorization': 'Bearer <token_goes_here>',
#         'Content-Type': 'application/json',
#         'Accept': 'application/json'
#     }

    token = ''
    if path.isfile(auth0_file):
        with open(auth0_file, 'r') as auth0_fh:
            token = auth0_fh.read().replace("\n", "")
            auth0_fh.close
    else:
        token = update_token()
    headers = generate_headers(token)

    resource_primary_id_to_curie_file = base_path + 'resource_primary_id_to_curie'
    errors_in_posting_resource_file = base_path + 'errors_in_posting_resource'

    already_processed_primary_id = set()
    if path.isfile(resource_primary_id_to_curie_file):
        with open(resource_primary_id_to_curie_file, 'r') as read_fh:
            for line in read_fh:
                line_data = line.split("\t")
                if line_data[0]:
                    already_processed_primary_id.add(line_data[0].rstrip())

    with open(resource_primary_id_to_curie_file, 'a') as mapping_fh, open(errors_in_posting_resource_file, 'a') as error_fh:
        for fileset in filesets:
            logger.info("processing %s", fileset)
            # if fileset != 'NLM':
            #     continue

            filename = json_storage_path + 'RESOURCE_' + fileset + '.json'
            f = open(filename)
            resource_data = json.load(f)
            # counter = 0
            for entry in resource_data['data']:
                # to debub json from data file before changes
                # json_object = json.dumps(entry, indent=4)
                # print(json_object)

                primary_id = entry['primaryId']
                if primary_id in already_processed_primary_id:
                    # logger.info("%s\talready in", primary_id)
                    # print("already in " + primary_id)
                    continue
                # if primary_id != 'NLM:8404639':
                #     continue

                # counter += 1
                # if counter > 2:
                #     break

                identifiers = set()
                identifiers.add(primary_id)

                for key in keys_to_remove:
                    if key in entry:
                        del entry[key]
                for key in entry:
                    keys_found.add(key)
                    if key in remap_keys:
                        entry[remap_keys[key]] = entry.pop(key)
                if 'cross_references' in entry:
                    for xref in entry['cross_references']:
                        if 'id' in xref:
                            identifiers.add(xref['id'])
                            xref['curie'] = xref.pop('id')
                if 'editors' in entry:
                    for editor in entry['editors']:
                        for key in editor_keys_to_remove:
                            if key in editor:
                                del editor[key]
                        for key in editor:
                            if key in remap_editor_keys:
                                editor[remap_editor_keys[key]] = editor.pop(key)

                # UNCOMMENT to test data by replacing unique data with a timestamp
                #             xref['curie'] = str(datetime.now())
                # entry['iso_abbreviation'] = str(datetime.now())

                post_return = requests.post(url, headers=headers, json=entry)
                response_dict = json.loads(post_return.text)

                print(primary_id + "\ttext " + str(post_return.text))
                print(primary_id + "\tstatus_code " + str(post_return.status_code))

                if (post_return.status_code == 201):
                    response_dict = response_dict.replace('"', '')
                    for identifier in identifiers:
                        logger.info("I %s\t%s", identifier, response_dict)
                        mapping_fh.write("%s\t%s\n" % (identifier, response_dict))
                # if making multiple runs on data that has already gone into api
                # elif (post_return.status_code == 409):
                #     continue
                else:
                    logger.info("ERROR %s primaryId %s message %s", post_return.status_code, primary_id, response_dict['detail'])
                    error_fh.write("ERROR %s primaryId %s message %s\n" % (post_return.status_code, primary_id, response_dict['detail']))

                # to debug json after changes that was sent to api
                # json_object = json.dumps(entry, indent = 4)
                # print(json_object)

        # if wanting to output keys in data for figuring out mapping
        # for key in keys_found:
        #     logger.info("key %s", key)

        mapping_fh.close
        error_fh.close


if __name__ == "__main__":
    """ call main start function """
    logger.info("starting post_resource_to_api.py")

    post_resources()

    logger.info("ending post_resource_to_api.py")

# pipenv run python3 post_resource_to_api.py
