
import json
import requests
from os import environ, path
import logging
import logging.config

from datetime import datetime

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

# pipenv run python post_resource_to_api.py

# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
base_path = environ.get('XML_PATH')

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

    url = 'http://localhost:49161/resource/'
    headers = {
        'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IllYOGpVX2NObGgzRFBUT2NPNTVKeSJ9.eyJpc3MiOiJodHRwczovL2FsbGlhbmNlZ2Vub21lLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJWVGhRVENKSzkwSjBjS0s5bG9CbTlZQmRSaDB6YWl1bkBjbGllbnRzIiwiYXVkIjoiYWxsaWFuY2UiLCJpYXQiOjE2MjE4OTg4MTYsImV4cCI6MTYyMTk4NTIxNiwiYXpwIjoiVlRoUVRDSks5MEowY0tLOWxvQm05WUJkUmgwemFpdW4iLCJzY29wZSI6InJlYWQ6bWV0YWRhdGEiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMifQ.OArJLkYDc06XQFfKTgtzRsDBHdJ0-WOT7rlKfcTV7CC6lzJZUEv8QqOQFzxDgjJuF9tE2LudD7FgdQhS6ooddnVRDVe-f0BtoYPXbqurrKxgOgYSwCBnWEX6Ze-hXID0QTmqhz65GKAKnB3_52NWOaioZWpjnomw8KlSo6Hi3p15ypaf4SnXsFc8owGMphEzejyGx6LNpktFIg15TDvDmq8ZVt20H7c8F69gpjTZFmlzzsNOTeSDNRGCC7M72LHAe8gTDXBguHegdyEvsq4NKB47kdHGIesccjJqstFPLcVGGnZI0Q6PlJSpHqXDbx_T-2tPEfdbOhjdUH8mQydySw',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    resource_primary_id_to_curie_file = base_path + 'resource_primary_id_to_curie'
    errors_in_posting_resource_file = base_path + 'errors_in_posting_resource'

    already_processed_primary_id = set()
    with open(resource_primary_id_to_curie_file, 'r') as read_fh:
        for line in read_fh:
            line_data = line.split("\t")
            if line_data[0]:
                already_processed_primary_id.add(line_data[0].rstrip())

    with open(resource_primary_id_to_curie_file, 'a') as mapping_fh, open(errors_in_posting_resource_file, 'a') as error_fh:
        for fileset in filesets:
            # if fileset != 'NLM':
            #     continue

            filename = json_storage_path + 'RESOURCE_' + fileset + '.json'
            f = open(filename)
            resource_data = json.load(f)
#             counter = 0
            for entry in resource_data['data']:
                # json_object = json.dumps(entry, indent=4)
                # print(json_object)

                primary_id = entry['primaryId']
                if primary_id in already_processed_primary_id:
                    continue
#                 if primary_id != 'NLM:8404639':
#                     continue

#                 counter += 1
#                 if counter > 2:
#                     break

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
#                             xref['curie'] = str(datetime.now())
#                 entry['iso_abbreviation'] = str(datetime.now())

                post_return = requests.post(url, headers=headers, json=entry)
                response_dict = json.loads(post_return.text)

#                 print(primary_id + 'text ' + str(post_return.text))
#                 print(primary_id + 'status_code ' + str(post_return.status_code))

                if (post_return.status_code == 201):
                    response_dict = response_dict.replace('"', '')
                    logger.info("%s\t%s", primary_id, response_dict)
                    mapping_fh.write("%s\t%s\n" % (primary_id, response_dict))
#                 elif (post_return.status_code == 409):
#                     continue
                else:
                    logger.info("ERROR %s primaryId %s message %s", post_return.status_code, primary_id, response_dict['detail'])
                    error_fh.write("ERROR %s primaryId %s message %s\n" % (post_return.status_code, primary_id, response_dict['detail']))

#                 json_object = json.dumps(entry, indent = 4)
#                 print(json_object)

#    if wanting to output keys in data for figuring out mapping
#        for key in keys_found:
#            logger.info("key %s", key)

        mapping_fh.close
        error_fh.close


if __name__ == "__main__":
    """ call main start function """
    logger.info("starting post_resource_to_api.py")

    post_resources()

#     pubmed_by_nlm = load_pubmed_resource()
#     pubmed_by_nlm = load_zfin_resource(pubmed_by_nlm)
#     pubmed_by_nlm = load_fb_resource(pubmed_by_nlm)
#     save_pubmed_resource(pubmed_by_nlm)

    logger.info("ending post_resource_to_api.py")

# pipenv run python post_resource_to_api.py
