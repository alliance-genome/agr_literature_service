from os import path
from os import environ
import json
import requests
# import argparse
import logging
import logging.config

from helper_post_to_api import generate_headers, update_token

# pipenv run python get_references_cross_references.py

# about 1 minute 13 seconds to generate file with cross_references and is_obsolete
# about 45 seconds to generate file when it only had cross_references without is_obsolete
# generate reference_curie_to_xref file mapping alliance reference curies to cross_references identifiers from database


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('post_comments_corrections_to_api')


def update_reference_cross_reference():
    """

    :return:
    """

    api_port = environ.get('API_PORT')
    base_path = environ.get('XML_PATH')

    okta_file = base_path + 'okta_token'
    token = ''
    if path.isfile(okta_file):
        with open(okta_file, 'r') as okta_fh:
            token = okta_fh.read().replace("\n", "")
            okta_fh.close
    else:
        token = update_token()
    headers = generate_headers(token)

    url = 'http://localhost:' + api_port + '/bulk_download/references/external_ids/'
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

    ref_xref_file = 'reference_curie_to_xref'
    with open(ref_xref_file, "w") as ref_xref_file_fh:
        ref_xref_file_fh.write(mapping_output)


if __name__ == "__main__":
    """
    
    call main start function
    
    """

    update_reference_cross_reference()

    logger.info("Done Processing")
