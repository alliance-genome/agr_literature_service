from json import dumps, loads
from os import environ, path
import logging
import logging.config
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

import requests

init_tmp_dir()
base_path = environ.get('XML_PATH')


def generate_headers(token):
    """

    :param token:
    :return:
    """

    authorization = 'Bearer ' + token
    headers = {
        'Authorization': authorization,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    return headers


def update_okta_token():
    """

    :return:
    """

    url = 'https://dev-30456587.okta.com/oauth2/default/v1/token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }
    data_dict = dict()
    data_dict['grant_type'] = 'client_credentials'
    data_dict['client_id'] = environ.get('OKTA_CLIENT_ID')
    data_dict['client_secret'] = environ.get('OKTA_CLIENT_SECRET')
    data_dict['scope'] = 'admin'
    post_return = requests.post(url, headers=headers, data=data_dict)
    logging.warning(post_return.text)
    response_dict = loads(post_return.text)
    token = response_dict['access_token']
    # logger.info("token %s", token)
    okta_file = base_path + 'okta_token'
    with open(okta_file, 'w') as okta_fh:
        okta_fh.write("%s" % (token))
        okta_fh.close
    return token


def get_authentication_token():
    okta_file = base_path + 'okta_token'
    token = ''
    if path.isfile(okta_file):
        with open(okta_file, 'r') as okta_fh:
            token = okta_fh.read().replace("\n", "")
            okta_fh.close
    else:
        token = update_okta_token()
    return token
