from os import environ

import json
import requests

base_path = environ.get('XML_PATH')


def generate_headers(token):
    authorization = 'Bearer ' + token
    headers = {
        'Authorization': authorization,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    return headers


def update_auth0_token():
    url = 'https://alliancegenome.us.auth0.com/oauth/token'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    data_dict = dict()
    data_dict['audience'] = 'alliance'
    data_dict['grant_type'] = 'client_credentials'
    data_dict['client_id'] = environ.get('AUTH0_CLIENT_ID')
    data_dict['client_secret'] = environ.get('AUTH0_CLIENT_SECRET')
    # data for this api must be a string instead of a dict
    header_entry = json.dumps(data_dict)
    # logger.info("data %s data end", header_entry)
    post_return = requests.post(url, headers=headers, data=header_entry)
    # logger.info("post return %s status end", post_return.status_code)
    # logger.info("post return %s text end", post_return.text)
    response_dict = json.loads(post_return.text)
    token = response_dict['access_token']
    # logger.info("token %s", token)
    auth0_file = base_path + 'auth0_token'
    with open(auth0_file, 'w') as auth0_fh:
        auth0_fh.write("%s" % (token))
        auth0_fh.close
    return token


def update_okta_token():
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
    # logger.info("token %s", token)
    response_dict = json.loads(post_return.text)
    token = response_dict['access_token']
    # logger.info("token %s", token)
    okta_file = base_path + 'okta_token'
    with open(okta_file, 'w') as okta_fh:
        okta_fh.write("%s" % (token))
        okta_fh.close
    return token


def update_token():
    token = update_okta_token()
    return token
