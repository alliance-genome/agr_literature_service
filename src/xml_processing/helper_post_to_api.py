from os import environ

import json
import requests

base_path = environ.get('XML_PATH')

auth0_file = base_path + 'auth0_token'


def generate_headers(token):
    authorization = 'Bearer ' + token
    headers = {
        'Authorization': authorization,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    return headers


def update_token():
    url = 'https://alliancegenome.us.auth0.com/oauth/token'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    header_dict = dict()
    header_dict['audience'] = 'alliance'
    header_dict['grant_type'] = 'client_credentials'
    header_dict['client_id'] = environ.get('AUTH0_CLIENT_ID')
    header_dict['client_secret'] = environ.get('AUTH0_CLIENT_SECRET')
    # data for this api must be a string instead of a dict
    header_entry = json.dumps(header_dict)
    # logger.info("data %s data end", header_entry)
    post_return = requests.post(url, headers=headers, data=header_entry)
    # logger.info("post return %s status end", post_return.status_code)
    # logger.info("post return %s text end", post_return.text)
    response_dict = json.loads(post_return.text)
    token = response_dict['access_token']
    # logger.info("token %s", token)
    print("updated token %s" % (token))
    with open(auth0_file, 'w') as auth0_fh:
        auth0_fh.write("%s" % (token))
        auth0_fh.close
    return token
