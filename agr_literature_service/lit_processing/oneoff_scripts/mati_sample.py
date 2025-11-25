"""
# generate an authentication token and request reference curie from mati.  first test getting the latest,
# then create one new one.  for batch request higher 'value' and add to first.curie, probably
# also validate against last.curie
# python mati_sample.py

"""

import logging.config
from os import path
from agr_cognito_auth import get_authentication_token, generate_headers
import requests

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def do_everything():
    token = get_authentication_token()
    # logger.info(f"token {token}")
    headers = generate_headers(token)
    headers['subdomain'] = 'reference'  # reference or resource or person
    url = 'https://alpha-mati.alliancegenome.org/api/identifier'

    # to request latest curie
    post_return = requests.get(url, headers=headers)
    # returns : {"value":"AGRKB:101000000000002"}
    logger.info(post_return.text)
    result_dict = post_return.json()
    logger.info(f"Latest is {result_dict['value']}")

    # to request one new curie
    headers['value'] = '1'  # amount of curies to request
    post_return = requests.post(url, headers=headers)
    # returns : {"first":{"counter":3,"curie":"AGRKB:101000000000003","subdomain_code":"101","subdomain_name":"reference"},"last":{"counter":3,"curie":"AGRKB:101000000000003","subdomain_code":"101","subdomain_name":"reference"}}
    logger.info(post_return.text)
    result_dict = post_return.json()
    logger.info(f"Generated first curie : {result_dict['first']['curie']}")


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("Starting mati_sample.py")
    do_everything()
    logger.info("Ending mati_sample.py")
