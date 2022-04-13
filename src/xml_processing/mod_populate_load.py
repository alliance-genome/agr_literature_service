
# import json
import logging
import logging.config
from os import environ, path

from helper_post_to_api import (generate_headers, get_authentication_token,
                                process_api_request)

# post to api data for eight mods
# python mod_populate_load.py


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def post_mods():      # noqa: C901
    """

    :return:
    """

    api_port = environ.get('API_PORT')
    base_path = environ.get('XML_PATH')

    token = get_authentication_token()
    headers = generate_headers(token)
    api_server = environ.get('API_SERVER', 'localhost')
    url = 'http://' + api_server + ':' + api_port + '/reference/'

    mod_data = [{"abbreviation": "FB",
                 "short_name": "FlyBase",
                 "full_name": "FlyBase"},
                {"abbreviation": "WB",
                 "short_name": "WormBase",
                 "full_name": "WormBase"},
                {"abbreviation": "ZFIN",
                 "short_name": "ZFIN",
                 "full_name": "Zebrafish Information Network"},
                {"abbreviation": "SGD",
                 "short_name": "SGD",
                 "full_name": "Saccharomyces Genome Database"},
                {"abbreviation": "MGI",
                 "short_name": "MGD",
                 "full_name": "Mouse Genome Database"},
                {"abbreviation": "RGD",
                 "short_name": "RGD",
                 "full_name": "Rat Genome Database"},
                {"abbreviation": "XB",
                 "short_name": "Xenbase",
                 "full_name": "Xenbase"},
                {"abbreviation": "GO",
                 "short_name": "GOC",
                 "full_name": "Gene Ontology Consortium"}]

    process_results = []
    errors_in_posting_mod_file = base_path + 'errors_in_posting_mod'
    with open(errors_in_posting_mod_file, 'a') as error_fh:
        for new_entry in mod_data:
            # output what is sent to API after converting file data
            # json_object = json.dumps(new_entry, indent=4)
            # print(json_object)

            url = 'http://' + api_server + ':' + api_port + '/mod/'
            api_response_tuple = process_api_request('POST', url, headers, new_entry, new_entry['abbreviation'], None, None)
            headers = api_response_tuple[0]
            response_text = api_response_tuple[1]
            response_status_code = api_response_tuple[2]
            log_info = api_response_tuple[3]

            if (response_status_code == 201):
                process_result = dict()
                process_result['text'] = response_text
                process_result['status_code'] = response_status_code
                process_results.append(process_result)
                logger.info("%s\t%s", new_entry['abbreviation'], response_text)
                if log_info:
                    logger.info(log_info)
            else:
                logger.info("api error %s primaryId %s message %s", str(response_status_code), new_entry['abbreviation'], response_text)
                error_fh.write("api error %s primaryId %s message %s\n" % (str(response_status_code), new_entry['abbreviation'], response_text))
        error_fh.close
    return process_results


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("Starting mod_populate_load.py")
    post_mods()
    logger.info("ending mod_populate_load.py")

# pipenv run python mod_populate_load.py
