import requests
import json
from os import environ

api_port = environ.get('API_PORT', '8080')
api_server = environ.get('API_SERVER', 'localhost')
file_name = "./sample_curies.txt"


#### methods that do the work
def use_curl(curies, max_number, translate=True, count_start=0, verbose=False):

    count = 0
    while(count <= max_number):
        url = 'http://' + api_server + ':' + api_port + '/reference/' + curies[count + count_start]
        # logger.info("get AGR reference info from database %s", url)
        get_return = requests.get(url)
        if translate:
            json.loads(get_return.text)
        if verbose:
            if count < 5:
                print(url)
        count += 1
    return count_start + count
