from json import dumps, loads
from os import environ, path

import requests


# move to parameters between functions
base_path = environ.get("XML_PATH")


def generate_headers(token):
    """

    :param token:
    :return:
    """

    authorization = "Bearer " + token
    headers = {
        "Authorization": authorization,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    return headers


def update_auth0_token():
    """

    :return:
    """

    url = "https://alliancegenome.us.auth0.com/oauth/token"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    data_dict = {"audience": "alliance", "grant_type": "client_credentials", "client_id": environ.get("AUTH0_CLIENT_ID"),
                 "client_secret": environ.get("AUTH0_CLIENT_SECRET")}

    # data for this api must be a string instead of a dict
    header_entry = dumps(data_dict)
    # logger.info("data %s data end", header_entry)
    post_return = requests.post(url, headers=headers, data=header_entry)
    # logger.info("post return %s status end", post_return.status_code)
    # logger.info("post return %s text end", post_return.text)
    response_dict = loads(post_return.text)
    token = response_dict["access_token"]
    # logger.info("token %s", token)
    auth0_file = base_path + "auth0_token"
    with open(auth0_file, "w") as auth0_fh:
        auth0_fh.write("%s" % token)
        auth0_fh.close()

    return token


def update_okta_token():
    """

    :return:
    """

    url = "https://dev-30456587.okta.com/oauth2/default/v1/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }

    data_dict = {"grant_type": "client_credentials", "client_id": environ.get("OKTA_CLIENT_ID"),
                 "client_secret": environ.get("OKTA_CLIENT_SECRET"), "scope": "admin"}
    post_return = requests.post(url, headers=headers, data=data_dict)
    # logger.info("token %s", token)
    response_dict = loads(post_return.text)
    token = response_dict["access_token"]
    # logger.info("token %s", token)
    okta_file = base_path + "okta_token"
    with open(okta_file, "w") as okta_fh:
        okta_fh.write("%s" % token)
        okta_fh.close()
    return token


def update_token():
    """

    :return:
    """

    token = update_okta_token()
    return token


def get_authentication_token():
    """

    :return:
    """

    okta_file = base_path + "okta_token"
    token = ""
    if path.isfile(okta_file):
        with open(okta_file) as okta_fh:
            token = okta_fh.read().strip()
    else:
        token = update_token()
    return token


def process_api_request(
    method, url, headers, json_data, primary_id, mapping_fh, error_fh
):
    """
    Call API with method, url, headers, optional json of data, agr reference curie,
    optional mapping filehandle, optional error filehandle

    :param method:
    :param url:
    :param headers:
    :param json_data:
    :param primary_id:
    :param mapping_fh:
    :param error_fh:
    :return:
    """
    # output the json getting posted to the API
    # json_object = json.dumps(json_data, indent = 4)
    # print(json_object)

    log_info = ("")  # for now until figuring out how to get a called function use the logger

    request_return = requests.request(method, url=url, headers=headers, json=json_data)
    process_text = str(request_return.text)
    process_status_code = request_return.status_code
    # logger.info(primary_id + ' text ' + process_text)
    # logger.info(primary_id + ' status_code ' + str(process_status_code))

    response_dict = dict()
    if not method == "DELETE" and request_return.status_code == 204:
        try:
            response_dict = loads(request_return.text)
        except ValueError:
            # logger.info("%s\tValueError", primary_id)
            log_info += "api error ValueError: " + primary_id + " did not return json"
            # if error_fh is not None:
            #     error_fh.write("api error %s primaryId did not return json\n" % (primary_id))
            return headers, process_text, process_status_code, log_info

    if method == "POST" and request_return.status_code == 201:
        pass
        # response_dict = str(response_dict).replace('"', '')
        # logger.info("%s\t%s", primary_id, response_dict)
        # log_info += primary_id + "\t" + response_dict
        # if mapping_fh is not None:
        #     mapping_fh.write("%s\t%s\n" % (primary_id, response_dict))
    elif method == "PATCH" and request_return.status_code == 202:
        pass
    elif method == "DELETE" and request_return.status_code == 204:
        pass
        # logger.info('%s\t%s\tsuccess', primary_id, url)
        # log_info += primary_id + '\t' + url + '\tsuccess'
    elif request_return.status_code == 401:
        # logger.info('%s\texpired token', primary_id)
        log_info += "api error 401\texpired token\t" + primary_id
        # if mapping_fh is not None:
        #     mapping_fh.write("%s\t%s\n" % (primary_id, response_dict))
        token = update_token()
        headers = generate_headers(token)
        response_tuple = process_api_request(
            method, url, headers, json_data, primary_id, mapping_fh, error_fh
        )
        headers = response_tuple[0]
        process_text = response_tuple[1]
        process_status_code = response_tuple[2]
        # log_info += response_tuple[3]
    elif request_return.status_code == 500:
        # logger.info("%s\tFAILURE", primary_id)
        log_info += primary_id + "\t500 FAILURE"
        # if mapping_fh is not None:
        #     mapping_fh.write("%s\t%s\n" % (primary_id, response_dict))
    # if redoing a run and want to skip errors of data having already gone in
    # elif (request_return.status_code == 409):
    #     continue
    else:
        # usually when the api will have a string in response_dict['detail'], but if
        # it fails because e.g. there isn't a title, it will give
        # {"detail": [{"loc": ["body", "title"], "msg": "field required", "type": "value_error.missing"}]}
        # so safer to json.dumps messages
        # detail = ''
        # if 'detail' in response_dict:
        #     detail = response_dict['detail']
        detail = dumps(response_dict)
        # logger.info('ERROR %s primaryId %s message %s', request_return.status_code, primary_id, detail)
        log_info += ("api error unexpected response " + str(request_return.status_code) + " primaryId " + primary_id + " message " + detail)

        # if error_fh is not None:
        #     error_fh.write("ERROR %s primaryId %s message %s\n" % (str(request_return.status_code), primary_id, detail))

    return headers, process_text, process_status_code, log_info
