"""
api.py - API for tthe md5sum comparisons

Paulo Nuin Apr 2022

Redis DBs

XML/JSON
0 - XML/JSON md5sum "old" files
1 - XML/JSON md5sum "new" files
10 - XML/JSON md5sum new and changed files

DQM
11 - FB
12 - MGD
13 - RGD
14 - SGD
15 - WB
16 - ZFIN
17 - GO
18 - XB

"""


import flask
import json
import redis

api = flask.Flask(__name__)


@api.route("/", methods=['GET'])
def index():
    """

    Get all the keys in DB 1, which is storing all the new items

    :return:
    """

    r = redis.StrictRedis(host='localhost', port=6379, db=1, password="password", decode_responses=True)
    keys = r.keys()
    data = {}
    for key in keys:
        data[key] = r.get(key)

    return json.dumps(data)


@api.route("/new/<string:key>", methods=['GET'])
def get_new_key(key):
    """

    get a specific key from DB 1, new values

    :param key:
    :return:
    """

    r = redis.StrictRedis(host='localhost', port=6379, db=1, password="password", decode_responses=True)
    data = r.get(key)
    return json.dumps(data)


@api.route("/old/<string:key>", methods=['GET'])
def get_old_key(key):
    """

    get a specific key from DB 0, old values

    :param key:
    :return:
    """
    r = redis.StrictRedis(host='localhost', port=6379, db=0, password="password", decode_responses=True)
    data = r.get(key)
    return json.dumps(data)


@api.route("/all_changes", methods=['GET'])
def get_all_changes():
    """

    Get all the keys in DB 0, which is storing all the changed items, can be iterated over with a
    generator

    :return:
    """
    r = redis.StrictRedis(host='localhost', port=6379, db=10, password="password", decode_responses=True)
    keys = r.keys()
    data = {}
    for key in keys:
        data[key] = r.get(key)

    return json.dumps(data)


@api.route("/dqm_changes/<string:mod>", methods=['GET'])
def get_dqm_changes(mod):

    if mod == "fb":
        db = 11
    elif mod == "mgd":
        db = 12
    elif mod == "rgd":
        db = 13
    elif mod == "sgd":
        db = 14
    elif mod == "wb":
        db = 15
    elif mod == "zfin":
        db = 16
    elif mod == "xb":
        db = 18

    r = redis.StrictRedis(host='localhost', port=6379, db=db, password="password", decode_responses=True)
    keys = r.keys()
    data = {}
    for key in keys:
        data[key] = r.get(key)

    return json.dumps(data)


if __name__ == "__main__":
    """
    Run the API
    """

    api.run(host="0.0.0.0", port=8080, debug=True)
