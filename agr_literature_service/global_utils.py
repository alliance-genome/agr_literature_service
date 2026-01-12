from collections.abc import Hashable
import functools
from os import environ
import requests
from sqlalchemy import text
# from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
#    create_postgres_session
from agr_cognito_py import get_authentication_token, generate_headers


class memoized(object):
    """Decorator. Caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned
   (not reevaluated).
   """

    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        if not isinstance(args, Hashable):
            # uncacheable. a list, for instance.
            # better to not cache than blow up.
            return self.func(*args)
        if args in self.cache:
            return self.cache[args]
        else:
            value = self.func(*args)
            self.cache[args] = value
            return value

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)


def execute_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)
    wrapper.has_run = False
    return wrapper


def get_next_curie(subdomain, db=None):  # pragma: no cover

    if environ.get('ENV_STATE') and environ.get('ENV_STATE') != 'test':
        token = get_authentication_token()
        headers = generate_headers(token)
        headers['subdomain'] = subdomain
        url = environ['ID_MATI_URL']
        headers['value'] = '1'
        res = requests.post(url, headers=headers)
        res_json = res.json()
        new_curie = res_json['first']['curie']
        return new_curie
    return get_next_local_curie(subdomain, db)


def get_next_local_curie(subdomain, db):

    ### it is only for testing purpose
    # db_session = db
    # if db is None:
    #    db_session = create_postgres_session(False)
    curie_start = "AGRKB:102"
    rs = None
    if subdomain == 'reference':
        curie_start = "AGRKB:101"
        rs = db.execute(text("SELECT curie FROM reference order by reference_id desc limit 1"))
    else:
        rs = db.execute(text("SELECT curie FROM resource order by resource_id desc limit 1"))
    rows = None
    if rs:
        rows = rs.fetchall()
    if rows and len(rows) > 0:
        last_curie = rows[0][0]
    else:
        last_curie = curie_start + "000000000000"
    number_part = last_curie.replace(curie_start, "")
    number = int(number_part)
    number += 1
    new_curie = curie_start + str(number).rjust(12, "0")

    return new_curie


def get_next_reference_curie(db=None):  # pragma: no cover

    return get_next_curie('reference', db)


def get_next_resource_curie(db=None):  # pragma: no cover

    return get_next_curie('resource', db)
