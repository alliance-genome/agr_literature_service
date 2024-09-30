"""
check_crud.py

This "crud" only allows get, it is not intended to chage any setting via this code.

General checks, that may be useful for debugging etc.
So maybe not useful for general users but it is unlikely they would use the
swagger interface so should be fine.
Also test for Ateam api, but more could be added.
==============
"""
import json
import urllib.request
from os import environ

from sqlalchemy.orm import Session
from sqlalchemy import text
from fastapi_okta.okta_utils import get_authentication_token

import logging

logger = logging.getLogger(__name__)


def check_ateam_api():
    token = get_authentication_token()
    ateam_api_base_url = environ.get('ATEAM_API_URL')
    ateam_health = ateam_api_base_url.replace('api', 'health')
    try:
        request = urllib.request.Request(url=ateam_health)
        request.add_header("Authorization", f"Bearer {token}")
        request.add_header("Content-type", "application/json")
        request.add_header("Accept", "application/json")
        with urllib.request.urlopen(request) as response:
            resp = response.read().decode("utf8")
            resp_obj = json.loads(resp)
            return resp_obj
    except Exception as e:
        logger.error(f"Exception checking ateam api: {e}")
        return {}


def check_database(db: Session):
    res = {}
    query = "select version_num from alembic_version"
    try:
        rows = db.execute(text(query)).fetchall()
        alembic_version = []
        for row in rows:
            alembic_version.append(row[0])
            print(row[0])
        if len(alembic_version) != 1:
            res['alembic_version'] = f"Problem we do not have 1 value we have: {alembic_version}"
        else:
            res['alembic_version'] = alembic_version[0]
    except Exception as e:
        res['alembic_version'] = f"Unable to query database for alembic version: {e}"

    query = "select count(1) from reference"
    try:
        rows = db.execute(text(query)).fetchall()
        # ref_count = rows[0]
        res['ref_count'] = rows[0][0]
    except Exception as e:
        res['ref_count'] = f"Unable to query database for number of references: {e}"

    return res


def show_environments():
    """
    But only those that are not sensitive. i.e. NO passwords etc
    """
    res = {}
    for test_env in ['API_PORT', 'API_SERVER', 'XML_PATH', 'ENV_STATE',
                     'PSQL_HOST', 'PSQL_PORT', 'PSQL_DATABASE',
                     'HOST', 'ATEAM_API_URL']:
        res[test_env] = environ.get(test_env)

    return res
