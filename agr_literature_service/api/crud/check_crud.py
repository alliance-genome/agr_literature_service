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
from os import environ, path
from collections import defaultdict
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


def check_obsolete_entities():

    log_path = environ.get('LOG_PATH', '.')
    log_file = path.join(log_path, "QC/obsolete_entity_report.log")
    date_produced = None
    data = defaultdict(list)

    with open(log_file, 'r') as f:
        for line in f:
            if 'date-produced:' in line:
                date_produced = line.split('date-produced: ')[1].strip()
            else:
                pieces = line.strip().split('\t')
                if len(pieces) > 6:
                    reference_curies_raw = pieces[6] if len(pieces) > 6 else ''
                    reference_curies_list = [curie.strip() for curie in reference_curies_raw.split(',') if curie.strip()]
                    if len(reference_curies_list) > 5:
                        display_curies = ', '.join(reference_curies_list[:5]) + ', ...'
                    else:
                        display_curies = ', '.join(reference_curies_list)
                    data[pieces[0]].append({
                        "entity_type": pieces[1],
                        "entity_status": pieces[2],
                        "entity_curie": pieces[3],
                        "entity_name": pieces[4] if len(pieces) > 4 else None,
                        "reference_count": pieces[5],
                        "reference_curies": display_curies
                    })

    return {
        "date-produced": date_produced,
        "obsolete_entities": dict(data)
    }


def check_redacted_references_with_tags():

    log_path = environ.get('LOG_PATH', '.')
    log_file = path.join(log_path, "QC/redacted_references_with_tags.log")
    date_produced = None
    data = defaultdict(list)

    with open(log_file, 'r') as f:
        for line in f:
            if 'date-produced:' in line:
                date_produced = line.split('date-produced: ')[1].strip()
            else:
                pieces = line.strip().split('\t')
                if len(pieces) >= 3:
                    data[pieces[1]].append({
                        "reference_id": pieces[0]
                    })

    return {
        "date-produced": date_produced,
        "redacted-references": dict(data)
    }


def check_obsolete_pmids():

    log_path = environ.get('LOG_PATH', '.')
    log_file = path.join(log_path, "QC/obsolete_pmid_report.log")
    date_produced = None
    data = defaultdict(list)

    with open(log_file, 'r') as f:
        for line in f:
            if 'date-produced:' in line:
                date_produced = line.split('date-produced: ')[1].strip()
            else:
                pieces = line.strip().split('\t')
                if len(pieces) >= 2:
                    data[pieces[0]].append(pieces[1])

    return {
        "date-produced": date_produced,
        "obsolete_pmids": dict(data)
    }


def check_duplicate_orcids():
    log_path = environ.get('LOG_PATH', '.')
    log_file = path.join(log_path, "QC/duplicate_orcid_report.log")
    date_produced = None
    data = defaultdict(list)

    try:
        with open(log_file, 'r') as f:
            for line in f:
                if 'date-produced:' in line:
                    date_produced = line.split('date-produced: ')[1].strip()
                else:
                    pieces = line.strip().split('\t')
                    if len(pieces) >= 4:
                        data[pieces[0]].append({
                            "reference_curie": pieces[1],
                            "orcid": pieces[2],
                            "author_names": pieces[3]
                        })
    except FileNotFoundError:
        return {"date-produced": None, "duplicate_orcids": {}}

    return {
        "date-produced": date_produced,
        "duplicate_orcids": dict(data)
    }


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
