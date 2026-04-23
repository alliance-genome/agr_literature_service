import argparse
import logging
from os import environ

import requests
from agr_cognito_py import get_authentication_token, generate_headers

from agr_literature_service.api.models import PersonModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

MATI_PERSON_PREFIX = "AGRKB:103"
MATI_PERSON_CURIE_WIDTH = 12
SCRIPT_USER_ID = "update_person_curie_script"


def get_mati_headers(subdomain):
    token = get_authentication_token()
    headers = generate_headers(token)
    headers["subdomain"] = subdomain
    return headers


def _format_curie(counter):
    return f"{MATI_PERSON_PREFIX}{str(counter).rjust(MATI_PERSON_CURIE_WIDTH, '0')}"


def get_current_mati_counter(url, headers):
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    latest_curie = res.json()["value"]
    return int(latest_curie.replace(MATI_PERSON_PREFIX, ""))


def reserve_curies(url, headers, count):
    req_headers = dict(headers)
    req_headers["value"] = str(count)
    res = requests.post(url, headers=req_headers)
    res.raise_for_status()
    payload = res.json()
    start_counter = payload["first"]["counter"]
    reserved = [(start_counter + i, _format_curie(start_counter + i)) for i in range(count)]
    expected_last = reserved[-1][1]
    actual_last = payload["last"]["curie"]
    if expected_last != actual_last:
        raise RuntimeError(
            f"MATI range mismatch: expected last curie {expected_last}, got {actual_last}"
        )
    return reserved


def run_strict(db, url, headers, logger):
    counter = get_current_mati_counter(url, headers)
    if counter != 0:
        logger.error(f"STRICT: MATI person counter is {counter}, expected 0. Aborting.")
        return False

    non_null = (
        db.query(PersonModel)
        .filter(PersonModel.curie.isnot(None))
        .order_by(PersonModel.person_id)
        .all()
    )
    if non_null:
        logger.error(
            f"STRICT: {len(non_null)} person row(s) already have a curie. Aborting."
        )
        for p in non_null:
            logger.error(f"  person_id={p.person_id} curie={p.curie}")
        return False

    persons = db.query(PersonModel).order_by(PersonModel.person_id).all()
    if not persons:
        logger.info("STRICT: no person rows to update. Nothing to do.")
        return True

    ids = [p.person_id for p in persons]
    if ids[0] != 1 or ids[-1] != len(ids):
        logger.error(
            "STRICT: person_ids not contiguous 1..N "
            f"(count={len(ids)}, min={ids[0]}, max={ids[-1]}). Aborting."
        )
        return False

    max_id = ids[-1]
    reserved = reserve_curies(url, headers, max_id)
    counter_to_curie = {c: curie for c, curie in reserved}

    for p in persons:
        curie = counter_to_curie.get(p.person_id)
        if curie is None:
            raise RuntimeError(f"No curie reserved for person_id={p.person_id}")
        p.curie = curie
        logger.info(f"{p.person_id} NULL {curie}")

    db.commit()
    logger.info(f"STRICT: assigned {len(persons)} curies in one transaction.")
    return True


def run_best_effort(db, url, headers, logger):
    persons = (
        db.query(PersonModel)
        .filter(PersonModel.curie.is_(None))
        .order_by(PersonModel.person_id)
        .all()
    )
    if not persons:
        logger.info("BEST-EFFORT: no persons with NULL curie. Nothing to do.")
        return True

    reserved = reserve_curies(url, headers, len(persons))
    for p, (_, curie) in zip(persons, reserved):
        p.curie = curie
        logger.info(f"{p.person_id} NULL {curie}")

    db.commit()
    logger.info(f"BEST-EFFORT: assigned {len(persons)} curies in one transaction.")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Backfill AGRKB:103 curies on person rows via MATI."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--strict",
        action="store_true",
        default=True,
        help="Prod mode (default): require MATI counter=0, all NULL curies, contiguous person_ids.",
    )
    mode.add_argument(
        "--best-effort",
        dest="strict",
        action="store_false",
        help="Test/alpha mode: skip preconditions, assign curies in person_id order.",
    )
    args = parser.parse_args()

    logging.basicConfig(format="%(message)s", level=logging.INFO)
    logger = logging.getLogger()

    db = create_postgres_session(False)
    try:
        set_global_user_id(db, SCRIPT_USER_ID)
        url = environ["ID_MATI_URL"]
        headers = get_mati_headers("person")
        if args.strict:
            ok = run_strict(db, url, headers, logger)
        else:
            ok = run_best_effort(db, url, headers, logger)
        if not ok:
            raise SystemExit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
