"""
SCRUM-5477. Script to load curation_status or topic_entity_tags into database.
The metaData of the input json file has an endpoint that tells us which to load to.
This can be ran from the alliance gocd and has a pipeline setup which is called LoadCurationStatus.
"""
import argparse
import json
import logging
import os
import time
import requests
from os import environ

from agr_cognito_py import get_authentication_token, generate_headers

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Transient upstream errors that warrant a retry rather than being recorded as a
# permanent failure (502/503/504 come from nginx when the API backend is briefly
# unavailable, restarting, or times out).
TRANSIENT_STATUS_CODES = {502, 503, 504}
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 2


def post_with_retry(post_url, record, auth_headers):
    """POST a record, retrying transient upstream failures with linear backoff.

    Retries on connection-level errors and on 502/503/504 responses. Returns the
    final response (which the caller treats as a failure if still not 201), or
    re-raises the last connection error once retries are exhausted.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url=post_url, json=record, headers=auth_headers)
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                raise
            wait = RETRY_BACKOFF_SECONDS * attempt
            logger.warning(f"Transient error on attempt {attempt}/{MAX_RETRIES} ({e}); retrying in {wait}s")
            time.sleep(wait)
            continue
        if response.status_code in TRANSIENT_STATUS_CODES and attempt < MAX_RETRIES:
            wait = RETRY_BACKOFF_SECONDS * attempt
            logger.warning(
                f"Transient {response.status_code} on attempt {attempt}/{MAX_RETRIES}; retrying in {wait}s")
            time.sleep(wait)
            continue
        return response
    # Unreachable: the final attempt always returns above or re-raises.
    raise RuntimeError("post_with_retry exhausted retries without returning")


def is_already_present(response):
    """Return True if the response indicates the record was already in the database.

    For the idempotent topic_entity_tag endpoint a re-POST of a record that was
    actually committed (e.g. when a previous run's response was lost to a 502)
    comes back as 200 (existing tag, note appended) or 409 with
    reason="duplicate". These are not real failures: the data is present. Other
    409 reasons (opposite_negation, different_creator) are genuine conflicts and
    are treated as failures.

    The workflow_tag endpoint signals an already-present record differently: it
    proactively detects a duplicate (same reference/mod/tag) and returns 422 with
    a plain-string detail ending "can not create duplicate record." That is also a
    record that is already present, not a real failure. The match is on the
    specific message so the endpoint's other genuine 422s (e.g. "Reference ...
    does not exist", "Mod ... does not exist") stay classified as errors.
    """
    if response.status_code == 200:
        return True
    if response.status_code == 409:
        try:
            detail = response.json().get("detail")
        except (ValueError, AttributeError):
            return False
        if isinstance(detail, dict) and detail.get("reason") == "duplicate":
            return True
    if response.status_code == 422:
        try:
            detail = response.json().get("detail")
        except (ValueError, AttributeError):
            return False
        if isinstance(detail, str) and "can not create duplicate record" in detail:
            return True
    return False


def is_missing_xref(response):
    """Return True if the record's reference_curie has no cross_reference row.

    The workflow_tag endpoint resolves reference_curie via normalize_reference_curie,
    which returns 404 "The XREF <curie> is not in the cross_reference table" when the
    reference is not in the database at all. This is a genuine data gap rather than a
    transient failure or a duplicate, so these records are segregated into their own
    output file instead of the general error bucket.
    """
    if response.status_code == 404:
        try:
            detail = response.json().get("detail")
        except (ValueError, AttributeError):
            return False
        if isinstance(detail, str) and "is not in the cross_reference table" in detail:
            return True
    return False


def write_output_files(datafile, metadata, success_records, already_present_records,
                       failed_records, missing_xref_records):
    base, _ = os.path.splitext(datafile)
    for suffix, records in [("_success.json", success_records),
                            ("_already_present.json", already_present_records),
                            ("_failed.json", failed_records),
                            ("_missing_xref.json", missing_xref_records)]:
        output_path = base + suffix
        output_data = {"metaData": metadata, "data": records}
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=3)
        logger.info(f"Wrote {len(records)} records to {output_path}")


def load_data(datafile):

    token = get_authentication_token()
    auth_headers = generate_headers(token)

    with open(datafile) as f:
        json_data = json.load(f)

    # Get the end point from the metaData.
    metadata = json_data['metaData']
    post_url = environ['API_URL'] + metadata["endpoint"] + "/"
    logger.info(f'Loading data for {metadata["endpoint"]}')
    logger.info(f'Into end point {post_url}')

    records = json_data["data"]
    total = len(records)
    logger.info(f"Total records to load: {total}")

    success_count = 0
    already_present_count = 0
    missing_xref_count = 0
    error_count = 0
    success_records = []
    already_present_records = []
    missing_xref_records = []
    failed_records = []
    count = 0
    for i, record in enumerate(records, start=1):
        count += 1
        try:
            response = post_with_retry(post_url, record, auth_headers)
            if response.status_code == 201:
                success_count += 1
                success_records.append(record)
            elif is_already_present(response):
                already_present_count += 1
                already_present_records.append(record)
            elif is_missing_xref(response):
                missing_xref_count += 1
                missing_xref_records.append(record)
            else:
                error_count += 1
                failed_records.append(record)
                mess = f"FAILED [{i}/{total}] {record}: {response.status_code} {response.text}"
                logger.error(mess)
        except Exception as e:
            error_count += 1
            failed_records.append(record)
            logger.error(f"ERROR [{i}/{total}] {record}: {e}")

        if count > 20 and success_count * 3 < error_count:
            rate = (success_count / (error_count + success_count)) * 100
            logger.error(f"STOPPING TOO MANY ERRORS: SUCCESS RATE {rate}% last and {i}th record{record['reference_curie']} ")
            write_output_files(datafile, metadata, success_records, already_present_records,
                               failed_records, missing_xref_records)
            exit(-1)
        if i % 500 == 0:
            logger.info(f"Progress: {i}/{total} (success={success_count}, "
                        f"already_present={already_present_count}, "
                        f"missing_xref={missing_xref_count}, errors={error_count})")

    write_output_files(datafile, metadata, success_records, already_present_records,
                       failed_records, missing_xref_records)
    logger.info(f"DONE! Total={total}, Success={success_count}, "
                f"AlreadyPresent={already_present_count}, "
                f"MissingXref={missing_xref_count}, Errors={error_count}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Load curation status or topic_entity_tag data from a JSON file")
    parser.add_argument("-f", "--json_file",
                        help="Path to the JSON file containing curation status data",
                        type=str, required=True)
    args = parser.parse_args()
    load_data(args.json_file)
