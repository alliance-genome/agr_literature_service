"""
SCRUM-5477. Script to load curation_status or topic_entity_tags into database.
The metaData of the input json file has an endpoint that tells us which to load to.
This can be ran from the alliance gocd and has a pipeline setup which is called LoadCurationStatus.
"""
import argparse
import json
import logging
import os
import requests
from os import environ

from agr_cognito_py import get_authentication_token, generate_headers

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_output_files(datafile, metadata, success_records, failed_records):
    base, _ = os.path.splitext(datafile)
    for suffix, records in [("_success.json", success_records), ("_failed.json", failed_records)]:
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
    error_count = 0
    success_records = []
    failed_records = []
    for i, record in enumerate(records, start=1):
        try:
            response = requests.post(url=post_url, json=record, headers=auth_headers)
            if response.status_code == 201:
                success_count += 1
                success_records.append(record)
            else:
                error_count += 1
                failed_records.append(record)
                mess = f"FAILED [{i}/{total}] {record}: {response.status_code} {response.text}"
                logger.error(mess)
        except Exception as e:
            error_count += 1
            failed_records.append(record)
            logger.error(f"ERROR [{i}/{total}] {record}: {e}")

        if success_count * 3 < error_count:
            rate = (success_count / (error_count + success_count)) * 100
            logger.error(f"STOPPING TOO MANY ERRORS: SUCCESS RATE {rate}% last and {i}th record{record['reference_curie']} ")
            write_output_files(datafile, metadata, success_records, failed_records)
            exit(-1)
        if i % 500 == 0:
            logger.info(f"Progress: {i}/{total} (success={success_count}, errors={error_count})")

    write_output_files(datafile, metadata, success_records, failed_records)
    logger.info(f"DONE! Total={total}, Success={success_count}, Errors={error_count}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Load curation status or topic_entity_tag data from a JSON file")
    parser.add_argument("-f", "--json_file",
                        help="Path to the JSON file containing curation status data",
                        type=str, required=True)
    args = parser.parse_args()
    load_data(args.json_file)
