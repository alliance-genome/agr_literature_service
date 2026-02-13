import argparse
import json
import logging
import requests
from os import environ

from agr_cognito_py import get_authentication_token, generate_headers

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

post_url = environ['API_URL'] + "curation_status/"


def load_data(datafile):

    token = get_authentication_token()
    auth_headers = generate_headers(token)

    with open(datafile) as f:
        json_data = json.load(f)

    records = json_data["data"]
    total = len(records)
    logger.info(f"Total records to load: {total}")

    success_count = 0
    error_count = 0
    for i, record in enumerate(records, start=1):
        data = {
            "reference_curie": record["reference_curie"],
            "mod_abbreviation": record["mod_abbreviation"],
            "topic": record["topic"],
            "curation_status": record.get("curation_status"),
            "created_by": record.get("created_by"),
            "updated_by": record.get("updated_by"),
            "date_created": record.get("date_created"),
            "date_updated": record.get("date_updated"),
        }
        try:
            response = requests.post(url=post_url, json=data, headers=auth_headers)
            if response.status_code == 201:
                success_count += 1
            else:
                error_count += 1
                logger.info(f"FAILED [{i}/{total}] {record['reference_curie']} "
                            f"topic={record['topic']}: {response.status_code} {response.text}")
        except Exception as e:
            error_count += 1
            logger.info(f"ERROR [{i}/{total}] {record['reference_curie']}: {e}")

        if success_count * 3 < error_count:
            rate = (success_count / (error_count + success_count)) * 100
            logger.error(f"STOPPING TOO MANY ERRORS: SUCCESS RATE {rate}% last and {i}th record{record['reference_curie']} ")
            exit(-1)
        if i % 500 == 0:
            logger.info(f"Progress: {i}/{total} (success={success_count}, errors={error_count})")

    logger.info(f"DONE! Total={total}, Success={success_count}, Errors={error_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load curation status data from a JSON file")
    parser.add_argument("-f", "--json_file", help="Path to the JSON file containing curation status data")
    args = parser.parse_args()
    load_data(args.json_file)
