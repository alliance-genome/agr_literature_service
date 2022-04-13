"""
dqm_md5.py - MD5 checksum calculation

Paulo Nuin Apr 2022

"""


import hashlib
import json
import logging
import tracemalloc
import os
import click
import coloredlogs
import pandas as pd
# from mongita import MongitaClientDisk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")


def hash_df(df):
    """

    :param df:
    :return:
    """

    logger.info("Hashing dataframe")
    hashes = []
    for _index, row in df.iterrows():
        hashes.append(hashlib.md5(str(row).encode('utf-8')).hexdigest())
    df['md5'] = hashes

    return df


def read_dqm_file(file_name):
    """

    :param file_name:
    :return:
    """

    logger.info(f"Reading file {file_name}")
    with open(file_name) as f:
        data = json.loads(f.read())

    df = pd.json_normalize(data['data'])

    new_index = ["title",
                 "datePublished",
                 "abstract",
                 "primaryId",
                 "pages",
                 "allianceCategory",
                 "tags",
                 "citation",
                 "crossReferences",
                 "volume",
                 "authors",
                 "MODReferenceTypes",
                 "resourceAbbreviation",
                 "publisher"]

    df = df.reindex(columns=new_index)
    df = hash_df(df)

    return df


def sort_json(file_name):
    """

    :param file_name:
    :return:
    """

    logger.info(f"Sorting file {file_name}")
    a = json.load(open(file_name))
    b = json.dumps(a, sort_keys=True, indent=4)

    result = open(f"{file_name}_sorted.json", "w")
    result.write(b)
    result.close()

    return f"{file_name}_sorted.json"


def get_new_items(old_dqm, new_dqm):
    """

    :param old_dqm:
    :param new_dqm:
    :return:
    """

    logger.info("Getting new items")
    new_items = new_dqm[~new_dqm.primaryId.isin(old_dqm.primaryId)]

    return new_items


def read_dqm_csv(file_name):
    """
    faster, for testing only

    :param file_name:
    :return:
    """
    logger.info(f"Reading file {file_name}")
    df = pd.read_csv(file_name)

    return df


def get_changed_items(old_dqm, new_dqm):
    """

    :param old_dqm:
    :param new_dqm:
    :return:
    """

    logger.info("Getting changed items")
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')
    pd.set_option('display.precision', 2)
    merged = old_dqm.merge(new_dqm, how='inner', left_on=["primaryId"], right_on=["primaryId"])

    differences = merged[merged.md5_x != merged.md5_y]

    return differences


def generate_output(new_items, changed_items, mod_output):
    """

    :param new_items:
    :param changed_items:
    :param mod_output:
    :return:
    """

    logger.info("Generating output")
    new_items.to_json(f"{mod_output}_new.json", orient='records')
    changed_items.to_json(f"{mod_output}_changed.json", orient='records')

    return "success"


@click.command()
@click.option("--old-version", "-O", "old_version", default=None, help="Old version of the file")
@click.option("--new-version", "-N", "new_version", default=None, help="New version of the file")
@click.option("--output", "-o", "output", is_flag=True, default=None, help="Generate output")
@click.option("--test", "-t", "test", is_flag=True, default=False, help="Test mode, reading csv files")
@click.option("--mtrace", "-m", "mtrace", is_flag=True, default=False, help="Memory trace")
def process_dqm_data(old_version, new_version, output, test, mtrace):
    """

    :param old_version:
    :param new_version:
    :param output:
    :return:
    """

    tracemalloc.start()
    if not test:
        sorted_new = sort_json(new_version)
        sorted_old = sort_json(old_version)
        new_dqm = read_dqm_file(sorted_new)
        old_dqm = read_dqm_file(sorted_old)
    else:
        new_dqm = read_dqm_csv(new_version)
        old_dqm = read_dqm_csv(old_version)

    new_items = get_new_items(old_dqm, new_dqm)
    changed_items = get_changed_items(old_dqm, new_dqm)
    if mtrace:
        print(f"Peak memory at {tracemalloc.get_traced_memory()[1]}")
    tracemalloc.stop()

    if output:
        mod_output = os.path.basename(old_version).split("_")[-1].replace(".json", "")
        logger.info(f"Generating output file for {mod_output}")
        generate_output(new_items, changed_items, mod_output)


if __name__ == "__main__":

    process_dqm_data()
