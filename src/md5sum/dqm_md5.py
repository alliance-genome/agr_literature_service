"""
dqm_md5.py - MD5 checksum calculation

Paulo Nuin Apr 2022

"""


import hashlib
import json
import logging

import click
import coloredlogs
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")


# MODS = ["SGD", "RGD", "FB", "WB", "MGI", "ZFIN"]
# DATATYPES = ["REFERENCE", "REF-EXCHANGE", "RESOURCE"]


def hash_df(df):
    """

    :param df:
    :return:
    """

    logger.info("Hashing dataframe")
    hashes = []
    for index, row in df.iterrows():
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

    print(new_items)


def read_dqm_csv(file_name):
    """
    faster for testing only

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
    logger.info("Getting changed items")
    merged = old_dqm.merge(new_dqm, how='inner', left_on=["primaryId"], right_on=["primaryId"])

    differences = merged[merged.md5_x != merged.md5_y]

    return differences


@click.command()
@click.option("--old-version", "-O", "old_version", default=None, help="Old version of the file")
@click.option("--new-version", "-N", "new_version", default=None, help="New version of the file")
@click.option("--output", "-o", "output", default=None, help="Output file")
@click.option("--test", "-t", "test", is_flag=True, default=False, help="Test mode, reading csv files")
def process_dqm_data(old_version, new_version, output, test):
    """

    :param old_version:
    :param new_version:
    :param output:
    :return:
    """

    if not test:
        sorted_new = sort_json(new_version)
        sorted_old = sort_json(old_version)
        new_dqm = read_dqm_file(sorted_new)
        old_dqm = read_dqm_file(sorted_old)
    else:
        new_dqm = read_dqm_csv(new_version)
        old_dqm = read_dqm_csv(old_version)

    get_new_items(old_dqm, new_dqm)
    get_changed_items(old_dqm, new_dqm)

    # new_dqm.to_csv("new_dqm.csv", index=False)
    # old_dqm.to_csv("old_dqm.csv", index=False)


if __name__ == "__main__":

    process_dqm_data()
