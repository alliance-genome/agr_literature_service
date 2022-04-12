"""
dqm_md5.py - MD5 checksum calculation

Paulo Nuin Apr 2022

"""


import hashlib
import logging

import click
import coloredlogs
import pandas as pd
import os
import glob


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


def get_new_items(old_dqm, new_dqm):
    """

    :param old_dqm:
    :param new_dqm:
    :return:
    """

    logger.info("Getting new items")
    new_items = new_dqm[~new_dqm.filename.isin(old_dqm.filename)]

    return new_items


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
    merged = old_dqm.merge(new_dqm, how='inner', left_on=["filename"], right_on=["filename"])

    differences = merged[merged.md5_x != merged.md5_y]

    return differences


def get_data(location, test=False, json=False):
    """

    :param location:
    :param test:
    :return:
    """

    if json:
        extension = ".json"
    else:
        extension = ".xml"

    md5sum = {}
    logger.info(f"Getting data from {location}")
    for ff in glob.glob(f"{location}/*{extension}"):
        # logger.info(f"Reading file: {ff}")
        md5sum[os.path.basename(ff)] = hashlib.md5(open(ff, 'rb').read()).hexdigest()

    return md5sum


@click.command()
@click.option("--old-location", "-O", "old_location", default=None, help="Old version of the file")
@click.option("--new-location", "-N", "new_location", default=None, help="New version of the file")
@click.option("--json", "-j", "json", is_flag=True, help="Input is JSON files")
@click.option("--output", "-o", "output", default=None, help="Output file")
@click.option("--test", "-t", "test", is_flag=True, default=False, help="Test mode, reading csv files")
def process_xml_data(old_location, new_location, output, test, json):
    """

    :param old_version:
    :param new_version:
    :param output:
    :return:
    """

    logger.info("Processing XML data")
    old_md5sum = get_data(old_location, test, json)
    new_md5sum = get_data(new_location, test, json)

    old_df = pd.DataFrame(list(old_md5sum.items()), columns=['filename', 'md5_x'])
    new_df = pd.DataFrame(list(new_md5sum.items()), columns=['filename', 'md5_y'])

    new_items = get_new_items(old_df, new_df)
    compare_md5sum = get_changed_items(old_df, new_df)
    print(new_items)
    print(compare_md5sum)


if __name__ == "__main__":

    process_xml_data()
