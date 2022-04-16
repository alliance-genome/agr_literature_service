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
import redis


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")


def hash_df(df):
    """

    function that hashes a dataframe and add a column with the hash

    TODO: can be optimized

    :param df: dataframe with data
    :return: dataframe with hash column
    """

    logger.info("Hashing dataframe")
    hashes = []
    for _index, row in df.iterrows():
        hashes.append(hashlib.md5(str(row).encode('utf-8')).hexdigest())
    df['md5'] = hashes

    return df


def get_new_items(old_dqm, new_dqm):
    """

    function that compares two md5sums and returns new items

    :param old_dqm: dataframe with old file md5sums
    :param new_dqm: dataframe with new file md5sums
    :return:
    """

    logger.info("Getting new items")
    new_items = new_dqm[~new_dqm.filename.isin(old_dqm.filename)]

    return new_items


def get_changed_items(old_dqm, new_dqm):
    """

    function that compare two md5sums and returns changed items

    :param old_dqm: dataframe with old file md5sums
    :param new_dqm: dataframe with new file md5sums
    :return: dataframe with changed items
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

    function that compares md5sums of files in two columns

    :param location: directory with files
    :param test: not implemented yet
    :param json: flag to determine if input is JSON files
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


def generate_output(new_items, changed_items):
    """

    function that generates output files
    new_items: new files
    changed_items: changed files

    only filename and md5sums are output, output is JSON for now
    can be extended to XML or CSV

    :param new_items:
    :param changed_items:
    :return:
    """

    new_items.to_json("new_items.json", orient="records")
    changed_items.to_json("changed_items.json", orient="records")


def save_to_redis(old_df, new_df):
    """

    :param old_df:
    :param new_df:
    :return:
    """

    logger.info("Saving to redis")
    r = redis.Redis(host='localhost', port=6379, db=1, password="password")
    for _idx, row in new_df.iterrows():
        r.set(row["filename"], row["md5_y"])
    r = redis.Redis(host='localhost', port=6379, db=0, password="password")
    for _idx, row in old_df.iterrows():
        r.set(row["filename"], row["md5_x"])


@click.command()
@click.option("--old-location", "-O", "old_location", default=None, help="Old version of the file")
@click.option("--new-location", "-N", "new_location", default=None, help="New version of the file")
@click.option("--json", "-j", "json", is_flag=True, help="Input is JSON files")
@click.option("--output", "-o", "output", is_flag=True, default=None, help="Generate output file")
@click.option("--test", "-t", "test", is_flag=True, default=False, help="Test mode, reading csv files")
@click.option("--redis", "-r", "redis_export", is_flag=True, default=False, help="Save md5 to redis")
def process_xml_data(old_location, new_location, output, test, json, redis_export):
    """

    :param old_location: directory with older version of the files
    :param new_location: directory with newer version of the files
    :param output: flag to determine if output file is generated
    :param test: not implemented yet
    :param json: flag to determine if input is JSON files
    :return:
    """

    logger.info("Processing XML data")
    old_md5sum = get_data(old_location, test, json)
    new_md5sum = get_data(new_location, test, json)

    old_df = pd.DataFrame(list(old_md5sum.items()), columns=['filename', 'md5_x'])
    new_df = pd.DataFrame(list(new_md5sum.items()), columns=['filename', 'md5_y'])

    new_items = get_new_items(old_df, new_df)
    chanmged_items = get_changed_items(old_df, new_df)

    if redis_export:
        save_to_redis(old_df, new_df)

    if output:
        logger.info("Generating output file")
        generate_output(new_items, chanmged_items)


if __name__ == "__main__":

    process_xml_data()
