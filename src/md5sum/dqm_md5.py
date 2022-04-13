"""
dqm_md5.py - MD5 checksum calculation for DQM JSON files

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

    function that hashes the dataframe and adds a md5 column to the it

    TODO: might be able to be optimized by using some lambda function on the DF

    :param df: dataframe to be hashed
    :return: dataframe with md5 column
    """

    logger.info("Hashing dataframe")
    hashes = []
    for _index, row in df.iterrows():
        hashes.append(hashlib.md5(str(row).encode('utf-8')).hexdigest())
    df['md5'] = hashes

    return df


def read_dqm_file(file_name):
    """

    function thar reads the DQM file
    pandas has issues with JSON with mixed contents so we need to normalize the JSON
    Main element on the DQM JSON is the [data] element, and this is used as the main
    source of information, and all other elements become rows in the dataframe

    :param file_name: DQM file name
    :return: datafrane with DQM contents
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

    function that compares the dataframes with old and new DQM files and
    outputs the unique items in the new DQM file

    :param old_dqm: dataframe with old DQM file, sorted
    :param new_dqm: dataframe with new DQM file, sorted
    :return: dataframe with new items, only columns from new DQM file
    """

    logger.info("Getting new items")
    new_items = new_dqm[~new_dqm.primaryId.isin(old_dqm.primaryId)]

    return new_items


def read_dqm_csv(file_name):
    """

    functions that reads a sorted JSON file dumped as CSV for quicker processing

    TODO: function that reads a sorted JSON file dumps as CSV

    :param file_name: DQM file name
    :return: datagrame with DQM contents
    """
    logger.info(f"Reading file {file_name}")
    df = pd.read_csv(file_name)

    return df


def get_changed_items(old_dqm, new_dqm):
    """

    function that compares the dataframes with old and new DQM files for
    different md5sums

    :param old_dqm: dataframe with old DQM file, sorted
    :param new_dqm: dataframe with new DQM file, sorted
    :return: datadframe with changed items all columns from both files
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

    Function that generates the output files
    {MOD}_new.json for new items
    {MOD}_changed.json for changed items
    {MOD}_changed.csv for changed items in CSV format (only primaryId and md5s are output)

    :param new_items: dataframe with new items
    :param changed_items: dataframe with changed items
    :param mod_output: output MOD abbreviation
    :return: success
    """

    logger.info("Generating output")
    new_items.to_json(f"{mod_output}_new.json", orient="records")
    changed_items.to_json(f"{mod_output}_changed.json", orient="records")

    md5sum_only = changed_items[["primaryId", "md5_x", "md5_y"]]
    md5sum_only.to_csv(f"{mod_output}_changed.csv", index=False)

    return "success"


@click.command()
@click.option("--old-version", "-O", "old_version", default=None, help="Old version of the file")
@click.option("--new-version", "-N", "new_version", default=None, help="New version of the file")
@click.option("--output", "-o", "output", is_flag=True, default=None, help="Generate output")
@click.option("--test", "-t", "test", is_flag=True, default=False, help="Test mode, reading csv files")
@click.option("--mtrace", "-m", "mtrace", is_flag=True, default=False, help="Memory trace")
def process_dqm_data(old_version, new_version, output, test, mtrace):
    """

    Main function of the script

    :param old_version: old version of the file
    :param new_version: new version of the file
    :param output: Flag if output will be generated or not
    :param test: Flag for quicj testing, JSON files needs to be sorted and saved as CSV
    :param mtrace: Flag for memory trace, only if interested in memory usage
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
