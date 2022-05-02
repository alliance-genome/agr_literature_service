
import json
import hashlib
import argparse
import sys
from os import environ, path, makedirs, listdir
import logging
import logging.config

from helper_file_processing import split_identifier, write_json
from helper_s3 import upload_file_to_s3, download_file_from_s3

from dotenv import load_dotenv

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()


# pipenv run python filter_dqm_md5sum.py -f dqm_data/ -m all
# pipenv run python filter_dqm_md5sum.py -f dqm_data/ -m WB
#
# 4.5 minutes to read 888418 papers in 2.7G of dqm_data/ and output 4.0G of dqm_json/
# 1 minute 22 seconds to generate md5sum for that set and upload to s3
# 2 seconds to load that data from s3

# To use this in a mod update, update sort_dqm_json_reference_updates.py to load
# the old md5sum from s3 (load_s3_md5data), generate the new md5sum from
# dqm_data/ (generate_new_md5), filter on differences to process entries, if all
# loads well, # update and append new md5sum to old md5sum and save to s3
# (save_s3_md5data).


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_json_storage_path(mod):
    """

    :param mod:
    :return:
    """

    base_path = environ.get('XML_PATH')
    mod_json_storage_path = base_path + 'dqm_json/' + mod + '/'
    if not path.exists(mod_json_storage_path):
        makedirs(mod_json_storage_path)
    return mod_json_storage_path


def generate_new_md5(input_path, mods):
    """

    :param input_path:
    :param mods:
    :return:
    """

    # if it's a pmid, ignore fields that come from pubmed, since they'll be updated from pubmed update
    pmid_fields = ['author', 'volume', 'title', 'pages', 'issueName', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher', 'meshTerms', 'plainLanguageAbstract', 'pubmedAbstractLanguages', 'publicationStatus']

    # these fields we never want from dqm updates.  keyword was one time, tags will be processed differently later
    remove_fields = ['dateLastModified', 'issueDate', 'citation', 'keywords', 'tags']

    base_path = environ.get('XML_PATH')
    json_storage_path = base_path + 'dqm_json/'
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)

    md5dict = {}
    counter = 0
    for mod in mods:
        md5dict[mod] = {}

        filename = base_path + input_path + '/REFERENCE_' + mod + '.json'
        logger.info("Loading %s data from %s", mod, filename)
        dqm_data = dict()
        try:
            with open(filename, 'r') as f:
                dqm_data = json.load(f)
                f.close()
        except IOError:
            logger.info("No reference data to update from MOD %s", mod)
        if not dqm_data:
            continue

        for entry in dqm_data['data']:
            counter += 1
            # if counter > 10:
            #     break
            primary_id = entry['primaryId']
            # logger.info("counting %s %s %s", counter, mod, primary_id)

            is_pmid = False
            if 'crossReference' in entry:
                for xref in entry['crossReference']:
                    if 'id' in xref:
                        prefix, identifier, separator = split_identifier(xref['id'])
                        if prefix == 'PMID':
                            is_pmid = True
            prefix, identifier, separator = split_identifier(entry['primaryId'])
            if prefix == 'PMID':
                is_pmid = True
            if is_pmid is True:
                for pmid_field in pmid_fields:
                    if pmid_field in entry:
                        del entry[pmid_field]
            for ignore_field in remove_fields:
                if ignore_field in entry:
                    del entry[ignore_field]

            # Write the json data to output json file to debug
            # json_data = json.dumps(entry, indent=4, sort_keys=True)
            # mod_json_storage_path = get_json_storage_path(mod)
            # json_filename = mod_json_storage_path + primary_id + '.json'
            # with open(json_filename, "w") as json_file:
            #     json_file.write(json_data)
            #     json_file.close()
            # md5sum = hashlib.md5(json_data.encode('utf-8')).hexdigest()

            md5sum = generate_md5sum_from_dict(entry)
            md5dict[mod][primary_id] = md5sum

    logger.info(f"processed {counter} entries")
    return md5dict


def save_s3_md5data(md5dict, mods):
    """

    :param md5dict:
    :param mods:
    :return:
    """

    env_state = environ.get('ENV_STATE', 'develop')
    if env_state == 'build':
        env_state = 'develop'
    if env_state != 'test':
        bucketname = 'agr-literature'
        for mod in mods:
            mod_json_storage_path = get_json_storage_path(mod)
            md5file = mod_json_storage_path + 'md5sum'
            write_json(md5file, md5dict[mod])
            s3_file_location = env_state + '/reference/metadata/md5sum/' + mod + '_md5sum'
            upload_file_to_s3(md5file, bucketname, s3_file_location)


def load_s3_md5data(mods):
    """

    :param mods:
    :return:
    """

    md5dict = {}
    env_state = environ.get('ENV_STATE', 'develop')
    if env_state == 'build':
        env_state = 'develop'
    bucketname = 'agr-literature'
    for mod in mods:
        if env_state == 'test':
            md5dict[mod] = {}
        else:
            s3_file_location = env_state + '/reference/metadata/md5sum/' + mod + '_md5sum'
            mod_json_storage_path = get_json_storage_path(mod)
            md5file = mod_json_storage_path + 'md5sum'
            download_file_from_s3(md5file, bucketname, s3_file_location)
            try:
                with open(md5file, 'r') as f:
                    md5dict[mod] = json.load(f)
                    f.close()
            except IOError:
                logger.info(f"No md5sum data to update from s3 {s3_file_location}")
    # debug
    # json_data = json.dumps(md5dict, indent=4, sort_keys=True)
    # print(json_data)
    return md5dict


def pubmed_json_generate_md5sum_and_save():
    """

    One time only to generate md5sum of PMID .json that were generate from
    xml_to_json from the 2021 11 04 run.  Future entries should be added by
    updating process_single_pmid.py query_pubmed_mod_updates.py and future
    code to do pubmed xml updates.

    :return:
    """

    base_path = environ.get('XML_PATH')
    json_storage_path = base_path + 'pubmed_json/'
    # json_storage_path = base_path + 'pubmed_sample/'
    dir_list = listdir(json_storage_path)
    md5dict = {}
    md5dict['PMID'] = {}
    for filename in dir_list:
        if filename.endswith(".json"):
            json_dict = dict()
            pmid = filename.replace(".json", "")
            filepath = json_storage_path + filename
            try:
                with open(filepath, 'r') as f:
                    json_dict = json.load(f)
                    f.close()
            except IOError:
                logger.info(f"No json data to update from PMID {filename}")
            # json_data = json.dumps(json_dict, indent=4, sort_keys=True)
            # md5sum = hashlib.md5(json_data.encode('utf-8')).hexdigest()
            md5sum = generate_md5sum_from_dict(json_dict)
            md5dict['PMID'][pmid] = md5sum
    save_s3_md5data(md5dict, ['PMID'])


def generate_md5sum_from_dict(json_dict):
    """

    Standard way to generate json format and md5sum

    :param json_dict:
    :return:
    """

    json_data = json.dumps(json_dict, indent=4, sort_keys=True)
    md5sum = hashlib.md5(json_data.encode('utf-8')).hexdigest()
    return md5sum


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mod', action='store', help='which mod, use all for all')
    parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')

    args = vars(parser.parse_args())

    logger.info("start filter_dqm_md5sum")

    # todo: use sqlalchemy to query list of mod abbreviations (maybe when we onboard XB)
    all_mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']
    mods = all_mods
    if args['mod']:
        if args['mod'] in all_mods:
            mods = [args['mod']]

    folder = 'dqm_data/'
    if args['file']:
        folder = args['file']

    # to generate md5sum data from dqm files
    md5dict = generate_new_md5(folder, mods)

    # to save md5sum data into s3
    save_s3_md5data(md5dict, mods)

    # to load md5sum data from s3
    # md5dict = load_s3_md5data(mods)

    # one time pubmed json generation
    # pubmed_json_generate_md5sum_and_save()

    logger.info("end filter_dqm_md5sum")
