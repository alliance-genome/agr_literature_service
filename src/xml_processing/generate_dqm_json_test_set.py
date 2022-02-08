
import itertools
import json
import logging
import sys
from os import environ, makedirs, path

from dotenv import load_dotenv

from helper_file_processing import split_identifier

load_dotenv()


# pipenv run python generate_dqm_json_test_set.py
# Take large dqm json data and generate a smaller subset to test with
# This takes about 90 seconds to run


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def generate_dqm_json_test_set_from_sample_json():
    """
    generate dqm_sample/ files based on manually chosen entries in inputs/sample.json
    """

    base_path = environ.get('XML_PATH')
    sample_path = base_path + 'dqm_sample/'
    if not path.exists(sample_path):
        makedirs(sample_path)
    sample_file = base_path + 'inputs/sample.json'
    sample_json = dict()
    try:
        with open(sample_file, 'r') as f:
            sample_json = json.load(f)
            f.close()
    except IOError:
        logger.info("No sample.json file at %s", sample_file)
    if not sample_json:
        return
    if 'data' not in sample_json:
        logger.info("No 'data' in sample.json file at %s", sample_file)
        return
    pmids_wanted = set()
    mod_ids_wanted = dict()
    ids_wanted = set()
    for entry in sample_json['data']:
        if 'pmid' in entry:
            prefix, identifier, separator = split_identifier(entry['pmid'])
            pmids_wanted.add(identifier)
            ids_wanted.add(entry['pmid'])
        if 'modId' in entry:
            for mod_id in entry['modId']:
                prefix, identifier, separator = split_identifier(mod_id)
                if prefix not in mod_ids_wanted:
                    mod_ids_wanted[prefix] = set()
                mod_ids_wanted[prefix].add(identifier)
                ids_wanted.add(mod_id)
    for mod in mod_ids_wanted:
        logger.info("generating sample set for %s", mod)
        input_filename = base_path + 'dqm_data/REFERENCE_' + mod + '.json'
        logger.info("reading file %s", input_filename)
        dqm_data = dict()
        try:
            with open(input_filename, 'r') as f:
                dqm_data = json.load(f)
                f.close()
        except IOError:
            logger.info("No %s file at %s", mod, input_filename)
        if 'data' not in dqm_data:
            logger.info("No 'data' in %s file at %s", mod, input_filename)
            continue
        dqm_wanted = []
        for entry in dqm_data['data']:
            if 'primaryId' in entry and entry['primaryId'] in ids_wanted:
                dqm_wanted.append(entry)
                logger.info("Found primaryId %s in %s", entry['primaryId'], mod)
        dqm_data['data'] = dqm_wanted
        output_json_file = sample_path + 'REFERENCE_' + mod + '.json'
        with open(output_json_file, "w") as json_file:
            json_data = json.dumps(dqm_data, indent=4, sort_keys=True)
            json_file.write(json_data)
            json_file.close()


def generate_dqm_json_test_set_from_start_mid_end():
    """
    generate dqm_sample/ files based on sampling from beginning, middle, and end of dqm files.
    """

    # base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
    base_path = environ.get('XML_PATH')
    sample_path = base_path + 'dqm_sample/'
    if not path.exists(sample_path):
        makedirs(sample_path)
    sample_amount = 10
    mods = ['SGD', 'RGD', 'FB', 'WB', 'MGI', 'ZFIN']
    # mods = ['MGI']
    for mod in mods:
        logger.info("generating sample set for %s", mod)
        input_filename = base_path + 'dqm_data/REFERENCE_' + mod + '.json'
        logger.info("reading file %s", input_filename)
        f = open(input_filename)
        dqm_data = json.load(f)

        # generate half-as-big set
        # sample_amount = int(len(dqm_data['data']) / 2)
        # dqm_data['data'] = dqm_data['data'][:sample_amount]	# half one
        # dqm_data['data'] = dqm_data['data'][-sample_amount:]	# half two

        reference_amount = len(dqm_data['data'])
        if reference_amount > 3 * sample_amount:
            sample1 = dqm_data['data'][:sample_amount]
            start = int(reference_amount / 2) - 1
            sample2 = dqm_data['data'][start:start + sample_amount]
            sample3 = dqm_data['data'][-sample_amount:]
            dqm_data['data'] = list(itertools.chain(sample1, sample2, sample3))
        output_json_file = sample_path + 'REFERENCE_' + mod + '.json'
        with open(output_json_file, "w") as json_file:
            json_data = json.dumps(dqm_data, indent=4, sort_keys=True)
            json_file.write(json_data)
            json_file.close()


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("starting generate_dqm_json_test_set.py")
    generate_dqm_json_test_set_from_sample_json()
    # generate_dqm_json_test_set_from_start_mid_end()
    logger.info("ending generate_dqm_json_test_set.py")
