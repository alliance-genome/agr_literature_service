import json
import logging
import os
from os import environ

from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

init_tmp_dir()

base_path = environ.get('XML_PATH')

logger = logging.getLogger(__name__)


def load_pubmed_resource_basic():
    """

    :return:
    """

    filename = base_path + 'pubmed_resource_json/resource_pubmed_all.json'
    f = open(filename)
    resource_data = json.load(f)
    pubmed_by_nlm = dict()
    nlm_by_issn = dict()
    for entry in resource_data:
        # primary_id = entry['primaryId']
        nlm = entry['nlm']
        pubmed_by_nlm[nlm] = entry
        if 'printISSN' in entry:
            pissn = entry['printISSN']
            if pissn in nlm_by_issn:
                if nlm not in nlm_by_issn[pissn]:
                    nlm_by_issn[pissn].append(nlm)
            else:
                nlm_by_issn[pissn] = [nlm]
        if 'onlineISSN' in entry:
            oissn = entry['onlineISSN']
            if oissn in nlm_by_issn:
                if nlm not in nlm_by_issn[oissn]:
                    nlm_by_issn[oissn].append(nlm)
            else:
                nlm_by_issn[oissn] = [nlm]
    return pubmed_by_nlm, nlm_by_issn


def save_resource_file(json_storage_path, pubmed_by_nlm, datatype):
    """

    :param json_storage_path:
    :param pubmed_by_nlm:
    :return:
    """

    pubmed_data = dict()
    pubmed_data['data'] = []
    for nlm in pubmed_by_nlm:
        pubmed_data['data'].append(pubmed_by_nlm[nlm])
    json_filename = json_storage_path + 'RESOURCE_' + datatype + '.json'
    write_json(json_filename, pubmed_data)


def write_json(json_filename, dict_to_output):
    """

    :param json_filename:
    :param dict_to_output:
    :return:
    """

    with open(json_filename, "w") as json_file:
        # not sure how to logger from imported function without breaking logger in main function
        # logger.info("Generating JSON for %s", json_filename)
        json_data = json.dumps(dict_to_output, indent=4, sort_keys=True)
        json_file.write(json_data)
        json_file.close()


def chunks(list, size):
    for i in range(0, len(list), size):
        yield list[i:i + size]