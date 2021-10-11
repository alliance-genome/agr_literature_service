
import json
from os import environ, path, makedirs
import logging
import logging.config

from dotenv import load_dotenv

load_dotenv()

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

# pipenv run python parse_dqm_json_resource.py

# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
base_path = environ.get('XML_PATH')
json_storage_path = base_path + 'sanitized_resource_json/'

# resource_fields = ['primaryId', 'nlm', 'title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
# resource_fields_from_pubmed = ['title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
resource_fields_not_in_pubmed = ['titleSynonyms', 'abbreviationSynonyms', 'isoAbbreviation', 'copyrightDate',
                                 'publisher', 'editorsOrAuthors', 'volumes', 'pages', 'abstractOrSummary']


def create_storage_path():
    """

    :return:
    """

    if not path.exists(json_storage_path):
        makedirs(json_storage_path)


def split_identifier(identifier, ignore_error=False):
    """

    Split Identifier.

    Does not throw exception anymore. Check return, if None returned, there was an error

    :param identifier:
    :param ignore_error:
    :return:
    """

    prefix = None
    identifier_processed = None
    separator = None

    if ':' in identifier:
        prefix, identifier_processed = identifier.split(':', 1)  # Split on the first occurrence
        separator = ':'
    elif '-' in identifier:
        prefix, identifier_processed = identifier.split('-', 1)  # Split on the first occurrence
        separator = '-'
    else:
        if not ignore_error:
            logger.critical('Identifier does not contain \':\' or \'-\' characters.')
            logger.critical('Splitting identifier is not possible.')
            logger.critical('Identifier: %s', identifier)
        prefix = identifier_processed = separator = None

    return prefix, identifier_processed, separator


def write_json(json_filename, dict_to_output):
    """

    :param json_filename:
    :param dict_to_output:
    :return:
    """

    with open(json_filename, "w") as json_file:
        logger.info("Generating JSON for %s", json_filename)
        json_data = json.dumps(dict_to_output, indent=4, sort_keys=True)
        json_file.write(json_data)
        json_file.close()


def load_fb_resource_to_nlm():
    """

    :return:
    """

    filename = base_path + 'FB_resourceAbbreviation_to_NLM.json'
    fb_to_nlm = dict()
    try:
        with open(filename, 'r') as f:
            fb_to_nlm = json.load(f)
    except IOError:
        pass
    return fb_to_nlm


def load_fb_resource(pubmed_by_nlm):
    """

    :param pubmed_by_nlm:
    :return:
    """

    filename = base_path + 'dqm_data/RESOURCE_FB.json'
    fb_to_nlm = load_fb_resource_to_nlm()
    try:
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            sanitized_data = []
            for entry in dqm_data['data']:
                nlm = ''
                if 'abbreviationSynonyms' in entry:
                    for abbreviation in entry['abbreviationSynonyms']:
                        if abbreviation in fb_to_nlm:
                            nlm = fb_to_nlm[abbreviation]
                if nlm != '':
                    if nlm in pubmed_by_nlm:
                        if 'crossReferences' in entry:
                            for cross_ref in entry['crossReferences']:
                                pubmed_by_nlm[nlm]['crossReferences'].append(cross_ref)
                        if 'primaryId' in entry:
                            cross_ref = dict()
                            cross_ref['id'] = entry['primaryId']
                            pubmed_by_nlm[nlm]['crossReferences'].append(cross_ref)
                        for field in resource_fields_not_in_pubmed:
                            if field in entry:
                                pubmed_by_nlm[nlm][field] = entry[field]
                else:
                    if 'primaryId' in entry:
                        cross_ref = dict()
                        cross_ref['id'] = entry['primaryId']
                        if 'crossReferences' in entry:
                            entry['crossReferences'].append(cross_ref)
                        else:
                            entry['crossReferences'] = [cross_ref]
                    sanitized_data.append(entry)
            dqm_data['data'] = sanitized_data
            json_filename = json_storage_path + 'RESOURCE_FB.json'
            write_json(json_filename, dqm_data)
    except IOError:
        pass
    return pubmed_by_nlm


def load_zfin_resource(pubmed_by_nlm):
    """

    :param pubmed_by_nlm:
    :return:
    """

    filename = base_path + 'dqm_data/RESOURCE_ZFIN.json'
    try:
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            sanitized_data = []
            for entry in dqm_data['data']:
                primary_id = entry['primaryId']
                if primary_id in pubmed_by_nlm:
                    if 'crossReferences' in entry:
                        for cross_ref in entry['crossReferences']:
                            pubmed_by_nlm[primary_id]['crossReferences'].append(cross_ref)
                else:
                    prefix, identifier, separator = split_identifier(primary_id)
                    if prefix == 'ZFIN':
                        sanitized_data.append(entry)
                    else:
                        logger.info("unexpected DQM ZFIN resource %s : %s", prefix, primary_id)
            dqm_data['data'] = sanitized_data
            json_filename = json_storage_path + 'RESOURCE_ZFIN.json'
            write_json(json_filename, dqm_data)
    except IOError:
        pass
    return pubmed_by_nlm


def save_pubmed_resource(pubmed_by_nlm):
    """

    :param pubmed_by_nlm:
    :return:
    """

    pubmed_data = dict()
    pubmed_data['data'] = []
    for nlm in pubmed_by_nlm:
        pubmed_data['data'].append(pubmed_by_nlm[nlm])
    json_filename = json_storage_path + 'RESOURCE_NLM.json'
    write_json(json_filename, pubmed_data)


def load_pubmed_resource():
    """

    :return:
    """

    filename = base_path + 'pubmed_resource_json/resource_pubmed_all.json'
    f = open(filename)
    resource_data = json.load(f)
    pubmed_by_nlm = dict()
    for entry in resource_data:
        # primary_id = entry['primaryId']
        nlm = entry['nlm']
        pubmed_by_nlm[nlm] = entry
    return pubmed_by_nlm


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("Starting parse_dqm_json_resource.py")

    create_storage_path()
    pubmed_by_nlm = load_pubmed_resource()
    pubmed_by_nlm = load_zfin_resource(pubmed_by_nlm)
    pubmed_by_nlm = load_fb_resource(pubmed_by_nlm)
    save_pubmed_resource(pubmed_by_nlm)

    logger.info("ending parse_dqm_json_resource.py")

# pipenv run python parse_dqm_json_resource.py
