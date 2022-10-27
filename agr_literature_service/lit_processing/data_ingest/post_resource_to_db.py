import argparse
import json
import logging
import traceback
from sqlalchemy.orm import Session

from os import environ
from typing import Dict, Tuple
from dotenv import load_dotenv

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ResourceModel, EditorModel
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.global_utils import get_next_resource_curie
from agr_literature_service.lit_processing.utils.resource_reference_utils import (
    get_agr_for_xref,
    agr_has_xref_of_prefix,
    add_xref,
    load_xref_data
)

load_dotenv()
init_tmp_dir()

# pipenv run python3 post_resource_to_api.py > log_post_resource_to_api

# resource_fields = ['primaryId', 'nlm', 'title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
# resource_fields_from_pubmed = ['title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']

resource_fields_not_in_pubmed = ['titleSynonyms', 'abbreviationSynonyms', 'isoAbbreviation',
                                 'copyrightDate', 'publisher', 'editorsOrAuthors',
                                 'volumes', 'pages', 'abstractOrSummary']

# keys that exist in data
# 2021-05-24 23:06:27,844 - literature logger - INFO - key publisher
# 2021-05-24 23:06:27,844 - literature logger - INFO - key isoAbbreviation
# 2021-05-24 23:06:27,844 - literature logger - INFO - key title
# 2021-05-24 23:06:27,844 - literature logger - INFO - key primaryId
# 2021-05-24 23:06:27,844 - literature logger - INFO - key medlineAbbreviation
# 2021-05-24 23:06:27,844 - literature logger - INFO - key onlineISSN
# 2021-05-24 23:06:27,844 - literature logger - INFO - key abbreviationSynonyms
# 2021-05-24 23:06:27,844 - literature logger - INFO - key volumes
# 2021-05-24 23:06:27,844 - literature logger - INFO - key crossReferences
# 2021-05-24 23:06:27,844 - literature logger - INFO - key editorsOrAuthors
# 2021-05-24 23:06:27,844 - literature logger - INFO - key nlm
# 2021-05-24 23:06:27,845 - literature logger - INFO - key pages
# 2021-05-24 23:06:27,845 - literature logger - INFO - key printISSN

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

base_path = environ.get('XML_PATH', "")

remap_editor_keys: Dict[str, str] = dict()                # global, set where used if empty
remap_cross_references_keys: Dict[str, str] = dict()      # global, set where used if empty
cross_references_keys_to_remove: Dict[str, str] = dict()  # global, set where used it empty

remap_keys: Dict[str, str] = dict()


def process_editors(db_session: Session, resource_id: int, editors: Dict) -> Tuple:
    global remap_editor_keys
    if not remap_editor_keys:
        remap_editor_keys['authorRank'] = 'order'
        remap_editor_keys['firstName'] = 'first_name'
        remap_editor_keys['lastName'] = 'last_name'

    editor_keys_to_remove = {'referenceId'}
    okay = True
    err_msg = ''
    try:
        for editor in editors:
            new_editor = dict()
            for subkey in editor:
                if subkey in remap_editor_keys:
                    new_editor[remap_editor_keys[subkey]] = editor[subkey]
                elif subkey not in editor_keys_to_remove:
                    new_editor[subkey] = editor[subkey]
            new_editor['resource_id'] = resource_id
            editor_obj = EditorModel(**new_editor)
            db_session.add(editor_obj)
    except Exception as e:
        err_msg = f"An error occurred when adding editors into database for '{editors}'. {e}\n"
        okay = False
    return okay, err_msg


def process_cross_references(db_session: Session, resource_id: int, agr: str, cross_references: Dict) -> Tuple:
    global remap_cross_references_keys
    if not remap_cross_references_keys:
        remap_cross_references_keys['id'] = 'curie'

    okay = True
    error_mess = ""
    for xref in cross_references:
        new_xref = dict()
        for subkey in xref:
            if subkey in remap_cross_references_keys:
                new_xref[remap_cross_references_keys[subkey]] = xref[subkey]
            elif subkey not in cross_references_keys_to_remove:
                new_xref[subkey] = xref[subkey]
        new_xref['resource_id'] = resource_id
        prefix, identifier, _ = split_identifier(new_xref['curie'])
        logger.info(f"Processing {prefix} {identifier}")
        xrefs_agr = get_agr_for_xref(prefix, identifier)
        if xrefs_agr:
            logger.info(f"{prefix} {identifier} ALREADY EXISTS?")
            # Just duplicated not an error as to same resource
            if xrefs_agr == agr:
                continue
            mess = f"CrossReference with curie = {new_xref['curie']} already exists with a different resource -> {xrefs_agr}"
            logger.error(mess)
            okay = False
            error_mess += mess
        # elif agr in ref_xref_valid and prefix in ref_xref_valid[agr]:  # Duplicate prefix
        elif agr_has_xref_of_prefix(agr, prefix):
            okay = False
            error_mess += f"Not allowed same prefix {prefix} multiple time for the same resource"
        else:
            # print("pre add xref")
            # dump_xrefs()
            add_xref(agr, new_xref)
            # print("post add xref")
            # dump_xrefs()
    if not okay:
        return okay, error_mess
    return okay, "Cross References processed successfully"


def remap_keys_get_new_entry(entry: Dict) -> Dict:
    global remap_keys
    if not remap_keys:
        remap_keys['isoAbbreviation'] = 'iso_abbreviation'
        remap_keys['medlineAbbreviation'] = 'medline_abbreviation'
        remap_keys['abbreviationSynonyms'] = 'abbreviation_synonyms'
        remap_keys['crossReferences'] = 'cross_references'
        remap_keys['editorsOrAuthors'] = 'editors'
        remap_keys['printISSN'] = 'print_issn'
        remap_keys['onlineISSN'] = 'online_issn'
    keys_to_remove = {'nlm', 'primaryId'}
    new_entry = dict()

    for key in entry:
        # logger.info("key found\t%s\t%s", key, entry[key])
        if key in remap_keys:
            new_entry[remap_keys[key]] = entry[key]
        elif key not in keys_to_remove:
            new_entry[key] = entry[key]
    return new_entry


def process_resource_entry(db_session: Session, entry: Dict) -> Tuple:
    """Process json and add to db.
    Adds resourses, cross references and editors.

    :param db_session: database session
    :param entry:      json format of dqm resource
    """
    primary_id = entry['primaryId']
    prefix, identifier, separator = split_identifier(primary_id)
    if get_agr_for_xref(prefix, identifier):
        return True, ""

    new_entry = remap_keys_get_new_entry(entry)
    try:
        resource_id = None
        curie = get_next_resource_curie(db_session)

        # cross_references and editors done seperately
        # so do not want to pass them to the resource creator
        # make a copy and deal with them after we have a resource_id
        # to attach too.
        cross_references = new_entry.get('cross_references', [])
        if "cross_references" in new_entry:
            del new_entry["cross_references"]
        editors = new_entry.get('editors', [])
        if "editors" in new_entry:
            del new_entry["editors"]

        new_entry['curie'] = curie
        if 'iso_abbreviation' in new_entry:
            logger.info("Adding resource into database for '" + new_entry['iso_abbreviation'] + "'")
        else:
            logger.info(" NOOO iso_abbreviation: Adding resource into database for '" + new_entry['curie'] + "'")
        x = ResourceModel(**new_entry)
        db_session.add(x)
        db_session.flush()
        # db_session.commit()
        db_session.refresh(x)
        resource_id = x.resource_id

        xref_okay, message = process_cross_references(db_session, resource_id, curie, cross_references)

        if not xref_okay:
            return xref_okay, message

        editor_okay, message = process_editors(db_session, resource_id, editors)

        if not editor_okay:
            return editor_okay, message

        db_session.commit()
        return True, f"{primary_id}\t{curie}\n"
    except Exception as e:
        traceback.print_exc()
        message = f"An error occurred when adding resource into database for '{entry}'. {e}\n"
        logger.info(message)
        db_session.rollback()
        return False, message


def post_resources(db_session: Session, input_path: str, input_mod: str, base_input_dir: str = base_path) -> None:      # noqa: C901
    """
    Parse the json file and load the data into the db after some remapping of keys etc.
    i.e.
    base_input_dir = "/user/bob/")
    json_path = "sanitized_resources/"
    post_resources(db, json_path, 'ZFIN', base_input_dir)

    reads the file
    /user/bob/sanitized_resource_json_updates/RESOURCE_ZFIN.json"

    If base_input_dir not defined environment value of XML_PATH used.

    :param input_path:
    :return:
    """

    json_storage_path = base_input_dir + input_path + '/'
    filesets = ['NLM', 'FB', 'ZFIN']
    if input_mod in filesets:
        filesets = [input_mod]

    resource_primary_id_to_curie_file = base_path + 'resource_primary_id_to_curie'
    errors_in_posting_resource_file = base_path + 'errors_in_posting_resource'

    load_xref_data(db_session, 'resource')

    with open(resource_primary_id_to_curie_file, 'a') as mapping_fh, open(errors_in_posting_resource_file, 'a') as error_fh:

        for fileset in filesets:
            logger.info("processing %s", fileset)

            filename = json_storage_path + 'RESOURCE_' + fileset + '.json'
            f = open(filename)
            resource_data = json.load(f)
            for entry in resource_data['data']:
                process_okay, message = process_resource_entry(db_session, entry)
                if process_okay:
                    if message:
                        mapping_fh.write(message)
                else:
                    error_fh.write(message)
        db_session.commit()
        db_session.close()


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', action='store', help='take input from RESOURCE files in full path')

    args = vars(parser.parse_args())

    logger.info("starting post_resource_to_db.py")

    if args['file']:
        db_session = create_postgres_session(False)
        post_resources(db_session, args['file'], 'all')
    else:
        logger.info("No flag passed in.  Use -h for help.")

    logger.info("ending post_resource_to_api.py")
