import argparse
import json
import logging.config

from sqlalchemy.orm import Session
import sqlalchemy

import sys
from os import environ
from typing import Dict, Tuple
from dotenv import load_dotenv

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session, \
    sqlalchemy_load_ref_xref
from agr_literature_service.api.models import ResourceModel, CrossReferenceModel, EditorModel
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.global_utils import get_next_resource_curie

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


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

base_path = environ.get('XML_PATH', "")

remap_editor_keys: Dict[str, str] = dict()                # global, set where used if empty
remap_cross_references_keys: Dict[str, str] = dict()      # global, set where used if empty
cross_references_keys_to_remove: Dict[str, str] = dict()  # global, set where used it empty

remap_keys: Dict[str, str] = dict()


def process_editors(db_session: Session, resource_id: int, editors: Dict) -> None:
    global remap_editor_keys
    if not remap_editor_keys:
        remap_editor_keys['authorRank'] = 'order'
        remap_editor_keys['firstName'] = 'first_name'
        remap_editor_keys['lastName'] = 'last_name'

    editor_keys_to_remove = {'referenceId'}
    for editor in editors:
        new_editor = dict()
        for subkey in editor:
            if subkey in remap_editor_keys:
                new_editor[remap_editor_keys[subkey]] = editor[subkey]
            elif subkey not in editor_keys_to_remove:
                new_editor[subkey] = editor[subkey]
            if new_editor.get('orcid') and new_editor['orcid']:
                cross_reference_obj = db_session.query(
                    CrossReferenceModel).filter_by(
                        curie=new_editor['orcid']).first()
                if not cross_reference_obj:
                    cross_reference_obj = CrossReferenceModel(curie=new_editor['orcid'])
                    db_session.add(cross_reference_obj)
        new_editor['resource_id'] = resource_id
        editor_obj = EditorModel(**new_editor)
        db_session.add(editor_obj)


def process_cross_references(db_session: Session, resource_id: int, agr: str, cross_references: Dict, ref_xref_valid: Dict) -> Tuple:
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
        xref = db_session.query(CrossReferenceModel).filter_by(curie=new_xref['curie']).first()
        prefix, identifier, separator = split_identifier(new_xref['curie'])
        if xref:
            # Just duplicated not an error as to same resource
            if xref.resource_id == resource_id:
                continue
            mess = f"CrossReference with curie = {new_xref['curie']} already exists with a different resource -> {xref.resource}"
            logger.error(mess)
            # db_session.rollback()  # But how far back are we rolling back?
            okay = False
            error_mess += mess
        elif agr in ref_xref_valid and prefix in ref_xref_valid[agr]:  # Duplicate prefix
            okay = False
            error_mess += f"Not allowed same prefix {prefix} multiple time for the same resource"
        else:
            cr = CrossReferenceModel(**new_xref)
            db_session.add(cr)
            logger.info("Adding resource info into cross_reference table for " + new_xref['curie'])
            if agr not in ref_xref_valid:
                ref_xref_valid[agr] = {}
                if prefix:
                    ref_xref_valid[agr][prefix] = agr
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


def process_resource_entry(db_session: Session, entry: Dict, xref_ref: Dict, ref_xref_valid: Dict) -> Tuple:
    """Process json and add to db.
    Adds resourses, cross references and editors.

    :param db_session: database session
    :param entry:      json format of dqm resource
    :param xref_ref:   in the format xref_ref[prefix][identifier] = agr
                       Used to ensure xrefs only connect to 1 resource.
                       NOTE: Does not seem to get updated as resources are added.
    :param ref_xref_valid: in the format ref_xref[agr][prefix]
    """
    primary_id = entry['primaryId']
    prefix, identifier, separator = split_identifier(primary_id)
    if prefix in xref_ref:
        if identifier in xref_ref[prefix]:
            logger.info("%s\talready in", primary_id)
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
        logger.info("Adding resource into database for '" + new_entry['iso_abbreviation'] + "'")
        x = ResourceModel(**new_entry)
        db_session.add(x)
        db_session.flush()
        db_session.commit()
        db_session.refresh(x)
        resource_id = x.resource_id

        xref_okay, message = process_cross_references(db_session, resource_id, curie, cross_references, ref_xref_valid)

        # Still process editors if xrefs fail?
        process_editors(db_session, resource_id, editors)

        if not xref_okay:
            return xref_okay, message
        return True, f"{primary_id}\t{curie}\n"
    except Exception as e:
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

    If base_input_dir not defined environment value of XML_BASE used.

    :param input_path:
    :return:
    """

    json_storage_path = base_input_dir + input_path + '/'
    filesets = ['NLM', 'FB', 'ZFIN']
    if input_mod in filesets:
        filesets = [input_mod]

    resource_primary_id_to_curie_file = base_path + 'resource_primary_id_to_curie'
    errors_in_posting_resource_file = base_path + 'errors_in_posting_resource'

    # generate_cross_references_file('resource')
    # this updates from resources in the database, and takes 4 seconds.
    # if updating this script, comment it out after running it once
    # xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref_api_flatfile('resource')
    xref_ref, ref_xref_valid, ref_xref_obsolete = sqlalchemy_load_ref_xref('resource')

    # populating already_processed_primary_id from file generated by this script
    # to log created agr resource curies and identifiers, obsoleted by xref_ref
    # already_processed_primary_id = set()
    # if path.isfile(resource_primary_id_to_curie_file):
    #     with open(resource_primary_id_to_curie_file, 'r') as read_fh:
    #         for line in read_fh:
    #             line_data = line.split("\t")
    #             if line_data[0]:
    #                 already_processed_primary_id.add(line_data[0].rstrip())

    with open(resource_primary_id_to_curie_file, 'a') as mapping_fh, open(errors_in_posting_resource_file, 'a') as error_fh:

        for fileset in filesets:
            logger.info("processing %s", fileset)

            filename = json_storage_path + 'RESOURCE_' + fileset + '.json'
            f = open(filename)
            resource_data = json.load(f)
            for entry in resource_data['data']:
                process_okay, message = process_resource_entry(db_session, entry, xref_ref, ref_xref_valid)
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

    logger.info("starting post_resource_to_api.py")

    if args['file']:
        db_session = create_postgres_session(False)
        post_resources(db_session, args['file'], 'all')
    else:
        logger.info("No flag passed in.  Use -h for help.")

    logger.info("ending post_resource_to_api.py")

# pipenv run python3 post_resource_to_api.py
