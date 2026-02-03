"""
Functions to update and create resources.

sanitized json entry mentioned in the docs is the original dqm entry that has been
modified only wrt its primary_id and a cross reference added of the primary id if
it was not already in the cross references in the file.

NOTE:The script part of this has been removed as it is no longer used that way.
The functions now processes dicts that are modified from the original dqm format.
So no files are read anymore.
"""
import logging
import warnings
from typing import Dict, List, Tuple
from sqlalchemy.orm import Session

from dotenv import load_dotenv
from fastapi.encoders import jsonable_encoder

from agr_literature_service.api.models import ResourceModel
from sqlalchemy.orm.exc import NoResultFound
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.dqm_processing_utils import \
    compare_authors_or_editors
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.data_ingest.post_resource_to_db import \
    process_resource_entry
from agr_literature_service.lit_processing.utils.resource_reference_utils import (
    get_agr_for_xref,
    agr_has_xref_of_prefix,
    is_obsolete,
    add_xref,
    find_existing_resource
)
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()
init_tmp_dir()

remap_keys: Dict = {}
simple_fields: List = []
list_fields: List = []
resources_to_update: Dict = dict()

# Flags for the end processing result
PROCESSED_NEW = 0
PROCESSED_UPDATED = 1
PROCESSED_FAILED = 2
PROCESSED_NO_CHANGE = 3


logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../../logging.conf')
# logging.config.fileConfig(log_file_path)
# logger = logging.getLogger('literature logger')

batch_size_for_commit = 250


def process_single_resource(db_session: Session, resource_dict: Dict) -> Tuple:
    """
    Sorts out if the entry is new or an update and calls the appropriate
    function to create or update to the database.

    Uses comprehensive duplicate detection to check:
    1. primaryId cross-reference
    2. All cross-references in the entry
    3. ISSN values (print_issn, online_issn)

    :param db_session: db connection
    :param resource_dict: sanitized dqm json entry
    :return: Tuple of (stat, process_okay, message, field_changes, missing_prefix_xrefs, xref_conflicts)
             where field_changes is a list of dicts with 'agr', 'field', 'old_value', 'new_value'
    """
    primary_id = resource_dict['primaryId']
    logger.info("primary_id %s resource_dict %s", primary_id, resource_dict)

    # Use comprehensive duplicate detection
    existing = find_existing_resource(resource_dict)

    field_changes = []
    missing_prefix_xrefs = []
    xref_conflicts = []

    if existing:
        agr, resource_id, match_type = existing
        logger.info(f"Found existing resource {agr} via {match_type} match for {primary_id}")

        if agr in resources_to_update:
            message = f"ERROR agr {agr} has multiple values to update {primary_id} {resources_to_update[agr]['primaryId']}"
            stat = PROCESSED_FAILED
            process_okay = False
        else:
            # Update existing resource
            process_okay, message, actually_updated, changes, missing_prefix_xrefs, xref_conflicts = process_update_resource(db_session, resource_dict, agr)
            logger.info("update primary_id %s db %s (matched via %s)", primary_id, agr, match_type)
            # Add agr to each field change for logging
            for change in changes:
                change['agr'] = agr
                change['primary_id'] = primary_id
            field_changes = changes
            stat = PROCESSED_UPDATED if actually_updated else PROCESSED_NO_CHANGE
    else:
        # No existing resource found, create new
        process_okay, message = process_resource_entry(db_session, resource_dict)
        if process_okay:
            if message:
                logger.info(message)
            else:
                logger.error(message)
        stat = PROCESSED_NEW

    return stat, process_okay, message, field_changes, missing_prefix_xrefs, xref_conflicts


def update_resource(db_session: Session, dqm_entry: dict, db_entry: dict) -> Tuple[bool, List[Dict]]:
    """
    Update the resource database entry from the sanitized dqm entry.

    :param db_session: db connection
    :param dqm_entry: sanitized dqm entry in json format
    :param db_entry: db entry in json format.
    :return: Tuple of (actually_updated, field_changes) where field_changes is a list of
             dicts with 'field', 'old_value', 'new_value'
    """
    global simple_fields
    global list_fields
    global remap_keys
    if not simple_fields:
        simple_fields = ['title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN',
                         'onlineISSN', 'publisher', 'pages']
    if not list_fields:
        list_fields = ['abbreviationSynonyms', 'titleSynonyms', 'volumes']
    if not remap_keys:
        remap_keys['isoAbbreviation'] = 'iso_abbreviation'
        remap_keys['medlineAbbreviation'] = 'medline_abbreviation'
        remap_keys['printISSN'] = 'print_issn'
        remap_keys['onlineISSN'] = 'online_issn'
        remap_keys['abbreviationSynonyms'] = 'abbreviation_synonyms'
        remap_keys['titleSynonyms'] = 'title_synonyms'
        remap_keys['crossReferences'] = 'cross_references'
        remap_keys['editorsOrAuthors'] = 'editors'

    agr = db_entry['curie']
    update_json = dict()
    field_changes = []
    for field_camel in simple_fields:
        field_snake = camel_to_snake(field_camel, remap_keys)
        dqm_value = None
        db_value = None
        if field_camel in dqm_entry:
            dqm_value = dqm_entry[field_camel]
        if field_snake in db_entry:
            db_value = db_entry[field_snake]
        if dqm_value != db_value:
            logger.info(f"patch {agr} field {field_snake} from db {db_value} to dqm {dqm_value}")
            update_json[field_snake] = dqm_value
            field_changes.append({
                'field': field_snake,
                'old_value': db_value,
                'new_value': dqm_value
            })
    for field_camel in list_fields:
        list_changed = compare_list(db_entry, dqm_entry, field_camel, remap_keys)
        if list_changed[0]:
            logger.info(f"patch {agr} field {list_changed[3]} from db {list_changed[2]} to dqm {list_changed[1]}")
            update_json[list_changed[3]] = list_changed[1]
            field_changes.append({
                'field': list_changed[3],
                'old_value': list_changed[2],
                'new_value': list_changed[1]
            })
    if update_json:
        try:
            db_session.query(ResourceModel).filter_by(curie=agr).update(update_json)
            db_session.commit()
            logger.info("The resource row for curie = " + agr + " has been updated.")
            return True, field_changes
        except Exception as e:
            logger.error("An error occurred when updating resource row for curie = " + agr + " " + str(e))
            return False, field_changes
    return False, field_changes


def process_update_resource(db_session, dqm_entry, agr) -> Tuple:
    """
    Gets the db entry from the database and converts this to json.
    This is then used in the update_resource function to update the
    database. Its cross references and editors are also updated here.
    NOTE: Currently editors are not done. This should be addressed at some point.

    :param db_session: db connection
    :param dqm_entry: sanitized dqm entry in json format
    :param agr: curie to lookup the resource in the database
    :return: Tuple of (okay, error_message, actually_updated, field_changes,
             missing_prefix_xrefs, xref_conflicts)
    """
    try:
        db_entry = db_session.query(ResourceModel).filter(ResourceModel.curie == agr).one()
    except NoResultFound:
        return False, f"Unable to find unique resource with curie {agr}.", False, [], [], []
    db_entry = jsonable_encoder(db_entry)
    actually_updated, field_changes = update_resource(db_session, dqm_entry, db_entry)
    okay = True
    error_message = ""
    missing_prefix_xrefs = []
    xref_conflicts = []
    if 'crossReferences' in dqm_entry:
        okay, error_message, missing_prefix_xrefs, xref_conflicts = compare_xref(agr, db_entry['resource_id'], dqm_entry)

    editors_changed = compare_authors_or_editors(db_entry, dqm_entry, 'editors')
    # editor API needs updates.  reference_curie required to post reference authors but for some reason resource_curie not allowed here, cannot connect new editor to resource if resource_curie is not passed in
    if editors_changed[0]:
        pass
    #    # live_changes = True
    #    # e.g. FB:FBmultipub_7448
    #    for patch_data in editors_changed[1]:
    #        patch_dict = patch_data['patch_dict']
    #        # patch_dict['resource_curie'] = agr   # reference_curie required to patch reference authors but for some reason not allowed here
    #        logger.info("patch %s editor_id %s patch_dict %s", agr, patch_data['editor_id'], patch_dict)
    #        editor_patch_url = 'http://localhost:' + api_port + '/editor/' + str(patch_data['editor_id'])
    #        headers = generic_api_patch(live_changes, editor_patch_url, headers, patch_dict, str(patch_data['editor_id']), None, None)
    #    for create_dict in editors_changed[2]:
    #        create_dict['resource_curie'] = agr   # reference_curie required to post reference authors but for some reason not allowed here
    #        logger.info("add to %s create_dict %s", agr, create_dict)
    #        editor_post_url = 'http://localhost:' + api_port + '/editor/'
    #        headers = generic_api_post(live_changes, editor_post_url, headers, create_dict, agr, None, None)
    return okay, error_message, actually_updated, field_changes, missing_prefix_xrefs, xref_conflicts


def update_resources(db_session, resources_to_update):
    """
    Get the resource from the database, compare to the new resource data.
    Patch simple and list fields.  Add new cross_references and track other
    cases until curators tell us what reports they want.
    This takes 11 minutes to query 34284 resources one by one through the API

    :param  db_session:  db connection
    :param resources_to_update:
    :return:
    NOTE: Not used anymore the entrys are processed as they are recieved and not
          collected to do at the end. DELETE theis function once certain.
          exit(-1) added to check for this.
    """
    exit(-1)
    for agr in resources_to_update:
        process_update_resource(db_session, resources_to_update[agr], agr)


def camel_to_snake(field_camel, remap_keys):
    """

    :param field_camel:
    :param remap_keys:
    :return:
    """

    field_snake = field_camel
    if field_camel in remap_keys:
        field_snake = remap_keys[field_camel]
    return field_snake


def compare_xref(agr, resource_id, dqm_entry):
    """
    We're running dqm resource updates mod by mod instead of aggregating all their data into
    one entry and comparing that to the database.  Since we cannot track which mod submission
    an xref went into the database with, we cannot tell which ones should be removed.
    For example, if for a given resource FB sends an ISSN and ZFIN sends an ISBN, when running
    the ZFIN update it will see that the database has an ISSN that ZFIN doesn't have, so it
    will create notification about things that ZFIN doesn't necessarily care about.
    For that reason we're only doing of xrefs, and removals will have to be done at ABC
    through the UI.

    :param agr:
    :param resource_id
    :param dqm_entry:
    :return: Tuple of (okay, error_message, missing_prefix_xrefs, xref_conflicts)
    """

    okay = True
    error_mess = ""
    missing_prefix_xrefs = []
    xref_conflicts = []
    for xref in dqm_entry['crossReferences']:
        curie = xref['id']
        prefix, identifier, separator = split_identifier(curie)
        if prefix is None or identifier is None:
            missing_prefix_xrefs.append(curie)
            logger.warning(f"Cross-reference '{curie}' is missing a valid prefix:identifier format")
            continue
        agr_db_from_xref = get_agr_for_xref(prefix, identifier)
        if agr_db_from_xref == agr:
            # Okay just duplication of same data, so should be okay
            logger.info(f"Prefix found {prefix} for {identifier} and agr {agr_db_from_xref}")
        elif agr_has_xref_of_prefix(agr, prefix):
            # Skip - this resource already has an xref of this prefix type
            pass
        elif agr_db_from_xref:
            # Cross-reference already assigned to another resource - report conflict
            xref_conflicts.append(f"{curie} -> {agr_db_from_xref}")
            logger.warning(f"Cross-reference {curie} already assigned to {agr_db_from_xref}, cannot add to {agr}")
        else:
            if is_obsolete(agr, prefix, identifier):
                pass
            else:
                try:
                    logger.info("CREATE: add cross_reference %s to %s", curie, agr)
                    entry = {'curie': identifier,
                             'curie_prefix': prefix,
                             'resource_id': resource_id,
                             'pages': xref.get('pages', [])}
                    add_xref(agr, entry)
                    logger.info("The cross_reference row for curie = " + curie + " and resource_curie = " + agr + " has been added into database.")
                except Exception as e:
                    okay = False
                    mess = f"An error occurred when adding cross_reference row for curie = {curie} and resource_curie = {agr} Error:{e}"
                    logger.info(mess)
                    error_mess += mess
    return okay, error_mess, missing_prefix_xrefs, xref_conflicts


def compare_list(db_entry, dqm_entry, field_camel, remap_keys):
    """
    compare case-insensitive if two lists contain the same values from db and dqm dicts

    :param db_entry:
    :param dqm_entry:
    :param field_camel:
    :param remap_keys:
    :return:
    """

    field_snake = camel_to_snake(field_camel, remap_keys)
    db_values = []
    dqm_values = []
    if field_snake in db_entry:
        if db_entry[field_snake] is not None:
            db_values = db_entry[field_snake]
    lower_db_values = [i.lower() for i in db_values]
    if field_camel in dqm_entry:
        if dqm_entry[field_camel] is not None:
            dqm_values = dqm_entry[field_camel]
    lower_dqm_values = [i.lower() for i in dqm_values]
    if set(lower_db_values) == set(lower_dqm_values):
        return False, None, None
    else:
        return True, dqm_values, db_values, field_snake
