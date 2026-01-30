"""
Helper script to keep track of tracking dicts and to keep them updated.
Also provide helper methods that use these and make the code simpler by
using these as they will be more explainatory.


Format of these Dicts
So if we have something like ZFIN:123456 then the
prefix is ZFIN and the identifier is 123456.
agr is the curie i.e. AGRKB:102000000000002

xref_ref[prefix][identifier] = agr

ref_xref_valid[agr][prefix] = identifier

ref_xref_obsolete[agr][prefix] = set()
ref_xref_obsolete[agr][prefix].add(identifier.lower())

So ref_xref_valid contains the values where the cross reference is
NOT obsolete.

issn_to_resource[issn] = agr
    Maps ISSN values to resource curies for duplicate detection.

It is my hope that the developer does not need to worry about all that goes on here
but can just call the methods and everything will be taken care off.
"""
import sys
from typing import Dict, Optional, Tuple, Union
import logging.config
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ResourceModel, ReferenceModel, CrossReferenceModel
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier

xref_ref: Dict = {}
ref_xref_valid: Dict = {}
ref_xref_obsolete: Dict = {}
issn_to_resource: Dict = {}
title_to_resource: Dict = {}
datatype: str = ""
db_session: Session = create_postgres_session(False)

logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def update_xref_dicts(agr: str, prefix: str, identifier: str, is_obsolete: bool = False) -> None:
    """
    Update the three dicts.
    """
    # prefix, identifier, _ = split_identifier(xref, True)
    if is_obsolete is False:
        if agr not in ref_xref_valid:
            ref_xref_valid[agr] = dict()
        ref_xref_valid[agr][prefix] = identifier
        if prefix not in xref_ref:
            xref_ref[prefix] = dict()
        if identifier not in xref_ref[prefix]:
            xref_ref[prefix][identifier] = agr
    else:
        if agr not in ref_xref_obsolete:
            ref_xref_obsolete[agr] = dict()
        # a reference and prefix can still have multiple obsolete values
        if prefix not in ref_xref_obsolete[agr]:
            ref_xref_obsolete[agr][prefix] = set()
        if identifier not in ref_xref_obsolete[agr][prefix]:
            ref_xref_obsolete[agr][prefix].add(identifier.lower())


def reset_xref():
    xref_ref.clear()
    ref_xref_valid.clear()
    ref_xref_obsolete.clear()
    issn_to_resource.clear()
    title_to_resource.clear()


def load_xref_dicts() -> None:
    """
    Search the database and load the dicts.
    """
    query = None
    print("load_xref_dicts")
    if datatype == 'reference':
        # 14 seconds to load all xref through sqlalchemy
        query = db_session.query(
            ReferenceModel.curie,
            CrossReferenceModel.curie_prefix,
            CrossReferenceModel.curie,
            CrossReferenceModel.is_obsolete
        ).join(
            ReferenceModel.cross_reference
        ).filter(
            CrossReferenceModel.reference_id.isnot(None)
        )

    elif datatype == 'resource':
        print("Loading resource cross reference db data.")
        query = db_session.query(
            ResourceModel.curie,
            CrossReferenceModel.curie_prefix,
            CrossReferenceModel.curie,
            CrossReferenceModel.is_obsolete
        ).join(
            ResourceModel.cross_reference
        ).filter(
            CrossReferenceModel.resource_id.isnot(None)
        )

    if query is not None:
        results = query.all()

        for result in results:
            print(result)
            update_xref_dicts(result[0], result[1], result[2], result[3])


def load_issn_to_resource_dict() -> None:
    """
    Load ISSN-to-resource mapping from the database.
    This maps both print_issn and online_issn to resource curies.
    """
    global issn_to_resource

    if datatype != 'resource':
        return

    print("Loading ISSN-to-resource mapping from database.")
    query = db_session.query(
        ResourceModel.curie,
        ResourceModel.resource_id,
        ResourceModel.print_issn,
        ResourceModel.online_issn
    )

    results = query.all()

    for result in results:
        curie, resource_id, print_issn, online_issn = result
        if print_issn and print_issn.strip():
            issn_key = print_issn.strip()
            if issn_key not in issn_to_resource:
                issn_to_resource[issn_key] = {'curie': curie, 'resource_id': resource_id}
        if online_issn and online_issn.strip():
            issn_key = online_issn.strip()
            if issn_key not in issn_to_resource:
                issn_to_resource[issn_key] = {'curie': curie, 'resource_id': resource_id}

    print(f"Loaded {len(issn_to_resource)} ISSN mappings.")


def load_title_to_resource_dict() -> None:
    """
    Load title-to-resource mapping from the database.
    This maps normalized titles to resource curies for duplicate detection.
    Title matching is used as a fallback when other methods fail.
    """
    global title_to_resource

    if datatype != 'resource':
        return

    print("Loading title-to-resource mapping from database.")
    query = db_session.query(
        ResourceModel.curie,
        ResourceModel.resource_id,
        ResourceModel.title
    )

    results = query.all()

    for result in results:
        curie, resource_id, title = result
        if title and title.strip():
            # Normalize title: lowercase and strip whitespace
            title_key = title.strip().lower()
            if title_key not in title_to_resource:
                title_to_resource[title_key] = {'curie': curie, 'resource_id': resource_id}

    print(f"Loaded {len(title_to_resource)} title mappings.")


def load_xref_data(db_session_set: Session, load_datatype: str) -> None:
    """
    Load the 3 dicts with data from the database.
    Store the db_session and the datatype so they do not
    have to be passed around all the time.
    """
    global db_session
    global datatype

    print(f"lxd db:{db_session} datatype: {load_datatype}")
    db_session = db_session_set
    allowed_types = ["resource", "reference"]
    datatype = load_datatype
    if datatype not in allowed_types:
        mess = "datatype must be one of {allowed_types}"
        logger.error(mess)
        raise KeyError(mess)

    load_xref_dicts()

    # Also load ISSN and title mappings for resource duplicate detection
    if datatype == 'resource':
        load_issn_to_resource_dict()
        load_title_to_resource_dict()


def dump_xrefs():
    print(f"xref_ref = {xref_ref}")
    print(f"ref_xref_valid = {ref_xref_valid}")


def agr_has_xref_of_prefix(agr: str, prefix: str) -> bool:
    """
    Return if the agr curie already has an xref of this type.
    """
    if agr in ref_xref_valid and prefix in ref_xref_valid[agr]:
        return True
    return False


def get_agr_for_xref(prefix: str, identifier: str) -> Union[str, None]:
    """
    Return agr curie if the xref defined by prefix and identifier
    is already assigned to an agr.

    Return: agr curie or None if not found.
    """
    if prefix in xref_ref and identifier in xref_ref[prefix]:
        return xref_ref[prefix][identifier]
    return None


def create_entity(db_session, entry) -> Union[ResourceModel, ReferenceModel]:
    """
    Create the entity given by the json entry.
    NOTE: Not sure about this one, does not bring much to the table
    """
    if datatype == "resource":
        x = ResourceModel(**entry)
    else:
        x = ReferenceModel(**entry)
    db_session.add(x)
    db_session.flush()
    db_session.commit()
    db_session.refresh(x)
    return x


def add_xref(agr: str, new_xref: Dict) -> None:
    """
    Create xref and update the 3 dicts.
    NOTE: new_xref['resource_id'] is used to link to resource
    """

    crossRefs = db_session.query(CrossReferenceModel).filter_by(curie=new_xref['curie']).all()
    if len(crossRefs) > 0:
        return

    try:
        cr = CrossReferenceModel(**new_xref)
        db_session.add(cr)
        db_session.commit()
        logger.info("Adding resource info into cross_reference table for " + new_xref['curie'])
        update_xref_dicts(agr, new_xref['curie_prefix'], new_xref['curie'])
    except Exception as e:
        logger.error(e)


def is_obsolete(agr: str, prefix: str, identifier: str) -> bool:
    if agr in ref_xref_obsolete:
        if prefix in ref_xref_obsolete[agr]:
            if identifier.lower() in ref_xref_obsolete[agr][prefix]:
                return True
    return False


def get_resource_by_issn(issn: str) -> Optional[Dict]:
    """
    Look up a resource by ISSN value.

    :param issn: ISSN value (without prefix)
    :return: Dict with 'curie' and 'resource_id' if found, None otherwise.
    """
    if issn and issn.strip():
        issn_key = issn.strip()
        if issn_key in issn_to_resource:
            return issn_to_resource[issn_key]
    return None


def update_issn_mapping(curie: str, resource_id: int, print_issn: str, online_issn: str) -> None:
    """
    Update the ISSN-to-resource mapping when a new resource is created.

    :param curie: Resource CURIE (e.g., AGRKB:102000000000001)
    :param resource_id: Database resource_id
    :param print_issn: Print ISSN value (can be None)
    :param online_issn: Online ISSN value (can be None)
    """
    global issn_to_resource

    if print_issn and print_issn.strip():
        issn_key = print_issn.strip()
        if issn_key not in issn_to_resource:
            issn_to_resource[issn_key] = {'curie': curie, 'resource_id': resource_id}

    if online_issn and online_issn.strip():
        issn_key = online_issn.strip()
        if issn_key not in issn_to_resource:
            issn_to_resource[issn_key] = {'curie': curie, 'resource_id': resource_id}


def get_resource_by_title(title: str) -> Optional[Dict]:
    """
    Look up a resource by exact title match (case-insensitive).

    :param title: Title to look up
    :return: Dict with 'curie' and 'resource_id' if found, None otherwise.
    """
    if title and title.strip():
        title_key = title.strip().lower()
        if title_key in title_to_resource:
            return title_to_resource[title_key]
    return None


def update_title_mapping(curie: str, resource_id: int, title: str) -> None:
    """
    Update the title-to-resource mapping when a new resource is created.

    :param curie: Resource CURIE (e.g., AGRKB:102000000000001)
    :param resource_id: Database resource_id
    :param title: Resource title (can be None)
    """
    global title_to_resource

    if title and title.strip():
        title_key = title.strip().lower()
        if title_key not in title_to_resource:
            title_to_resource[title_key] = {'curie': curie, 'resource_id': resource_id}


def find_existing_resource_by_title(entry: Dict) -> Optional[Tuple[str, int]]:
    """
    Check if the entry's title matches an existing resource.
    This is used as a fallback when other matching methods fail.

    :param entry: DQM entry that may have 'title'
    :return: Tuple of (agr_curie, resource_id) if match found, None otherwise.
    """
    title = entry.get('title', '')
    if title:
        result = get_resource_by_title(title)
        if result:
            return (result['curie'], result['resource_id'])
    return None


def find_existing_resource_by_xrefs(entry: Dict) -> Optional[Tuple[str, int]]:
    """
    Check if any cross-reference in the entry matches an existing resource.
    This checks ALL cross-references, not just the primaryId.

    :param entry: DQM entry with 'crossReferences' list
    :return: Tuple of (agr_curie, resource_id) if match found, None otherwise.
    """
    cross_refs = entry.get('crossReferences', [])

    for xref in cross_refs:
        if 'id' not in xref:
            continue
        curie = xref['id']
        prefix, identifier, _ = split_identifier(curie, ignore_error=True)
        if prefix and identifier:
            agr = get_agr_for_xref(prefix, identifier)
            if agr:
                # Found existing resource via cross-reference
                resource = db_session.query(ResourceModel).filter(
                    ResourceModel.curie == agr
                ).first()
                if resource:
                    return (agr, resource.resource_id)

    return None


def find_existing_resource_by_issn(entry: Dict) -> Optional[Tuple[str, int]]:
    """
    Check if the entry's ISSNs match an existing resource.

    :param entry: DQM entry that may have 'printISSN' or 'onlineISSN'
    :return: Tuple of (agr_curie, resource_id) if match found, None otherwise.
    """
    # Check printISSN
    print_issn = entry.get('printISSN', '')
    if print_issn:
        result = get_resource_by_issn(print_issn)
        if result:
            return (result['curie'], result['resource_id'])

    # Check onlineISSN
    online_issn = entry.get('onlineISSN', '')
    if online_issn:
        result = get_resource_by_issn(online_issn)
        if result:
            return (result['curie'], result['resource_id'])

    # Also check cross-references for ISSN values
    cross_refs = entry.get('crossReferences', [])
    for xref in cross_refs:
        if 'id' not in xref:
            continue
        curie = xref['id']
        prefix, identifier, _ = split_identifier(curie, ignore_error=True)
        if prefix == 'ISSN' and identifier:
            result = get_resource_by_issn(identifier)
            if result:
                return (result['curie'], result['resource_id'])

    return None


def find_existing_resource(entry: Dict) -> Optional[Tuple[str, int, str]]:
    """
    Comprehensive check for existing resource using multiple methods:
    1. Check primaryId cross-reference
    2. Check all cross-references in the entry
    3. Check ISSN values
    4. Check exact title match (fallback)

    :param entry: DQM entry
    :return: Tuple of (agr_curie, resource_id, match_type) if found, None otherwise.
             match_type is one of: 'primaryId', 'crossReference', 'issn', 'title'
    """
    # 1. Check primaryId first
    primary_id = entry.get('primaryId', '')
    if primary_id:
        prefix, identifier, _ = split_identifier(primary_id, ignore_error=True)
        if prefix and identifier:
            agr = get_agr_for_xref(prefix, identifier)
            if agr:
                resource = db_session.query(ResourceModel).filter(
                    ResourceModel.curie == agr
                ).first()
                if resource:
                    return (agr, resource.resource_id, 'primaryId')

    # 2. Check all cross-references
    result = find_existing_resource_by_xrefs(entry)
    if result:
        return (result[0], result[1], 'crossReference')

    # 3. Check ISSNs
    result = find_existing_resource_by_issn(entry)
    if result:
        return (result[0], result[1], 'issn')

    # 4. Check exact title match (fallback to prevent duplicates with same title)
    result = find_existing_resource_by_title(entry)
    if result:
        return (result[0], result[1], 'title')

    return None
