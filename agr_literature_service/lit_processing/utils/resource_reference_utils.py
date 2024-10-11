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

It is my hope that the developer does not need to worry about all that goes on here
but can just call the methods and everything will be taken care off.
"""
import sys
from typing import Dict, Union
import logging.config
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ResourceModel, ReferenceModel, CrossReferenceModel
# from agr_literature_service.lit_processing.utils.generic_utils import split_identifier

xref_ref: Dict = {}
ref_xref_valid: Dict = {}
ref_xref_obsolete: Dict = {}
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
