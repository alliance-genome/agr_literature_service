from helper_file_processing import split_identifier

from literature.database.main import get_db
from literature.models import ReferenceModel, CrossReferenceModel, ResourceModel


def sqlalchemy_load_ref_xref(datatype):
    ref_xref_valid = dict()
    ref_xref_obsolete = dict()
    xref_ref = dict()
    db_session = next(get_db())

    query = None
    if datatype == 'reference':
        # 14 seconds to load all xref through sqlalchemy
        query = db_session.query(
            ReferenceModel.curie,
            CrossReferenceModel.curie,
            CrossReferenceModel.is_obsolete
        ).join(
            ReferenceModel.cross_references
        ).filter(
            CrossReferenceModel.reference_id.isnot(None)
        )

    elif datatype == 'resource':
        query = db_session.query(
            ResourceModel.curie,
            CrossReferenceModel.curie,
            CrossReferenceModel.is_obsolete
        ).join(
            ResourceModel.cross_references
        ).filter(
            CrossReferenceModel.resource_id.isnot(None)
        )

    if query is not None:
        results = query.all()
        for result in results:
            # print(result)
            agr = result[0]
            xref = result[1]
            is_obsolete = result[2]
            prefix, identifier, separator = split_identifier(xref, True)
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

    return xref_ref, ref_xref_valid, ref_xref_obsolete
