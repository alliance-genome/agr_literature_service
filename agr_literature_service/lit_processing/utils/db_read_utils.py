from sqlalchemy import or_
from fastapi.encoders import jsonable_encoder
from typing import Dict, Tuple, Union
from datetime import datetime, timedelta
import json

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.api.models import ReferenceModel, ResourceModel, \
    CrossReferenceModel, ModCorpusAssociationModel, ModModel, ReferenceCommentAndCorrectionModel


def get_pmid_association_to_mod_via_reference(db_session, pmids, mod_abbreviation):

    allRows = db_session.query(
        CrossReferenceModel.curie,
        ReferenceModel.curie,
        ModModel.abbreviation
    ).join(
        ReferenceModel.cross_reference
    ).filter(
        CrossReferenceModel.curie.in_(pmids)
    ).outerjoin(
        ReferenceModel.mod_corpus_association
    ).outerjoin(
        ModCorpusAssociationModel.mod
    ).all()

    pmid_curie_mod_dict: Dict[str, Tuple[Union[str, None], Union[str, None]]] = {}
    ## example: pmid_curie_mod_dict['PMID:35510023'] = ('AGR:AGR-Reference-0000862607', 'SGD')
    for x in allRows:
        pmid = x[0]
        ref_curie = x[1]
        mod = x[2]
        if pmid not in pmid_curie_mod_dict or pmid_curie_mod_dict[pmid][1] is None:
            pmid_curie_mod_dict[pmid] = (ref_curie, mod if mod == mod_abbreviation else None)
    for pmid in pmids:
        if pmid not in pmid_curie_mod_dict:
            pmid_curie_mod_dict[pmid] = (None, None)
    return pmid_curie_mod_dict


def get_curie_to_title_mapping(curie_list):

    if len(curie_list) == 0:
        return {}

    db_session = create_postgres_session(False)

    curies = ", ".join(["'" + x + "'" for x in curie_list])
    rs = db_session.execute("SELECT curie, title FROM reference WHERE curie IN (" + curies + ")")
    rows = rs.fetchall()

    ref_curie_to_title = {}
    for x in rows:
        ref_curie_to_title[x[0]] = x[1]

    db_session.close()

    return ref_curie_to_title


def get_references_by_curies(db_session, curie_list):

    if len(curie_list) == 0:
        return {}

    ref_curie_to_reference = {}

    for x in db_session.query(ReferenceModel).filter(ReferenceModel.curie.in_(curie_list)).all():
        ref_curie_to_reference[x.curie] = jsonable_encoder(x)

    return ref_curie_to_reference


def get_reference_id_by_curie(db_session, curie):

    x = db_session.query(ReferenceModel).filter_by(curie=curie).one_or_none()
    if x:
        return x.reference_id
    return None


def set_pmid_list(db_session, mod, pmids4mod, json_file):

    f = open(json_file)
    json_data = json.load(f)
    f.close()

    for x in json_data:
        if x.get('crossReferences'):
            for c in x['crossReferences']:
                if c['id'].startswith('PMID:'):
                    row = db_session.query(CrossReferenceModel).filter_by(curie=c['id']).one_or_none()
                    if row:
                        pmid = c['id'].replace('PMID:', '')
                        pmids4mod['all'].add(pmid)
                        pmids4mod[mod].add(pmid)


def retrieve_newly_added_pmids(db_session):

    pmids_new = []

    filter_after = datetime.today() - timedelta(days=150)

    for x in db_session.query(CrossReferenceModel).join(ReferenceModel.cross_reference).filter(CrossReferenceModel.curie.like('PMID:%')).filter(ReferenceModel.date_created >= filter_after).all():
        pmids_new.append(x.curie.replace('PMID:', ''))

    return pmids_new


def retrieve_all_pmids(db_session):

    pmids = []
    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie.like('PMID:%')).all():
        if x.is_obsolete:
            continue
        pmids.append(x.curie.replace("PMID:", ""))

    return pmids


def get_reference_id_by_pmid(db_session, pmid):

    x = db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie == 'PMID:' + pmid).one_or_none()
    if x:
        return x.reference_id
    else:
        return None


def get_orcid_data(db_session):

    orcid_dict = {}

    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie.like('ORCID:%')).all():
        orcid_dict[x.curie] = x.is_obsolete

    return orcid_dict


def adding_author_row(x, reference_id_to_authors):

    authors = []
    reference_id = x[0]
    if reference_id in reference_id_to_authors:
        authors = reference_id_to_authors[reference_id]
    authors.append({"orcid": x[1],
                    "first_author": x[2],
                    "order": x[3],
                    "corresponding_author": x[4],
                    "name": x[5],
                    "affiliations": x[6] if x[6] else [],
                    "first_name": x[7],
                    "last_name": x[8]})
    reference_id_to_authors[reference_id] = authors


def get_author_data(db_session, mod, reference_id_list, query_cutoff):

    reference_id_to_authors = {}

    if mod and len(reference_id_list) > query_cutoff:
        author_limit = 500000
        for index in range(500):
            offset = index * author_limit
            rs = db_session.execute("select a.reference_id, a.orcid, a.first_author, a.order, a.corresponding_author, a.name, a.affiliations, a.first_name, a.last_name from author a, mod_corpus_association mca, mod m where a.reference_id = mca.reference_id and mca.mod_id = m.mod_id and m.abbreviation = '" + mod + "' order by a.reference_id, a.order limit " + str(author_limit) + " offset " + str(offset))
            rows = rs.fetchall()
            if len(rows) == 0:
                break
            for x in rows:
                adding_author_row(x, reference_id_to_authors)
    elif reference_id_list and len(reference_id_list) > 0:
        # name & order are keywords in postgres so have use alias 'a' for table name
        ref_ids = ", ".join([str(x) for x in reference_id_list])
        raw_sql = "SELECT a.reference_id, a.orcid, a.first_author, a.order, a.corresponding_author, a.name, a.affiliations, a.first_name, a.last_name FROM author a WHERE reference_id IN (" + ref_ids + ") order by a.reference_id, a.order"
        rs = db_session.execute(raw_sql)
        rows = rs.fetchall()
        for x in rows:
            adding_author_row(x, reference_id_to_authors)

    return reference_id_to_authors


def adding_mesh_term_row(x, reference_id_to_mesh_terms):

    reference_id = x[0]
    mesh_terms = []
    if reference_id in reference_id_to_mesh_terms:
        mesh_terms = reference_id_to_mesh_terms[reference_id]
    qualifier_term = x[2] if x[2] else ''
    mesh_terms.append((x[1], qualifier_term))
    reference_id_to_mesh_terms[reference_id] = mesh_terms


def get_mesh_term_data(db_session, mod, reference_id_list, query_cutoff):

    reference_id_to_mesh_terms = {}

    if mod and len(reference_id_list) > query_cutoff:
        mesh_limit = 1000000
        for index in range(50):
            offset = index * mesh_limit
            rs = db_session.execute("select md.reference_id, md.heading_term, md.qualifier_term from mesh_detail md, mod_corpus_association mca, mod m where md.reference_id = mca.reference_id and mca.mod_id = m.mod_id and m.abbreviation = '" + mod + "' order by md.mesh_detail_id limit " + str(mesh_limit) + " offset " + str(offset))
            rows = rs.fetchall()
            if len(rows) == 0:
                break
            for x in rows:
                adding_mesh_term_row(x, reference_id_to_mesh_terms)
    elif reference_id_list and len(reference_id_list) > 0:
        ref_ids = ", ".join([str(x) for x in reference_id_list])
        raw_sql = "SELECT reference_id, heading_term, qualifier_term FROM mesh_detail WHERE reference_id IN (" + ref_ids + ")"
        rs = db_session.execute(raw_sql)
        rows = rs.fetchall()
        for x in rows:
            adding_mesh_term_row(x, reference_id_to_mesh_terms)

    return reference_id_to_mesh_terms


def get_cross_reference_data(db_session, mod, reference_id_list):

    reference_id_to_doi = {}
    reference_id_to_pmcid = {}

    allCrossRefs = None
    if mod:
        allCrossRefs = db_session.query(CrossReferenceModel).join(ReferenceModel.cross_reference).outerjoin(ReferenceModel.mod_corpus_association).outerjoin(ModCorpusAssociationModel.mod).filter(ModModel.abbreviation == mod).all()
    elif reference_id_list and len(reference_id_list) > 0:
        allCrossRefs = db_session.query(CrossReferenceModel).filter(CrossReferenceModel.reference_id.in_(reference_id_list)).all()

    if allCrossRefs is None:
        return (reference_id_to_doi, reference_id_to_pmcid)

    for x in allCrossRefs:
        if x.curie.startswith('DOI:'):
            reference_id_to_doi[x.reference_id] = x.curie.replace('DOI:', '')
        elif x.curie.startswith('PMCID:'):
            reference_id_to_pmcid[x.reference_id] = x.curie.replace('PMCID:', '')

    return (reference_id_to_doi, reference_id_to_pmcid)


def get_cross_reference_data_for_resource(db_session):

    resource_id_to_issn = {}
    resource_id_to_nlm = {}

    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.resource_id.isnot(None)).all():
        if x.curie.startswith('ISSN:'):
            resource_id_to_issn[x.resource_id] = x.curie.replace('ISSN:', '')
        elif x.curie.startswith('NLM:'):
            resource_id_to_nlm[x.resource_id] = x.curie.replace('NLM:', '')

    return (resource_id_to_issn, resource_id_to_nlm)


def get_comment_correction_data(db_session, mod, reference_id_list):

    reference_ids_to_comment_correction_type = {}

    allCommentCorrections = None
    if mod:
        allCommentCorrections = db_session.query(ReferenceCommentAndCorrectionModel).join(ReferenceModel.comment_and_corrections_in or ReferenceModel.comment_and_corrections_out).outerjoin(ReferenceModel.mod_corpus_association).outerjoin(ModCorpusAssociationModel.mod).filter(ModModel.abbreviation == mod).all()
    elif reference_id_list and len(reference_id_list) > 0:
        allCommentCorrections = db_session.query(ReferenceCommentAndCorrectionModel).filter(or_(ReferenceCommentAndCorrectionModel.reference_id_from.in_(reference_id_list), ReferenceCommentAndCorrectionModel.reference_id_to.in_(reference_id_list))).all()

    if allCommentCorrections is None:
        return reference_ids_to_comment_correction_type

    for x in allCommentCorrections:
        type = x.reference_comment_and_correction_type.replace("x.reference_comment_and_correction_type", "")
        reference_ids_to_comment_correction_type[(x.reference_id_from, x.reference_id_to)] = type

    return reference_ids_to_comment_correction_type


def get_all_comment_correction_data(db_session, logger=None):

    reference_id_to_comment_correction_data = {}

    type_mapping = {'ErratumFor': 'ErratumIn',
                    'RepublishedFrom': 'RepublishedIn',
                    'RetractionOf': 'RetractionIn',
                    'ExpressionOfConcernFor': 'ExpressionOfConcernIn',
                    'ReprintOf': 'ReprintIn',
                    'UpdateOf': 'UpdateIn'}

    reference_id_to_curies = {}
    rs = db_session.execute("select cc.reference_id, cc.curie, r.curie from cross_reference cc, reference r where cc.reference_id = r.reference_id and (cc.reference_id in (select reference_id_from from reference_comments_and_corrections) or cc.reference_id in (select reference_id_to from reference_comments_and_corrections))")
    rows = rs.fetchall()
    for x in rows:
        if x[1].startswith('PMID:'):
            reference_id_to_curies[x[0]] = (x[1], x[2])

    rs = db_session.execute("select reference_id_from, reference_id_to, reference_comment_and_correction_type from reference_comments_and_corrections")

    for x in rs:

        type_db = x[2]
        type_db = type_db.replace("ReferenceCommentAndCorrectionType.", "")
        reference_id_from = x[0]
        reference_id_to = x[1]

        ## for reference_id_from
        data = {}
        if reference_id_from in reference_id_to_comment_correction_data:
            data = reference_id_to_comment_correction_data[reference_id_from]
        if reference_id_from in reference_id_to_curies:
            (pmid, ref_curie) = reference_id_to_curies[reference_id_from]
            data[type_db] = {"PMID": pmid,
                             "reference_curie": ref_curie}
            reference_id_to_comment_correction_data[reference_id_from] = data

        ## for reference_id_to
        data = {}
        if reference_id_to in reference_id_to_comment_correction_data:
            data = reference_id_to_comment_correction_data[reference_id_to]

        if reference_id_to in reference_id_to_curies:
            (pmid, ref_curie) = reference_id_to_curies[reference_id_to]
            type = type_mapping.get(type_db)
            if type is None:
                if logger:
                    logger.info(type_db + " is not in type_mapping.")
            else:
                data[type] = {"PMID": pmid,
                              "reference_curie": ref_curie}
                reference_id_to_comment_correction_data[reference_id_to] = data

    return reference_id_to_comment_correction_data


def get_journal_data(db_session):

    journal_to_resource_id = {}

    for x in db_session.query(ResourceModel).all():
        journal_to_resource_id[x.iso_abbreviation] = (x.resource_id, x.title)

    return journal_to_resource_id


def get_reference_ids_by_pmids(db_session, pmids, pmid_to_reference_id, reference_id_to_pmid):

    pmid_list = []
    for pmid in pmids.split('|'):
        pmid_list.append('PMID:' + pmid)

    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie.in_(pmid_list)).all():
        if x.is_obsolete is True:
            continue
        pmid = x.curie.replace('PMID:', '')
        pmid_to_reference_id[pmid] = x.reference_id
        reference_id_to_pmid[x.reference_id] = pmid


def get_pmid_to_reference_id_for_papers_not_associated_with_mod(db_session, pmid_to_reference_id, reference_id_to_pmid):

    in_corpus = {}
    for x in db_session.query(ModCorpusAssociationModel).all():
        in_corpus[x.reference_id] = 1

    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie.like('PMID:%')).all():
        if x.reference_id in in_corpus:
            continue
        pmid = x.curie.replace('PMID:', '')
        pmid_to_reference_id[pmid] = x.reference_id
        reference_id_to_pmid[x.reference_id] = pmid


def get_pmid_to_reference_id(db_session, mod, pmid_to_reference_id, reference_id_to_pmid):

    for x in db_session.query(CrossReferenceModel).join(ReferenceModel.cross_reference).filter(CrossReferenceModel.curie.like('PMID:%')).outerjoin(ReferenceModel.mod_corpus_association).outerjoin(ModCorpusAssociationModel.mod).filter(ModModel.abbreviation == mod).all():
        if x.is_obsolete is True:
            continue
        pmid = x.curie.replace('PMID:', '')
        pmid_to_reference_id[pmid] = x.reference_id
        reference_id_to_pmid[x.reference_id] = pmid


def get_doi_data(db_session):

    doi_to_reference_id = {}

    rs = db_session.execute("select curie, reference_id from cross_reference where curie like 'DOI:%%'")
    rows = rs.fetchall()
    for x in rows:
        doi_to_reference_id[x[0]] = x[1]

    return doi_to_reference_id


def get_reference_by_pmid(db_session, pmid):

    x = db_session.query(CrossReferenceModel).filter_by(curie='PMID:' + pmid).one_or_none()

    if x:
        return x.reference_id
    return None


def get_journal_by_resource_id(db_session):

    resource_id_to_journal = {}

    rs = db_session.execute("SELECT resource_id, curie, title FROM resource")

    rows = rs.fetchall()

    for x in rows:
        resource_id_to_journal[x[0]] = (x[1], x[2])

    return resource_id_to_journal


def get_mod_corpus_association_data_for_ref_ids(db_session, ref_ids):

    reference_id_to_mod_corpus_data = {}

    rs = db_session.execute("SELECT mca.reference_id, mca.mod_corpus_association_id, m.abbreviation, mca.corpus, mca.mod_corpus_sort_source, mca.date_created, mca.date_updated FROM mod_corpus_association mca, mod m WHERE m.mod_id = mca.mod_id and mca.reference_id IN (" + ref_ids + ")")

    rows = rs.fetchall()

    for x in rows:
        data = []
        reference_id = x[0]
        if reference_id in reference_id_to_mod_corpus_data:
            data = reference_id_to_mod_corpus_data[reference_id]
        data.append({"mod_corpus_association_id": x[1],
                     "mod_abbreviation": x[2],
                     "corpus": x[3],
                     "mod_corpus_sort_source": x[4],
                     "date_created": str(x[5]),
                     "date_updated": str(x[6])})
        reference_id_to_mod_corpus_data[reference_id] = data

    return reference_id_to_mod_corpus_data


def get_cross_reference_data_for_ref_ids(db_session, ref_ids):

    reference_id_to_xrefs = {}

    rs = db_session.execute("SELECT reference_id, curie, is_obsolete FROM cross_reference WHERE reference_id IN (" + ref_ids + ")")

    rows = rs.fetchall()

    for x in rows:
        data = []
        reference_id = x[0]
        if reference_id in reference_id_to_xrefs:
            data = reference_id_to_xrefs[reference_id]
        row = {"curie": x[1],
               "is_obsolete": x[2]}
        data.append(row)
        reference_id_to_xrefs[reference_id] = data

    return reference_id_to_xrefs


def get_author_data_for_ref_ids(db_session, ref_ids):

    reference_id_to_authors = {}

    rs = db_session.execute("SELECT a.author_id, a.reference_id, a.orcid, a.first_author, a.order, a.corresponding_author, a.name, a.affiliations, a.first_name, a.last_name, a.date_updated, a.date_created FROM author a WHERE reference_id IN (" + ref_ids + ") order by a.reference_id, a.order")

    rows = rs.fetchall()

    for x in rows:
        data = []
        reference_id = x[1]
        if reference_id in reference_id_to_authors:
            data = reference_id_to_authors[reference_id]
        data.append({"author_id": x[0],
                     "orcid": x[2],
                     "first_author": x[3],
                     "order": x[4],
                     "corresponding_author": x[5],
                     "name": x[6],
                     "affilliations": x[7] if x[7] else [],
                     "first_name": x[8],
                     "last_name": x[9],
                     "date_updated": str(x[10]),
                     "date_created": str(x[11])})
        reference_id_to_authors[reference_id] = data

    return reference_id_to_authors


def get_mesh_term_data_for_ref_ids(db_session, ref_ids):

    reference_id_to_mesh_terms = {}

    rs = db_session.execute("SELECT mesh_detail_id, reference_id, heading_term, qualifier_term FROM mesh_detail WHERE reference_id IN (" + ref_ids + ") order by reference_id, mesh_detail_id")

    rows = rs.fetchall()

    for x in rows:
        reference_id = x[1]
        data = []
        if reference_id in reference_id_to_mesh_terms:
            data = reference_id_to_mesh_terms[reference_id]
        data.append({"heading_term": x[2],
                     "qualifier_term": x[3],
                     "mesh_detail_id": x[0]})
        reference_id_to_mesh_terms[reference_id] = data

    return reference_id_to_mesh_terms


def get_mod_reference_type_data_for_ref_ids(db_session, ref_ids):

    reference_id_to_mod_reference_types = {}

    rs = db_session.execute("SELECT rmrt.reference_mod_referencetype_id, rmrt.reference_id, rt.label, "
                            "mod.abbreviation FROM reference_mod_referencetype rmrt JOIN mod_referencetype mrt "
                            "ON rmrt.mod_referencetype_id = mrt.mod_referencetype_id JOIN referencetype rt "
                            "ON mrt.referencetype_id = rt.referencetype_id JOIN mod on mrt.mod_id = mod.mod_id "
                            "WHERE rmrt.reference_id IN (" + ref_ids + ")")

    rows = rs.fetchall()

    for x in rows:
        reference_id = x[1]
        data = []
        if reference_id in reference_id_to_mod_reference_types:
            data = reference_id_to_mod_reference_types[reference_id]
        data.append({"reference_type": x[2],
                     "source": x[3],
                     "mod_reference_type_id": x[0]})
        reference_id_to_mod_reference_types[reference_id] = data

    return reference_id_to_mod_reference_types
