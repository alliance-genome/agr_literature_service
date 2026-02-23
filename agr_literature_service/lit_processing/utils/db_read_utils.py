from collections import defaultdict
from sqlalchemy import or_, text, bindparam
from fastapi.encoders import jsonable_encoder
from typing import Dict, Tuple, Union, List, Any, TypedDict, Iterable
from datetime import datetime, timedelta
import json
import re

from sqlalchemy.orm import joinedload, Session

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.api.models import ReferenceModel, ResourceModel, \
    CrossReferenceModel, ModCorpusAssociationModel, ModModel, ReferenceRelationModel, \
    ReferenceModReferencetypeAssociationModel, ModReferencetypeAssociationModel
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils \
    import escape_special_characters, remove_surrogates


def get_pmid_association_to_mod_via_reference(db_session: Session, pmids, mod_abbreviation):
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
    rs = db_session.execute(text("SELECT curie, title FROM reference WHERE curie IN (" + curies + ")"))
    rows = rs.fetchall()

    ref_curie_to_title = {x[0]: x[1] for x in rows}

    db_session.close()

    return ref_curie_to_title


def get_references_by_curies(db_session: Session, curie_list):
    if len(curie_list) == 0:
        return {}

    ref_curie_to_reference = {}

    query = db_session.query(ReferenceModel)
    query = query.options(joinedload(ReferenceModel.mod_referencetypes)
                          .subqueryload(ReferenceModReferencetypeAssociationModel.mod_referencetype)
                          .subqueryload(ModReferencetypeAssociationModel.mod))
    query = query.options(joinedload(ReferenceModel.mod_referencetypes)
                          .subqueryload(ReferenceModReferencetypeAssociationModel.mod_referencetype)
                          .subqueryload(ModReferencetypeAssociationModel.referencetype))
    query = query.options(joinedload(ReferenceModel.author))
    query = query.options(joinedload(ReferenceModel.mod_corpus_association))
    query = query.options(joinedload(ReferenceModel.obsolete_reference))
    query = query.options(joinedload(ReferenceModel.mesh_term))
    query = query.options(joinedload(ReferenceModel.workflow_tag))

    for x in query.filter(ReferenceModel.curie.in_(curie_list)).all():
        ref_curie_to_reference[x.curie] = jsonable_encoder(x)

    return ref_curie_to_reference


def get_reference_id_by_curie(db_session: Session, curie):
    x = db_session.query(ReferenceModel).filter_by(curie=curie).one_or_none()
    if x:
        return x.reference_id
    return None


def set_pmid_list(db_session: Session, mod, pmids4mod, json_file):

    with open(json_file) as f:
        json_data = json.load(f)

    for x in json_data:
        if x.get('crossReferences'):
            for c in x['crossReferences']:
                if c['id'].startswith('PMID:'):
                    row = db_session.query(CrossReferenceModel).filter_by(curie=c['id']).one_or_none()
                    if row:
                        pmid = c['id'].replace('PMID:', '')
                        pmids4mod['all'].add(pmid)
                        pmids4mod[mod].add(pmid)


def retrieve_newly_added_pmids(db_session: Session):
    pmids_new = []

    filter_after = datetime.today() - timedelta(days=150)

    for x in db_session.query(CrossReferenceModel).join(ReferenceModel.cross_reference).filter(
            CrossReferenceModel.curie.like('PMID:%')).filter(ReferenceModel.date_created >= filter_after).all():
        pmids_new.append(x.curie.replace('PMID:', ''))

    return pmids_new


def retrieve_all_pmids(db_session: Session):
    pmids = []
    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie.like('PMID:%')).all():
        if x.is_obsolete:
            continue
        pmids.append(x.curie.replace("PMID:", ""))

    return pmids


def get_reference_id_by_pmid(db_session: Session, pmid):
    x = db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie == 'PMID:' + pmid).one_or_none()
    if x:
        return x.reference_id
    else:
        return None


def adding_author_row(x, reference_id_to_authors):
    authors = []
    reference_id = x[0]
    if reference_id in reference_id_to_authors:
        authors = reference_id_to_authors[reference_id]
    authors.append({
        "orcid": x[1],
        "first_author": x[2],
        "order": x[3],
        "corresponding_author": x[4],
        "name": x[5],
        "affiliations": x[6] if x[6] else [],
        "first_name": x[7],
        "last_name": x[8],
        "first_initial": x[9]
    })
    reference_id_to_authors[reference_id] = authors


def get_author_data(db_session: Session, mod, reference_id_list, query_cutoff):

    reference_id_to_authors: Dict[int, List[Dict[str, Any]]] = {}

    if mod and len(reference_id_list) > query_cutoff:
        author_limit = 500000
        for index in range(500):
            offset = index * author_limit
            rows = db_session.execute(text(f"SELECT a.reference_id, a.orcid, a.first_author, a.order, "
                                           f"a.corresponding_author, a.name, a.affiliations, a.first_name, "
                                           f"a.last_name, a.first_initial "
                                           f"FROM author a, mod_corpus_association mca, mod m "
                                           f"WHERE a.reference_id = mca.reference_id "
                                           f"AND mca.mod_id = m.mod_id "
                                           f"AND m.abbreviation = '{mod}' "
                                           f"ORDER BY a.reference_id, a.order "
                                           f"LIMIT {author_limit} "
                                           f"OFFSET {offset}")).fetchall()

            if len(rows) == 0:
                break
            for x in rows:
                adding_author_row(x, reference_id_to_authors)
    elif reference_id_list and len(reference_id_list) > 0:
        # name & order are keywords in postgres so have use alias 'a' for table name
        ref_ids = ", ".join([str(x) for x in reference_id_list])
        rows = db_session.execute(text(f"SELECT a.reference_id, a.orcid, a.first_author, a.order, "
                                       f"a.corresponding_author, a.name, a.affiliations, a.first_name, "
                                       f"a.last_name, a.first_initial "
                                       f"FROM author a "
                                       f"WHERE  reference_id IN ({ref_ids}) "
                                       f"ORDER BY a.reference_id, a.order")).fetchall()
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


def get_mesh_term_data(db_session: Session, mod, reference_id_list, query_cutoff):

    reference_id_to_mesh_terms: Dict[int, List[Tuple[str, str]]] = {}

    if mod and len(reference_id_list) > query_cutoff:
        mesh_limit = 1000000
        for index in range(50):
            offset = index * mesh_limit
            rs = db_session.execute(text(
                "select md.reference_id, md.heading_term, md.qualifier_term from mesh_detail md, mod_corpus_association mca, mod m where md.reference_id = mca.reference_id and mca.mod_id = m.mod_id and m.abbreviation = '" + mod + "' order by md.mesh_detail_id limit " + str(
                    mesh_limit) + " offset " + str(offset)))
            rows = rs.fetchall()
            if len(rows) == 0:
                break
            for x in rows:
                adding_mesh_term_row(x, reference_id_to_mesh_terms)
    elif reference_id_list and len(reference_id_list) > 0:
        ref_ids = ", ".join([str(x) for x in reference_id_list])
        raw_sql = "SELECT reference_id, heading_term, qualifier_term FROM mesh_detail WHERE reference_id IN (" + ref_ids + ")"
        rs = db_session.execute(text(raw_sql))
        rows = rs.fetchall()
        for x in rows:
            adding_mesh_term_row(x, reference_id_to_mesh_terms)

    return reference_id_to_mesh_terms


def get_cross_reference_data(db_session: Session, mod, reference_id_list):

    reference_id_to_doi: Dict[int, str] = {}
    reference_id_to_pmcid: Dict[int, str] = {}

    allCrossRefs = None
    if mod:
        allCrossRefs = db_session.query(CrossReferenceModel).join(ReferenceModel.cross_reference).outerjoin(
            ReferenceModel.mod_corpus_association).outerjoin(ModCorpusAssociationModel.mod).filter(
            ModModel.abbreviation == mod).all()
    elif reference_id_list and len(reference_id_list) > 0:
        allCrossRefs = db_session.query(CrossReferenceModel).filter(
            CrossReferenceModel.reference_id.in_(reference_id_list)).all()

    if allCrossRefs is None:
        return (reference_id_to_doi, reference_id_to_pmcid)

    for x in allCrossRefs:
        if x.is_obsolete is True:
            continue
        if x.curie.startswith('DOI:'):
            reference_id_to_doi[x.reference_id] = x.curie.replace('DOI:', '')
        elif x.curie.startswith('PMCID:'):
            reference_id_to_pmcid[x.reference_id] = x.curie.replace('PMCID:', '')

    return (reference_id_to_doi, reference_id_to_pmcid)


def get_cross_reference_data_for_resource(db_session: Session):
    resource_id_to_issn = {}
    resource_id_to_nlm = {}

    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.resource_id.isnot(None)).all():
        if x.curie.startswith('ISSN:'):
            resource_id_to_issn[x.resource_id] = x.curie.replace('ISSN:', '')
        elif x.curie.startswith('NLM:'):
            resource_id_to_nlm[x.resource_id] = x.curie.replace('NLM:', '')

    return (resource_id_to_issn, resource_id_to_nlm)


def get_reference_relation_data(db_session: Session, mod, reference_id_list):

    reference_ids_to_reference_relation_type: Dict[Tuple[int, int], str] = {}

    allReferenceRelations = None
    if mod:
        allReferenceRelations = db_session.query(ReferenceRelationModel).join(
            ReferenceModel.reference_relation_in or ReferenceModel.reference_relation_out).outerjoin(
            ReferenceModel.mod_corpus_association).outerjoin(ModCorpusAssociationModel.mod).filter(
            ModModel.abbreviation == mod).all()
    elif reference_id_list and len(reference_id_list) > 0:
        allReferenceRelations = db_session.query(ReferenceRelationModel).filter(
            or_(ReferenceRelationModel.reference_id_from.in_(reference_id_list),
                ReferenceRelationModel.reference_id_to.in_(reference_id_list))).all()

    if allReferenceRelations is None:
        return reference_ids_to_reference_relation_type

    for x in allReferenceRelations:
        type = x.reference_relation_type.replace("x.reference_relation_type", "")
        reference_ids_to_reference_relation_type[(x.reference_id_from, x.reference_id_to)] = type

    return reference_ids_to_reference_relation_type


def _normalize_ids(reference_id_list: Iterable[int] | str) -> List[int]:
    if not reference_id_list:
        return []
    if isinstance(reference_id_list, str):
        # accept "1,2  3, 4"
        parts = re.split(r"[,\s]+", reference_id_list.strip())
        return [int(p) for p in parts if p]
    return [int(x) for x in reference_id_list]


def get_citation_data(db_session: Session, reference_id_list=None) -> Dict[int, dict]:

    if reference_id_list is None:
        sql = text("""
            SELECT r.reference_id,
                   c.citation,
                   COALESCE(c.short_citation, c.citation) AS short_citation
            FROM reference r
            JOIN citation c ON r.citation_id = c.citation_id
        """)
        rows = db_session.execute(sql)
        return {rid: {"citation": cit, "short_citation": short} for rid, cit, short in rows}

    ids = _normalize_ids(reference_id_list)
    if not ids:
        return {}

    sql = text("""
        SELECT r.reference_id,
               c.citation,
               COALESCE(c.short_citation, c.citation) AS short_citation
        FROM reference r
        JOIN citation c ON r.citation_id = c.citation_id
        WHERE r.reference_id IN :ref_ids
        ORDER BY r.reference_id
    """).bindparams(bindparam("ref_ids", expanding=True))

    rows = db_session.execute(sql, {"ref_ids": ids}).tuples()
    return {rid: {"citation": cit, "short_citation": short} for rid, cit, short in rows}


def get_license_data(db_session: Session):
    sql_query = text(
        "SELECT r.reference_id, cl.name, cl.url, cl.open_access, cl.description "
        "FROM reference r "
        "JOIN copyright_license cl "
        "  ON r.copyright_license_id = cl.copyright_license_id"
    )
    return {
        ref_id: {"name": name, "url": url, "open_access": oa, "description": desc}
        for ref_id, name, url, oa, desc in db_session.execute(sql_query)
    }


def get_all_reference_relation_data(db_session: Session, logger=None):

    reference_id_to_reference_relation_data: Dict[int, Dict[str, List[Dict[str, str]]]] = {}

    type_mapping = {
        'ErratumFor': 'ErratumIn',
        'CommentOn': 'CommentIn',
        'RepublishedFrom': 'RepublishedIn',
        'RetractionOf': 'RetractionIn',
        'ExpressionOfConcernFor': 'ExpressionOfConcernIn',
        'ReprintOf': 'ReprintIn',
        'UpdateOf': 'UpdateIn',
        'ChapterIn': 'hasChapter'
    }

    reference_id_to_curies = {}
    rs = db_session.execute(text(
        "select cc.reference_id, cc.curie, r.curie from cross_reference cc, reference r where cc.reference_id = r.reference_id and (cc.reference_id in (select reference_id_from from reference_relation) or cc.reference_id in (select reference_id_to from reference_relation))"))
    rows = rs.fetchall()
    for x in rows:
        if x[1].startswith('PMID:'):
            reference_id_to_curies[x[0]] = (x[1], x[2])

    rs = db_session.execute(text(
        "select reference_id_from, reference_id_to, reference_relation_type from reference_relation"))

    for x in rs:

        type_db = x[2]
        type_db = type_db.replace("ReferenceRelationType.", "")
        reference_id_from = x[0]
        reference_id_to = x[1]

        ## for reference_id_from
        data = {}
        if reference_id_from in reference_id_to_reference_relation_data:
            data = reference_id_to_reference_relation_data[reference_id_from]
        if reference_id_to in reference_id_to_curies:
            (pmid, ref_curie) = reference_id_to_curies[reference_id_to]
            if type_db not in data:
                data[type_db] = []
            data[type_db].append({"PMID": pmid,
                                  "reference_curie": ref_curie})
            reference_id_to_reference_relation_data[reference_id_from] = data

        ## for reference_id_to
        data = {}
        if reference_id_to in reference_id_to_reference_relation_data:
            data = reference_id_to_reference_relation_data[reference_id_to]

        if reference_id_from in reference_id_to_curies:
            (pmid, ref_curie) = reference_id_to_curies[reference_id_from]
            type = type_mapping.get(type_db)
            if type is None:
                if logger:
                    logger.info(type_db + " is not in type_mapping.")
            else:
                if type not in data:
                    data[type] = []
                data[type].append({"PMID": pmid,
                                   "reference_curie": ref_curie})
                reference_id_to_reference_relation_data[reference_id_to] = data

    return reference_id_to_reference_relation_data


def get_journal_data(db_session: Session):
    journal_to_resource_id = {}

    for x in db_session.query(ResourceModel).order_by(ResourceModel.resource_id).all():
        if x.iso_abbreviation not in journal_to_resource_id:
            journal_to_resource_id[x.iso_abbreviation] = (x.resource_id, x.title)

    return journal_to_resource_id


def get_reference_ids_by_pmids(db_session: Session, pmids, pmid_to_reference_id, reference_id_to_pmid):
    pmid_list = []
    for pmid in pmids.split('|'):
        pmid_list.append('PMID:' + pmid)

    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie.in_(pmid_list)).all():
        if x.is_obsolete is True:
            continue
        pmid = x.curie.replace('PMID:', '')
        pmid_to_reference_id[pmid] = x.reference_id
        reference_id_to_pmid[x.reference_id] = pmid


def get_pmid_to_reference_id_for_papers_not_associated_with_mod(db_session: Session, pmid_to_reference_id, reference_id_to_pmid):
    in_corpus = {}
    for x in db_session.query(ModCorpusAssociationModel).all():
        in_corpus[x.reference_id] = 1

    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie.like('PMID:%')).all():
        if x.reference_id in in_corpus:
            continue
        pmid = x.curie.replace('PMID:', '')
        pmid_to_reference_id[pmid] = x.reference_id
        reference_id_to_pmid[x.reference_id] = pmid


"""
def get_pmid_to_reference_id(db_session: Session, mod, pmid_to_reference_id, reference_id_to_pmid):
    query = db_session.query(CrossReferenceModel)
    query = query.join(ReferenceModel.cross_reference)
    query = query.filter(CrossReferenceModel.curie.like('PMID:%'))
    query = query.outerjoin(ReferenceModel.mod_corpus_association)
    query = query.outerjoin(ModCorpusAssociationModel.mod)
    for x in query.filter(ModModel.abbreviation == mod).all():
        if x.is_obsolete is True:
            continue
        pmid = x.curie.replace('PMID:', '')
        pmid_to_reference_id[pmid] = x.reference_id
        reference_id_to_pmid[x.reference_id] = pmid
"""


def get_pmid_to_reference_id(db_session: Session, mod, pmid_to_reference_id, reference_id_to_pmid):
    rows = (
        db_session.query(CrossReferenceModel)
        .join(ReferenceModel.cross_reference)
        .join(ReferenceModel.mod_corpus_association)
        .join(ModCorpusAssociationModel.mod)
        .filter(ModModel.abbreviation == mod)
        .filter(CrossReferenceModel.curie.like('PMID:%'))
        .filter(CrossReferenceModel.is_obsolete.is_(False))
        .filter(ModCorpusAssociationModel.corpus.is_(True))
        .all()
    )

    for x in rows:
        pmid = x.curie.replace('PMID:', '')
        pmid_to_reference_id[pmid] = x.reference_id
        reference_id_to_pmid[x.reference_id] = pmid


def get_doi_data(db_session: Session):
    doi_to_reference_id = {}

    rs = db_session.execute(text("select curie, reference_id from cross_reference where curie like 'DOI:%%'"))
    rows = rs.fetchall()
    for x in rows:
        doi_to_reference_id[x[0]] = x[1]

    return doi_to_reference_id


def get_reference_by_pmid(db_session: Session, pmid):
    x = db_session.query(CrossReferenceModel).filter_by(curie='PMID:' + pmid).one_or_none()

    if x:
        return x.reference_id
    return None


def get_journal_by_resource_id(db_session: Session):
    resource_id_to_journal = {}

    rs = db_session.execute(text("SELECT resource_id, curie, title, medline_abbreviation FROM resource"))

    rows = rs.fetchall()

    for x in rows:
        resource_id_to_journal[x[0]] = (x[1], x[2], x[3])

    return resource_id_to_journal


def get_mod_corpus_association_data_for_ref_ids(db_session: Session, ref_ids):

    reference_id_to_mod_corpus_data: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

    query = text("""
        SELECT
            mca.reference_id,
            mca.mod_corpus_association_id,
            m.abbreviation,
            mca.corpus,
            mca.mod_corpus_sort_source,
            mca.date_created,
            mca.date_updated,
            mca.created_by,
            mca.updated_by,
            u.email
        FROM
            mod_corpus_association mca
        JOIN
            mod m ON m.mod_id = mca.mod_id
        JOIN
            users u ON u.id = mca.updated_by
        WHERE
            mca.reference_id IN :ref_id_list
    """)

    ref_id_list = [int(ref_id.strip()) for ref_id in ref_ids.split(',') if ref_id.strip().isdigit()]
    result = db_session.execute(query, {"ref_id_list": tuple(ref_id_list)})

    rows = result.fetchall()

    for row in rows:
        reference_id = row.reference_id
        data_entry = {
            "mod_corpus_association_id": row.mod_corpus_association_id,
            "mod_abbreviation": row.abbreviation,
            "corpus": row.corpus,
            "mod_corpus_sort_source": remove_surrogates(row.mod_corpus_sort_source),
            "date_created": row.date_created.isoformat() if row.date_created else None,
            "date_updated": row.date_updated.isoformat() if row.date_updated else None,
            "created_by": row.created_by,
            "updated_by": row.updated_by,
            "updated_by_email": row.email
        }
        reference_id_to_mod_corpus_data[reference_id].append(data_entry)

    return dict(reference_id_to_mod_corpus_data)


def get_cross_reference_data_for_ref_ids(db_session: Session, ref_ids):

    reference_id_to_xrefs: Dict[int, List[Dict[str, Any]]] = {}

    rs = db_session.execute(text(
        "SELECT reference_id, curie, is_obsolete FROM cross_reference WHERE reference_id IN (" + ref_ids + ")"))

    rows = rs.fetchall()

    for x in rows:
        data = []
        reference_id = x[0]
        if reference_id in reference_id_to_xrefs:
            data = reference_id_to_xrefs[reference_id]
        row = {
            "curie": remove_surrogates(x[1]),
            "is_obsolete": x[2]
        }
        data.append(row)
        reference_id_to_xrefs[reference_id] = data

    return reference_id_to_xrefs


def get_author_data_for_ref_ids(db_session: Session, ref_ids):

    reference_id_to_authors: Dict[int, List[Dict[str, Any]]] = {}

    rows = db_session.execute(text(f"SELECT a.author_id, a.reference_id, a.orcid, a.first_author, "
                                   f"a.order, a.corresponding_author, a.name, a.affiliations, a.first_name, "
                                   f"a.last_name, a.first_initial, a.date_updated, a.date_created "
                                   f"FROM author a "
                                   f"WHERE reference_id IN ({ref_ids}) "
                                   f"ORDER BY a.reference_id, a.order")).fetchall()
    for x in rows:
        data = []
        reference_id = x[1]
        if reference_id in reference_id_to_authors:
            data = reference_id_to_authors[reference_id]
        data.append({
            "author_id": x[0],
            "orcid": escape_special_characters(x[2]),
            "first_author": x[3],
            "order": x[4],
            "corresponding_author": x[5],
            "name": escape_special_characters(x[6]),
            "affilliations": [escape_special_characters(a) for a in x[7]] if x[7] else [],
            "first_name": escape_special_characters(x[8]),
            "last_name": escape_special_characters(x[9]),
            "first_initial": escape_special_characters(x[10]),
            "date_updated": escape_special_characters(str(x[11])),
            "date_created": escape_special_characters(str(x[12]))
        })
        reference_id_to_authors[reference_id] = data

    return reference_id_to_authors


def get_mesh_term_data_for_ref_ids(db_session: Session, ref_ids):

    reference_id_to_mesh_terms: Dict[int, List[Dict[str, Any]]] = {}

    rs = db_session.execute(text(
        "SELECT mesh_detail_id, reference_id, heading_term, qualifier_term FROM mesh_detail WHERE reference_id IN (" + ref_ids + ") order by reference_id, mesh_detail_id"))

    rows = rs.fetchall()

    for x in rows:
        reference_id = x[1]
        data = []
        if reference_id in reference_id_to_mesh_terms:
            data = reference_id_to_mesh_terms[reference_id]
        data.append({
            "heading_term": remove_surrogates(x[2]),
            "qualifier_term": remove_surrogates(x[3]),
            "mesh_detail_id": x[0]
        })
        reference_id_to_mesh_terms[reference_id] = data

    return reference_id_to_mesh_terms


class ModReferenceTypeEntry(TypedDict):
    reference_type: str
    source: str
    mod_reference_type_id: int


def get_mod_reference_type_data_for_ref_ids(db_session: Session, ref_ids):

    reference_id_to_mod_reference_types: Dict[int, List[ModReferenceTypeEntry]] = {}

    rs = db_session.execute(text("SELECT rmrt.reference_mod_referencetype_id, rmrt.reference_id, rt.label, "
                                 "mod.abbreviation FROM reference_mod_referencetype rmrt JOIN mod_referencetype mrt "
                                 "ON rmrt.mod_referencetype_id = mrt.mod_referencetype_id JOIN referencetype rt "
                                 "ON mrt.referencetype_id = rt.referencetype_id JOIN mod ON mrt.mod_id = mod.mod_id "
                                 "WHERE rmrt.reference_id IN (" + ref_ids + ")"))

    rows = rs.fetchall()

    for x in rows:
        reference_id = x[1]
        data = []
        if reference_id in reference_id_to_mod_reference_types:
            data = reference_id_to_mod_reference_types[reference_id]
        data.append({
            "reference_type": remove_surrogates(x[2]),
            "source": remove_surrogates(x[3]),
            "mod_reference_type_id": x[0]
        })
        reference_id_to_mod_reference_types[reference_id] = data

    return reference_id_to_mod_reference_types


def get_mod_abbreviations(db_session: Session = None):
    if db_session is None:
        db_session = create_postgres_session(False)
    return [res[0] for res in db_session.query(ModModel.abbreviation).filter(ModModel.abbreviation.not_in(['GO', 'alliance'])).all()]


def get_pmid_list_without_pmc_package(mods, db_session: Session = None):
    if db_session is None:
        db_session = create_postgres_session(False)

    mod_to_mod_id = {x.abbreviation: x.mod_id for x in db_session.query(ModModel).all()}

    rows = db_session.execute(text("SELECT distinct rf.reference_id "
                                   "FROM referencefile rf, referencefile_mod rfm "
                                   "WHERE rfm.mod_id is null "
                                   "AND rf.referencefile_id = rfm.referencefile_id ")).fetchall()

    reference_ids_with_PMC = {x[0] for x in rows}

    pmids = []

    for mod in mods:

        mod_id = mod_to_mod_id[mod]

        rows = db_session.execute(text(f"SELECT cr.reference_id, cr.curie "
                                       f"FROM cross_reference cr, mod_corpus_association mca "
                                       f"WHERE cr.curie_prefix = 'PMID' "
                                       f"AND cr.is_obsolete is False "
                                       f"AND cr.reference_id = mca.reference_id "
                                       f"AND mca.corpus is True "
                                       f"AND mca.mod_id = {mod_id} ")).mappings().fetchall()
        for x in rows:
            if x["reference_id"] not in reference_ids_with_PMC:
                pmid = x["curie"].replace("PMID:", "")
                if pmid not in pmids:
                    pmids.append(pmid)

    return pmids


def get_pmid_to_reference_id_mapping(db_session: Session):
    pmid_to_reference_id = {}
    rows = db_session.execute(text("SELECT curie, reference_id FROM cross_reference WHERE curie_prefix = 'PMID' and "
                                   "is_obsolete = False")).mappings().fetchall()
    for x in rows:
        pmid = x["curie"].replace("PMID:", "")
        pmid_to_reference_id[pmid] = x["reference_id"]

    return pmid_to_reference_id


def sort_pmids(db_session: Session, pmids, mod_to_pmids):

    pmids_with_prefix = ", ".join(["'" + "PMID:" + x + "'" for x in pmids])

    rows = db_session.execute(text(f"SELECT m.abbreviation, cr.curie "
                                   f"FROM   mod m, cross_reference cr, mod_corpus_association mca "
                                   f"WHERE  cr.curie in ({pmids_with_prefix}) "
                                   f"AND    cr.reference_id = mca.reference_id "
                                   f"AND    mca.corpus is True "
                                   f"AND    mca.mod_id = m.mod_id")).mappings().fetchall()
    pmids_in_mod = set()
    for x in rows:
        pmid = x["curie"].replace("PMID:", "")
        mod = x["abbreviation"]
        pmids_in_mod.add(pmid)
        if mod not in mod_to_pmids:
            mod_to_pmids[mod] = set()
        mod_to_pmids[mod].add(pmid)
    if 'NONE' not in mod_to_pmids:
        mod_to_pmids['NONE'] = set()
    for pmid in pmids:
        if pmid not in pmids_in_mod:
            mod_to_pmids['NONE'].add(pmid)

    return mod_to_pmids


def get_mod_papers(db_session: Session, mod):

    rows = db_session.execute(text(f"SELECT cr.curie, mca.corpus "
                                   f"FROM   cross_reference cr, mod_corpus_association mca, mod m "
                                   f"WHERE  cr.curie_prefix = 'PMID' "
                                   f"AND    cr.reference_id = mca.reference_id "
                                   f"AND    mca.mod_id = m.mod_id "
                                   f"AND    m.abbreviation = '{mod}'")).mappings().fetchall()
    in_corpus_set = set()
    out_corpus_set = set()
    for x in rows:
        pmid = x['curie'].replace('PMID:', '')
        if x['corpus'] is True:
            in_corpus_set.add(pmid)
        else:
            out_corpus_set.add(pmid)
    return in_corpus_set, out_corpus_set
