from sqlalchemy import or_
from fastapi.encoders import jsonable_encoder
from os import environ, makedirs, path
from typing import Dict, Tuple, Union
from datetime import datetime, timedelta
import json

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.api.models import ReferenceModel, AuthorModel, \
    CrossReferenceModel, ModCorpusAssociationModel, ModReferenceTypeModel, \
    ModModel, ReferenceCommentAndCorrectionModel, ResourceModel

batch_size_for_commit = 250


def add_cross_references(cross_references_to_add, ref_curie_list, logger, live_change=True):

    if len(ref_curie_list) == 0:
        return

    db_session = create_postgres_session(False)

    ref_curies = ", ".join(["'" + x + "'" for x in ref_curie_list])
    rs = db_session.execute("SELECT reference_id, curie FROM reference WHERE curie IN (" + ref_curies + ")")
    rows = rs.fetchall()
    curie_to_reference_id = {}
    for x in rows:
        curie_to_reference_id[x[1]] = x[0]

    i = 0
    j = 0
    for entry in cross_references_to_add:
        i += 1
        j += 1
        logger.info("Adding cross_reference #%s out of %s ", j, len(cross_references_to_add))
        if i > batch_size_for_commit:
            i = 0
            if live_change:
                db_session.commit()
            else:
                db_session.rollback()

        reference_id = curie_to_reference_id.get(entry['reference_curie'])
        if reference_id is None:
            # it won't happen, but still check
            continue

        rs = db_session.execute("SELECT reference_id, resource_id, is_obsolete FROM cross_reference WHERE curie = '" + entry["curie"] + "'")
        rows = rs.fetchall()
        if len(rows) > 0:
            for x in rows:
                logger.info("The XREF: " + entry["curie"] + " is already in cross_reference table. It is associated with reference_id = " + str(x[0]) + ", resource_id = " + str(x[1]) + ", is_obsolete=" + str(x[2]))
            continue

        try:
            x = CrossReferenceModel(reference_id=reference_id,
                                    curie=entry["curie"],
                                    pages=entry.get("pages"))
            db_session.add(x)
            logger.info("The cross_reference row for reference_id = " + str(reference_id) + " and curie = " + entry["curie"] + " has been added into database.")
        except Exception as e:
            logger.info("An error occurred when adding cross_reference row for reference_id = " + str(reference_id) + " and curie = " + entry["curie"] + " " + str(e))

    if live_change:
        db_session.commit()
    else:
        db_session.rollback()
    db_session.close()


def update_authors(db_session, reference_id, author_list_in_db, author_list_in_json, logger):

    if len(author_list_in_json) == 0:
        return

    authors_in_db = []
    author_list_with_first_or_corresponding_author = []
    if author_list_in_db:
        for x in author_list_in_db:
            if x['first_author'] or x['corresponding_author']:
                author_list_with_first_or_corresponding_author.append((x['name'], "first_author = " + str(x['first_author']), "corresponding_author = " + str(x['corresponding_author'])))
            affiliations = x['affiliations'] if x['affiliations'] else []
            orcid = x['orcid'] if x['orcid'] else ''
            authors_in_db.append((x['name'], x['first_name'], x['last_name'], x['order'], '|'.join(affiliations), orcid))

    authors_in_json = []
    noAuthorRankInJson = False
    if len(author_list_in_json) > 0 and 'authorRank' not in author_list_in_json[0]:
        noAuthorRankInJson = True
        # will use the order in the author list
    i = 0
    for x in author_list_in_json:
        orcid = 'ORCID:' + x['orcid'] if x.get('orcid') else ''
        affiliations = x['affiliations'] if x.get('affiliations') else []
        # if x.get('authorRank') is None:
        #    logger.info("The authors in json record for REFERENCE_ID:" + str(reference_id) + " has no authorRank")
        #    return
        i += 1
        authorRank = x.get('authorRank')
        if noAuthorRankInJson is True:
            authorRank = i
        authors_in_json.append((x.get('name', ''), x.get('firstName', ''), x.get('lastName', ''), authorRank, '|'.join(affiliations), orcid))

    if set(authors_in_db) == set(authors_in_json):
        return []

    if len(author_list_with_first_or_corresponding_author) > 0:
        logger.info("One of authors for reference_id = " + str(reference_id) + " is first_author or corresponding_author.")
        logger.info(str(author_list_with_first_or_corresponding_author))
        return

    ## deleting authors from database for the given REFERENCE_ID
    for x in db_session.query(AuthorModel).filter_by(reference_id=reference_id).order_by(AuthorModel.order).all():
        name = x.name
        affiliations = x.affiliations if x.affiliations else []
        try:
            db_session.delete(x)
            logger.info("REFERENCE_ID:" + str(reference_id) + ": DELETE AUTHOR: " + name + " | '" + '|'.join(affiliations) + "'")
        except Exception as e:
            logger.info("REFERENCE_ID:" + str(reference_id) + ": DELETE AUTHOR: " + name + " failed: " + str(e))

    ## adding authors from pubmed into database

    for x in authors_in_json:
        (name, firstname, lastname, authorRank, affiliations, orcid) = x
        affiliation_list = affiliations.split('|')
        if len(affiliation_list) == 0 or (len(affiliation_list) == 1 and affiliation_list[0] == ''):
            affiliation_list = None
        data = {"reference_id": reference_id,
                "name": name,
                "first_name": firstname,
                "last_name": lastname,
                "order": authorRank,
                "affiliations": affiliation_list,
                "orcid": orcid if orcid else None,
                "first_author": False,
                "corresponding_author": False}

        try:
            x = AuthorModel(**data)
            db_session.add(x)
            logger.info("REFERENCE_ID:" + str(reference_id) + ": INSERT AUTHOR: " + name + " | '" + affiliations + "'")
        except Exception as e:
            logger.info("REFERENCE_ID:" + str(reference_id) + ": INSERT AUTHOR: " + name + " failed: " + str(e))


def update_mod_corpus_associations(db_session, mod_to_mod_id, reference_id, mod_corpus_association_db, mod_corpus_association_json, logger):

    db_mod_corpus_association = {}
    for db_mca_entry in mod_corpus_association_db:
        if db_mca_entry.get('mod') is None or db_mca_entry['mod'].get('abbreviation') is None:
            continue
        mod = db_mca_entry['mod']['abbreviation']
        if mod not in db_mod_corpus_association:
            db_mod_corpus_association[mod] = {}
        db_mod_corpus_association[mod]['id'] = db_mca_entry['mod_corpus_association_id']
        db_mod_corpus_association[mod]['corpus'] = db_mca_entry['corpus']

    for json_mca_entry in mod_corpus_association_json:
        if json_mca_entry.get('mod_abbreviation') is None:
            continue
        mod = json_mca_entry['mod_abbreviation']
        if mod not in db_mod_corpus_association:
            logger.info(json_mca_entry)
            try:
                x = ModCorpusAssociationModel(reference_id=reference_id,
                                              mod_id=mod_to_mod_id[mod],
                                              corpus=json_mca_entry['corpus'],
                                              mod_corpus_sort_source=json_mca_entry['mod_corpus_sort_source'])
                db_session.add(x)
                logger.info("The mod_corpus_association row for reference_id = " + str(reference_id) + " and mod = " + mod + " has been added into database.")
            except Exception as e:
                logger.info("An error occurred when adding mod_corpus_association row for reference_id = " + str(reference_id) + " and mod = " + mod + ". " + str(e))

        elif json_mca_entry['corpus'] != db_mod_corpus_association[mod]['corpus']:
            mod_corpus_association_id = db_mod_corpus_association[mod]['id']
            try:
                db_session.query(ModCorpusAssociationModel).filter_by(mod_corpus_association_id=mod_corpus_association_id).update({"mod_corpus_sort_source": json_mca_entry['mod_corpus_sort_source'], "corpus": json_mca_entry['corpus']})
                logger.info("The mod_corpus_association row for mod_corpus_association_id = " + str(mod_corpus_association_id) + " has been updated in the database.")
            except Exception as e:
                logger.info("An error occurred when updating mod_corpus_association row for mod_corpus_association_id = " + str(mod_corpus_association_id) + " " + str(e))


def update_mod_reference_types(db_session, reference_id, db_mod_ref_types, json_mod_ref_types, logger):  # noqa: C901

    db_mrt_data = {}
    to_delete_duplicate_rows = []
    for mrt in db_mod_ref_types:
        source = mrt['source']
        ref_type = mrt['reference_type']
        mrt_id = mrt['mod_reference_type_id']
        if source not in db_mrt_data:
            db_mrt_data[source] = {}
        if ref_type not in db_mrt_data[source]:
            db_mrt_data[source][ref_type] = mrt_id
        else:
            to_delete_duplicate_rows.append((mrt_id, ref_type))

    json_mrt_data = dict()
    for mrt in json_mod_ref_types:
        source = mrt['source']
        ref_type = mrt['referenceType']
        if source not in json_mrt_data:
            json_mrt_data[source] = []
        # just in case there is any duplicate in json
        if ref_type not in json_mrt_data[source]:
            json_mrt_data[source].append(ref_type)

    for mod in json_mrt_data:
        lc_json = [x.lower() for x in json_mrt_data[mod]]
        lc_db = []
        if mod in db_mrt_data:
            lc_db = [x.lower() for x in db_mrt_data[mod].keys()]
        for ref_type in json_mrt_data[mod]:
            if ref_type.lower() not in lc_db:
                try:
                    x = ModReferenceTypeModel(reference_id=reference_id,
                                              reference_type=ref_type,
                                              source=mod)
                    db_session.add(x)
                    logger.info("The mod_reference_type for reference_id = " + str(reference_id) + " has been added into the database.")
                except Exception as e:
                    logger.info("An error occurred when adding mod_reference_type row for reference_id = " + str(reference_id) + " has been a\
dded into the database. " + str(e))

        if len(lc_db) == 0:
            continue
        for ref_type in db_mrt_data[mod]:
            if ref_type.lower() not in lc_json:
                mod_reference_type_id = db_mrt_data[mod][ref_type]
                if mod_reference_type_id not in to_delete_duplicate_rows:
                    to_delete_duplicate_rows.append((mod_reference_type_id, ref_type))

    for row in to_delete_duplicate_rows:
        (mod_reference_type_id, ref_type) = row
        try:
            x = db_session.query(ModReferenceTypeModel).filter_by(
                mod_reference_type_id=mod_reference_type_id).one_or_none()
            if x:
                db_session.delete(x)
                logger.info("The mod_reference_type for mod_reference_type_id = " + str(mod_reference_type_id) + " has been deleted from the database.")
        except Exception as e:
            logger.info("An error occurred when deleting mod_reference_type row for mod_reference_type_id = " + str(mod_reference_type_id) + " has been deleted from the database. " + str(e))


def add_mca_to_existing_references(db_session, agr_curies_to_corpus, mod, logger):

    m = db_session.query(ModModel).filter_by(abbreviation=mod).one_or_none()
    mod_id = m.mod_id

    curie_to_reference_id = {}
    for x in db_session.query(ReferenceModel).filter(
            ReferenceModel.curie.in_(agr_curies_to_corpus)).all():
        curie_to_reference_id[x.curie] = x.reference_id

    for curie in agr_curies_to_corpus:
        try:
            reference_id = curie_to_reference_id[x.curie]
            mca = db_session.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).all()
            if len(mca) > 0:
                continue
            mca = ModCorpusAssociationModel(reference_id=reference_id,
                                            mod_id=mod_id,
                                            mod_corpus_sort_source='mod_pubmed_search',
                                            corpus=None)
            db_session.add(mca)
            logger.info("The mod_corpus_association row has been added into database for mod = " + mod + ", reference_curie = " + curie)
        except Exception as e:
            logger.info("An error occurred when adding mod_corpus_association for mod = " + mod + ", reference_curie = " + curie + ". error = " + str(e))

    db_session.commit()


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


def check_handle_duplicate(db_session, mod, pmids, xref_ref, ref_xref_valid, ref_xref_obsolete, logger):

    # check for papers with same doi in the database
    # print ("ref_xref_valid=", str(ref_xref_valid['AGR:AGR-Reference-0000167781']))
    # ref_xref_valid= {'DOI': '10.1111/j.1440-1711.2005.01311.x', 'MGI': '3573820', 'PMID': '15748210'}
    # print ("xref_ref['DOI'][doi]=", str(xref_ref['DOI']['10.1111/j.1440-1711.2005.01311.x']))
    # xref_ref['DOI'][doi]= AGR:AGR-Reference-0000167781

    from datetime import datetime
    base_path = environ.get('XML', '')
    json_path = base_path + "pubmed_json/"
    log_path = base_path + 'pubmed_search_logs/'
    log_url = None
    if environ.get('LOG_PATH'):
        log_path = path.join(environ['LOG_PATH'], 'pubmed_search/')
        if environ.get('LOG_URL'):
            log_url = path.join(environ['LOG_URL'], 'pubmed_search/')
    if not path.exists(log_path):
        makedirs(log_path)
    log_file = log_path + "duplicate_rows_" + mod + ".log"
    fw = None
    if path.exists(log_file):
        fw = open(log_file, "a")
    else:
        fw = open(log_file, "w")
    not_loaded_pmids = []
    for pmid in pmids:
        json_file = json_path + pmid + ".json"
        if not path.exists(json_file):
            continue
        f = open(json_file)
        json_data = json.load(f)
        f.close()
        cross_references = json_data['crossReferences']
        doi = None
        for c in cross_references:
            if c['id'].startswith('DOI:'):
                doi = c['id'].replace('DOI:', '')
        if doi and doi in xref_ref['DOI']:
            ## the doi for the new paper is in the database
            agr = xref_ref['DOI'][doi]
            all_ref_xref = ref_xref_valid[agr] if agr in ref_xref_valid else {}
            if agr in ref_xref_obsolete:
                # merge two dictionaries
                all_ref_xref.update(ref_xref_obsolete[agr])
            found_pmids_for_this_doi = []
            for prefix in all_ref_xref:
                if prefix == 'PMID':
                    found_pmids_for_this_doi.append(all_ref_xref[prefix])
            if len(found_pmids_for_this_doi) == 0:
                reference_id = get_reference_id_by_curie(db_session, agr)
                if reference_id is None:
                    logger.info("The reference curie: " + agr + " is not in the database.")
                try:
                    cross_ref = CrossReferenceModel(curie="PMID:" + pmid, reference_id=reference_id)
                    db_session.add(cross_ref)
                    fw.write(str(datetime.now()) + ": adding PMID:" + pmid + " to the row with doi = " + doi + " in the database\n")
                except Exception as e:
                    logger.info(str(datetime.now()) + ": adding " + pmid + " to the row with " + doi + " is failed: " + str(e) + "\n")
            else:
                fw.write(str(datetime.now()) + ": " + doi + " for PMID:" + pmid + " is associated with PMID(s) in the database: " + ",".join(found_pmids_for_this_doi) + "\n")
                not_loaded_pmids.append((pmid, doi, ",".join(found_pmids_for_this_doi)))
            pmids.remove(pmid)
    fw.close()

    return (log_path, log_url, not_loaded_pmids)


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
