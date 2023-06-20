from os import environ, makedirs, path
import json
from sqlalchemy import or_

from agr_literature_service.api.crud.mod_reference_type_crud import insert_mod_reference_type_into_db
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_reference_id_by_curie, get_reference_id_by_pmid
from agr_literature_service.api.models import ReferenceModel, AuthorModel, \
    CrossReferenceModel, ModCorpusAssociationModel, ModModel, ReferenceCommentAndCorrectionModel, \
    MeshDetailModel, ReferenceModReferencetypeAssociationModel, \
    ReferencefileModel, ReferencefileModAssociationModel

batch_size_for_commit = 250


def move_mod_papers_into_corpus(db_session, mod, mod_id, mod_reference_id_set, logger=None):

    try:
        for x in db_session.query(ModCorpusAssociationModel).filter_by(
                mod_id=mod_id).filter(
                    or_(ModCorpusAssociationModel.corpus.is_(False),
                        ModCorpusAssociationModel.corpus.is_(None))).all():
            if x.reference_id in mod_reference_id_set:
                x.corpus = True
                if x.mod_corpus_sort_source != 'dqm_files':
                    x.mod_corpus_sort_source = 'dqm_files'
                db_session.add(x)
                if logger:
                    logger.info(f"Moving {mod} paper into {mod} corpus for reference_id = {x.reference_id}")
    except Exception as e:
        if logger:
            logger.info(f"An error occurred when moving {mod} paper(s) in/out {mod} corpus. error={e}")

    db_session.commit()
    # db_session.rollback()


def change_mod_curie_status(db_session, mod, mod_curie_set, logger=None):

    curie_prefix = mod
    if mod == 'XB':
        curie_prefix = "Xenbase"
    mod_reference_id_set = set()
    mod_curie_set_in_db = set()
    try:
        for x in db_session.query(CrossReferenceModel).filter_by(
                curie_prefix=curie_prefix).all():
            if x.reference_id is None:
                continue
            if x.curie in mod_curie_set:
                mod_reference_id_set.add(x.reference_id)
                mod_curie_set_in_db.add(x.curie)
                if x.is_obsolete is True:
                    x.is_obsolete = False
                    db_session.add(x)
                    if logger:
                        logger.info(f"Changing {mod} curie to valid for {x.curie}")
            elif x.is_obsolete is False:
                x.is_obsolete = True
                db_session.add(x)
                if logger:
                    logger.info(f"Changing {mod} curie to obsolete for {x.curie}")
    except Exception as e:
        if logger:
            logger.info(f"An error occurred when changing is_obsolete for {mod} curie. error={e}")

    db_session.commit()
    # db_session.rollback()

    # get mod curies that are not loaded into the database:
    mod_curies_not_in_db = mod_curie_set - mod_curie_set_in_db
    if logger:
        logger.info(f"{mod} curies that are not loaded into the database: {mod_curies_not_in_db}")

    mod_id = _get_mod_id_by_mod(db_session, mod)

    move_mod_papers_into_corpus(db_session, mod, mod_id, mod_reference_id_set, logger)
    move_obsolete_papers_out_of_corpus(db_session, mod, mod_id, curie_prefix, logger)


def move_obsolete_papers_out_of_corpus(db_session, mod, mod_id, curie_prefix, logger=None):

    rows = db_session.execute(f"SELECT mca.mod_corpus_association_id, cr.reference_id "
                              f"FROM mod_corpus_association mca, cross_reference cr "
                              f"WHERE mca.mod_id = {mod_id} "
                              f"AND mca.corpus is True "
                              f"AND mca.reference_id = cr.reference_id "
                              f"AND cr.curie_prefix = '{curie_prefix}' "
                              f"AND cr.is_obsolete is True").fetchall()

    for x in rows:
        positiveModCurie = db_session.execute(f"SELECT curie "
                                              f"FROM cross_reference "
                                              f"WHERE reference_id = {x[1]} "
                                              f"AND curie_prefix = '{curie_prefix}' "
                                              f"AND is_obsolete is False").fetchall()
        if len(positiveModCurie) > 0:
            # a paper has both valid and invalid MOD curies
            continue
        # move the papers outside corpus if they only have invalid MOD curies
        try:
            db_session.execute(f"UPDATE mod_corpus_association "
                               f"SET corpus = False "
                               f"WHERE mod_corpus_association_id = {int(x[0])}")
            if logger:
                logger.info(f"Moving {mod} paper out of corpus for mod_corpus_association_id = {x[0]}")
        except Exception as e:
            if logger:
                logger.info(f"An error occurred when moving {mod} paper out of corpus for mod_corpus_association_id = {x[0]}. Error = {e}")

    db_session.commit()
    # db_session.rollback()


def mark_not_in_mod_papers_as_out_of_corpus(mod, missing_papers_in_mod, logger=None):

    db_session = create_postgres_session(False)

    mod_id = _get_mod_id_by_mod(db_session, mod)

    # xref_id: MOD database ID, eg. SGD:00000000005
    for (xref_id, agr, pmid) in missing_papers_in_mod:
        try:
            cr = db_session.query(CrossReferenceModel).filter_by(curie=xref_id, is_obsolete=False).one_or_none()
            if cr:
                cr.is_obsolete = True
                db_session.add(cr)
                mca = db_session.query(ModCorpusAssociationModel).filter_by(reference_id=cr.reference_id, mod_id=mod_id).one_or_none()
                if mca:
                    mca.corpus = False
                    db_session.add(mca)
                    print(mod + ": marking", xref_id, "out of corpus")
                    if logger:
                        logger.info(mod + ": marking " + xref_id + "(AGR curie=" + agr + ", PMID=" + str(pmid) + ") as out of corpus")
        except Exception as e:
            if logger:
                logger.info("An error occurred when marking paper " + xref_id + "as out of corpus. error=" + str(e))

    db_session.commit()
    # db_session.rollback()


def mark_false_positive_papers_as_out_of_corpus(db_session, mod, fp_pmids, logger=None):  # noqa: C901

    mod_id = _get_mod_id_by_mod(db_session, mod)

    rows = db_session.execute(f"SELECT cr.reference_id, cr.curie "
                              f"FROM cross_reference cr, mod_corpus_association mca "
                              f"WHERE cr.curie_prefix = 'PMID' "
                              f"AND cr.reference_id = mca.reference_id "
                              f"AND mca.mod_id = {mod_id}").fetchall()

    to_unlink_reference_id_list = []
    i = 0
    for x in rows:
        reference_id = x[0]
        pmid = x[1].replace("PMID:", "")
        i += 1
        if i % 1000 == 0 and logger:
            logger.info(str(i) + " Retrieving PMIDs from the database...")
        if pmid in fp_pmids:
            to_unlink_reference_id_list.append((pmid, reference_id))

    i = 0
    for (pmid, reference_id) in to_unlink_reference_id_list:
        x = db_session.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).one_or_none()
        if x and x.corpus is not False:
            i += 1
            if i % 300 == 0:
                db_session.commit()
            try:
                x.corpus = False
                db_session.add(x)
                print("PMID:" + pmid + " has been marked as out of corpus for " + mod)
                if logger:
                    logger.info("PMID:" + pmid + " has been marked as out of corpus for " + mod)
            except Exception as e:
                if logger:
                    logger.info("An error occurred when marking PMID:" + pmid + " as out of corpus for " + mod + ". error = " + str(e))

    db_session.commit()

    curie_prefix = mod
    if mod == 'XB':
        curie_prefix = 'Xenbase'

    for (pmid, reference_id) in to_unlink_reference_id_list:
        x = db_session.query(CrossReferenceModel).filter_by(reference_id=reference_id, curie_prefix=curie_prefix, is_obsolete=False).one_or_none()
        if x:
            i += 1
            if i % 300 == 0:
                db_session.commit()
            try:
                x.is_obsolete = True
                db_session.add(x)
                print(mod + ": PMID:" + pmid + " set " + x.curie + " to obsolete")
                if logger:
                    logger.info(mod + ": PMID:" + pmid + " set " + x.curie + " to obsolete")
            except Exception as e:
                if logger:
                    logger.info(mod + ": An error occurred when setting PMID:" + pmid + " " + x.curie + " to obsolete. error = " + str(e))

    db_session.commit()


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
    just_added = set()
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

        if entry["curie"] in just_added:
            logger.info("The XREF: " + entry["curie"] + " is a DUPLICATE one, just added to cross_reference table.")
            continue
        just_added.add(entry["curie"])

        rs = db_session.execute("SELECT reference_id, resource_id, is_obsolete FROM cross_reference WHERE curie = '" + entry["curie"] + "'")
        rows = rs.fetchall()
        if len(rows) > 0:
            for x in rows:
                logger.info("The XREF: " + entry["curie"] + " is already in cross_reference table. It is associated with reference_id = " + str(x[0]) + ", resource_id = " + str(x[1]) + ", is_obsolete=" + str(x[2]))
            continue

        try:
            x = CrossReferenceModel(reference_id=reference_id,
                                    curie_prefix=entry["curie"].split(':')[0],
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


def _write_log_message(reference_id, log_message, pmid, logger, fw):  # pragma: no cover

    if pmid:
        log_message = "PMID:" + str(pmid) + log_message
    else:
        log_message = "REFERENCE_ID:" + str(reference_id) + log_message
    if logger:
        logger.info(log_message)
    elif fw:
        fw.write(log_message + "\n")


def update_authors(db_session, reference_id, author_list_in_db, author_list_in_json, logger=None, fw=None, pmid=None, update_log=None):  # noqa: C901
    # If any of the author fields are different, check if any of our ABC authors
    # have a different corresponding_author or first_author, in which case do not
    # update the authors at all (in theory, eventually send a message to a curator,
    # but for now don't do that).  If the first/corr flags have not been changed,
    # then get rid of all the ABC authors, and repopulate them from the PubMed data

    if author_list_in_json is None or len(author_list_in_json) == 0:
        return []

    authors_in_db = []
    author_list_with_first_or_corresponding_author = []
    if author_list_in_db:
        for x in author_list_in_db:
            if x['first_author'] or x['corresponding_author']:
                id = "REFERENCE_ID:" + str(reference_id)
                if pmid is not None:
                    id = "PMID:" + str(pmid)
                author_list_with_first_or_corresponding_author.append((id, x['name'], "first_author = " + str(x['first_author']), "corresponding_author = " + str(x['corresponding_author'])))
            affiliations = x['affiliations'] if x['affiliations'] else []
            orcid = x['orcid'] if x['orcid'] else ''
            authors_in_db.append((x['name'], x['first_name'], x['last_name'], x['first_initial'], x['order'], '|'.join(affiliations), orcid))

    authors_in_json = []
    noAuthorRankInJson = False
    if len(author_list_in_json) > 0 and 'authorRank' not in author_list_in_json[0]:
        noAuthorRankInJson = True
        # will use the order in the author list
    i = 0
    for x in author_list_in_json:
        orcid = 'ORCID:' + x['orcid'] if x.get('orcid') else ''
        affiliations = x['affiliations'] if x.get('affiliations') else []
        i += 1
        authorRank = x.get('authorRank')
        if noAuthorRankInJson is True:
            authorRank = i
        firstName = x['firstname'] if 'firstname' in x else x.get('firstName', '')
        lastName = x['lastname'] if 'lastname' in x else x.get('lastName', '')
        firstInitial = x['firstinit'] if 'firstinit' in x else x.get('firstInit', '')
        authors_in_json.append((x.get('name', ''), firstName, lastName, firstInitial, authorRank, '|'.join(affiliations), orcid))

    if set(authors_in_db) == set(authors_in_json):
        return []

    if len(author_list_with_first_or_corresponding_author) > 0:
        if logger:
            logger.info("One of authors for reference_id = " + str(reference_id) + " is first_author or corresponding_author.")
            logger.info(str(author_list_with_first_or_corresponding_author))
        return author_list_with_first_or_corresponding_author

    if update_log:
        update_log['author_name'] = update_log['author_name'] + 1
        update_log['pmids_updated'].append(pmid)

    ## deleting authors from database for the given REFERENCE_ID
    for x in db_session.query(AuthorModel).filter_by(reference_id=reference_id).order_by(AuthorModel.order).all():
        name = x.name
        affiliations = x.affiliations if x.affiliations else []
        try:
            db_session.delete(x)
            log_message = ": DELETE AUTHOR: " + name + " | '" + '|'.join(affiliations) + "'"
            _write_log_message(reference_id, log_message, pmid, logger, fw)
        except Exception as e:
            log_message = ": DELETE AUTHOR: " + name + " failed: " + str(e)
            _write_log_message(reference_id, log_message, pmid, logger, fw)

    # 3.5% authors in the database have orcid
    ## adding authors from pubmed into database
    for x in authors_in_json:
        (name, firstname, lastname, firstInitial, authorRank, affiliations, orcid) = x
        affiliation_list = affiliations.split('|')
        if len(affiliation_list) == 0 or (len(affiliation_list) == 1 and affiliation_list[0] == ''):
            affiliation_list = None
        data = {"reference_id": reference_id,
                "name": name,
                "first_name": firstname,
                "last_name": lastname,
                "first_initial": firstInitial,
                "order": authorRank,
                "affiliations": affiliation_list,
                "orcid": orcid if orcid else None,
                "first_author": False,
                "corresponding_author": False}

        try:
            a = AuthorModel(**data)
            db_session.add(a)
            log_message = ": INSERT AUTHOR: " + name + " | '" + affiliations + "'"
            _write_log_message(reference_id, log_message, pmid, logger, fw)
        except Exception as e:
            log_message = ": INSERT AUTHOR: " + name + " failed: " + str(e)
            _write_log_message(reference_id, log_message, pmid, logger, fw)

    return []


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


def update_mod_reference_types(db_session, reference_id, db_mod_ref_types, json_mod_ref_types, pubmed_types, logger):  # noqa: C901

    db_mrt_data = {}
    to_delete_duplicate_rows = []
    for mrt in db_mod_ref_types:
        source = mrt['mod_referencetype']['mod']['abbreviation']
        ref_type = mrt['mod_referencetype']['referencetype']['label']
        mrt_id = mrt['reference_mod_referencetype_id']
        if source not in db_mrt_data:
            db_mrt_data[source] = {}
        if ref_type not in db_mrt_data[source]:
            db_mrt_data[source][ref_type] = mrt_id
        else:
            to_delete_duplicate_rows.append((mrt_id, ref_type))

    json_mrt_data = dict()
    for mrt in json_mod_ref_types:
        source = mrt['source']
        ref_type_label = mrt['referenceType']
        if source not in json_mrt_data:
            json_mrt_data[source] = []
        # just in case there is any duplicate in json
        if ref_type_label not in json_mrt_data[source]:
            json_mrt_data[source].append(ref_type_label)

    for mod in json_mrt_data:
        lc_json = [x.lower() for x in json_mrt_data[mod]]
        lc_db = []
        if mod in db_mrt_data:
            lc_db = {x.lower() for x in db_mrt_data[mod].keys()}
        for ref_type_label in json_mrt_data[mod]:
            if ref_type_label.lower() not in lc_db:
                try:
                    insert_mod_reference_type_into_db(db_session, pubmed_types, mod, ref_type_label, reference_id)
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
            x = db_session.query(ReferenceModReferencetypeAssociationModel).filter_by(
                reference_mod_referencetype_id=mod_reference_type_id).one_or_none()
            if x:
                db_session.delete(x)
                logger.info("The mod_reference_type for mod_reference_type_id = " + str(mod_reference_type_id) + " has been deleted from the database.")
        except Exception as e:
            logger.info("An error occurred when deleting mod_reference_type row for mod_reference_type_id = " + str(mod_reference_type_id) + " has been deleted from the database. " + str(e))


def _get_mod_id_by_mod(db_session, mod):  # pragma: no cover

    m = db_session.query(ModModel).filter_by(abbreviation=mod).one_or_none()

    return m.mod_id


def add_mca_to_existing_references(db_session, agr_curies_to_corpus, mod, logger):

    mod_id = _get_mod_id_by_mod(db_session, mod)

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


def check_handle_duplicate(db_session, mod, pmids, xref_ref, ref_xref_valid, ref_xref_obsolete, logger):  # noqa: C901 # pragma: no cover

    # check for papers with same doi in the database
    # print ("ref_xref_valid=", str(ref_xref_valid['AGR:AGR-Reference-0000167781']))
    # ref_xref_valid= {'DOI': '10.1111/j.1440-1711.2005.01311.x', 'MGI': '3573820', 'PMID': '15748210'}
    # print ("xref_ref['DOI'][doi]=", str(xref_ref['DOI']['10.1111/j.1440-1711.2005.01311.x']))
    # xref_ref['DOI'][doi]= AGR:AGR-Reference-0000167781

    from datetime import datetime
    base_path = environ.get('XML_PATH', '')
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
                    if type(all_ref_xref[prefix]) is set:
                        for x in all_ref_xref[prefix]:
                            found_pmids_for_this_doi.append(x)
                    else:
                        found_pmids_for_this_doi.append(all_ref_xref[prefix])
            if len(found_pmids_for_this_doi) == 0:
                reference_id = get_reference_id_by_curie(db_session, agr)
                if reference_id is None:
                    logger.info("The reference curie: " + agr + " is not in the database.")
                try:
                    cross_ref = CrossReferenceModel(curie="PMID:" + pmid,
                                                    curie_prefix='PMID',
                                                    reference_id=reference_id)
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


def _insert_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type):  # pragma: no cover

    ## check to see if any newly added ones matches this entry
    rows = db_session.query(ReferenceCommentAndCorrectionModel).filter_by(reference_id_from=reference_id_from, reference_id_to=reference_id_to).all()
    if len(rows) > 0:
        return

    data = {"reference_id_from": reference_id_from,
            "reference_id_to": reference_id_to,
            "reference_comment_and_correction_type": type}
    try:
        x = ReferenceCommentAndCorrectionModel(**data)
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": INSERT CommentsAndCorrections: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": INSERT CommentsAndCorrections: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + " failed: " + str(e) + "\n")


def _update_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type):  # pragma: no cover

    all = db_session.query(ReferenceCommentAndCorrectionModel).filter_by(reference_id_from=reference_id_from, reference_id_to=reference_id_to).all()

    if len(all) == 0:
        return

    for x in all:
        db_session.delete(x)

    _insert_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type)


def _delete_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type):  # pragma: no cover

    for x in db_session.query(ReferenceCommentAndCorrectionModel).filter_by(reference_id_from=reference_id_from, reference_id_to=reference_id_to, reference_comment_and_correction_type=type).all():
        try:
            db_session.delete(x)
            fw.write("PMID:" + str(pmid) + ": DELETE CommentsAndCorrections: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + "\n")
        except Exception as e:
            fw.write("PMID:" + str(pmid) + ": DELETE CommentsAndCorrections: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + " failed: " + str(e) + "\n")


def update_comment_corrections(db_session, fw, pmid, reference_id, pmid_to_reference_id, reference_ids_to_comment_correction_type, comment_correction_in_json, update_log):

    if comment_correction_in_json is None or str(comment_correction_in_json) == '{}':
        return

    type_mapping = {'ErratumIn': 'ErratumFor',
                    'RepublishedIn': 'RepublishedFrom',
                    'RetractionIn': 'RetractionOf',
                    'ExpressionOfConcernIn': 'ExpressionOfConcernFor',
                    'ReprintIn': 'ReprintOf',
                    'UpdateIn': 'UpdateOf'}

    new_reference_ids_to_comment_correction_type = {}
    for type in comment_correction_in_json:
        other_pmids = comment_correction_in_json[type]
        other_reference_ids = []
        for this_pmid in other_pmids:
            other_reference_id = pmid_to_reference_id.get(this_pmid)
            if other_reference_id is None:
                other_reference_id = get_reference_id_by_pmid(db_session, this_pmid)
                if other_reference_id is None:
                    continue
            other_reference_ids.append(other_reference_id)
        if len(other_reference_ids) == 0:
            continue
        if type.endswith('For') or type.endswith('From') or type.endswith('Of'):
            reference_id_from = reference_id
            for reference_id_to in other_reference_ids:
                new_reference_ids_to_comment_correction_type[(reference_id_from, reference_id_to)] = type
        else:
            type = type_mapping.get(type)
            if type is None:
                continue
            reference_id_to = reference_id
            for reference_id_from in other_reference_ids:
                new_reference_ids_to_comment_correction_type[(reference_id_from, reference_id_to)] = type

    if len(new_reference_ids_to_comment_correction_type.keys()) == 0:
        return

    for key in new_reference_ids_to_comment_correction_type:
        if key in reference_ids_to_comment_correction_type:
            if reference_ids_to_comment_correction_type[key] == new_reference_ids_to_comment_correction_type[key]:
                continue
            (reference_id_from, reference_id_to) = key
            _update_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type)
            update_log['comment_erratum'] = update_log['comment_erratum'] + 1
            update_log['pmids_updated'].append(pmid)
        else:
            _insert_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type)
            update_log['comment_erratum'] = update_log['comment_erratum'] + 1
            update_log['pmids_updated'].append(pmid)

    for key in reference_ids_to_comment_correction_type:
        if key in new_reference_ids_to_comment_correction_type:
            continue
        (reference_id_from, reference_id_to) = key
        if reference_id in [reference_id_from, reference_id_to]:
            ## only remove the ones that are associated with given PMIDs
            _delete_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type)
            update_log['comment_erratum'] = update_log['comment_erratum'] + 1
            update_log['pmids_updated'].append(pmid)


def _insert_mesh_term(db_session, fw, pmid, reference_id, terms):  # pragma: no cover

    (heading_term, qualifier_term) = terms

    if qualifier_term == '':
        qualifier_term = None

    data = {'reference_id': reference_id, 'heading_term': heading_term, 'qualifier_term': qualifier_term}
    try:
        x = MeshDetailModel(**data)
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": INSERT mesh term: " + str(terms) + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": INSERT mesh term: " + str(terms) + " failed: " + str(e) + "\n")


def _delete_mesh_term(db_session, fw, pmid, reference_id, terms):  # pragma: no cover

    (heading_term, qualifier_term) = terms

    try:
        x = None
        if qualifier_term != '':
            x = db_session.query(MeshDetailModel).filter_by(reference_id=reference_id, heading_term=heading_term, qualifier_term=qualifier_term).one_or_none()
        else:
            for m in db_session.query(MeshDetailModel).filter_by(reference_id=reference_id, heading_term=heading_term).all():
                if not m.qualifier_term:
                    x = m
        if x is None:
            return
        db_session.delete(x)
        fw.write("PMID:" + str(pmid) + ": DELETE mesh term " + str(terms) + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": DELETE mesh term: " + str(terms) + " failed: " + str(e) + "\n")


def update_mesh_terms(db_session, fw, pmid, reference_id, mesh_terms_in_db, mesh_terms_in_json_data, update_log):

    if mesh_terms_in_json_data is None:
        return

    mesh_terms_in_json = []

    for m in mesh_terms_in_json_data:
        heading_term = m.get('meshHeadingTerm')
        qualifier_term = m.get('meshQualifierTerm', '')
        if heading_term is None:
            continue
        mesh_terms_in_json.append((heading_term, qualifier_term))

    if mesh_terms_in_db is None:
        mesh_terms_in_db = []

    if len(mesh_terms_in_json) == 0 or set(mesh_terms_in_json) == set(mesh_terms_in_db):
        return

    for m in mesh_terms_in_json:
        if m in mesh_terms_in_db:
            continue
        else:
            _insert_mesh_term(db_session, fw, pmid, reference_id, m)

    for m in mesh_terms_in_db:
        if m in mesh_terms_in_json:
            continue
        else:
            _delete_mesh_term(db_session, fw, pmid, reference_id, m)

    update_log['mesh_term'] = update_log['mesh_term'] + 1
    update_log['pmids_updated'].append(pmid)


def _update_doi(db_session, fw, pmid, reference_id, old_doi, new_doi):  # pragma: no cover

    try:
        x = db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie == 'DOI:' + new_doi).all()
        if len(x) > 0:
            fw.write('DOI:' + new_doi + " is already in the database.\n")
            return
        x = db_session.query(CrossReferenceModel).filter_by(reference_id=reference_id, is_obsolete=False).filter(CrossReferenceModel.curie == 'DOI:' + old_doi).one_or_none()
        if x is None:
            return
        x.curie = "DOI:" + new_doi
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": UPDATE DOI from " + old_doi + " to " + new_doi + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": UPDATE DOI from " + old_doi + " to " + new_doi + " failed: " + str(e) + "\n")


def _insert_doi(db_session, fw, pmid, reference_id, doi, logger=None):  # pragma: no cover

    ## for some reason, we need to add this check to make sure it is not in db
    x = db_session.query(CrossReferenceModel).filter_by(curie="DOI:" + doi).one_or_none()
    if x:
        if x.reference_id != reference_id:
            if logger:
                logger.info("The DOI:" + doi + " is associated with two papers: reference_ids=" + str(reference_id) + ", " + str(x.reference_id))
        return

    data = {"curie": "DOI:" + doi,
            "curie_prefix": "DOI",
            "reference_id": reference_id,
            "is_obsolete": False}
    try:
        x = CrossReferenceModel(**data)
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": INSERT DOI:" + doi + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": INSERT DOI:" + doi + " failed: " + str(e) + "\n")


def _update_pmcid(db_session, fw, pmid, reference_id, old_pmcid, new_pmcid):  # pragma: no cover

    try:
        x = db_session.query(CrossReferenceModel).filter_by(reference_id=reference_id).filter(CrossReferenceModel.curie == 'PMCID:' + old_pmcid).one_or_none()
        if x is None:
            return
        x.curie = "PMCID:" + new_pmcid
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": UPDATE PMCID from " + old_pmcid + " to " + new_pmcid + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": UPDATE PMCID from " + old_pmcid + " to " + new_pmcid + " failed: " + str(e) + "\n")


def _insert_pmcid(db_session, fw, pmid, reference_id, pmcid, logger=None):  # pragma: no cover

    ## for some reason, we need to add this check to make sure it is not in db
    x = db_session.query(CrossReferenceModel).filter_by(curie="PMCID:" + pmcid).one_or_none()
    if x:
        if x.reference_id != reference_id:
            if logger:
                logger.info("The PMCID:" + pmcid + " is associated with two papers: reference_ids=" + str(reference_id) + ", " + str(x.reference_id))
        return

    data = {"curie": "PMCID:" + pmcid,
            "curie_prefix": "PMCID",
            "reference_id": reference_id,
            "is_obsolete": False}
    try:
        x = CrossReferenceModel(**data)
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": INSERT PMCID:" + pmcid + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": INSERT PMCID:" + pmcid + " failed: " + str(e) + "\n")


def update_cross_reference(db_session, fw, pmid, reference_id, doi_db, doi_list_in_db, doi_json, pmcid_db, pmcid_list_in_db, pmcid_json, update_log, logger=None):

    doi_json = doi_json.replace("DOI:", "") if doi_json else None
    pmcid_json = pmcid_json.replace("PMCID:", "") if pmcid_json else None

    if doi_json is None and pmcid_json is None:
        return

    ## take care of DOI
    if doi_json and (doi_db is None or doi_json != doi_db) and doi_json in doi_list_in_db:
        fw.write("PMID:" + str(pmid) + ": DOI:" + doi_json + " is in the database for another paper.\n")
    else:
        if doi_json and doi_json != doi_db:
            try:
                if doi_db is None:
                    _insert_doi(db_session, fw, pmid, reference_id, doi_json, logger)
                else:
                    _update_doi(db_session, fw, pmid, reference_id, doi_db, doi_json)
                update_log['doi'] = update_log['doi'] + 1
                update_log['pmids_updated'].append(pmid)
            except Exception as e:
                logger.info(str(e))
    ## take care of PMCID
    if pmcid_json:
        if pmcid_json.startswith('PMC'):
            if not pmcid_json.replace('PMC', '').isdigit():
                pmcid_json = None
        else:
            pmcid_json = None

    if pmcid_json is None:
        return

    if pmcid_db and pmcid_db == pmcid_json:
        return

    if pmcid_json and (pmcid_db is None or pmcid_json != pmcid_db) and pmcid_json in pmcid_list_in_db:
        fw.write("PMID:" + str(pmid) + ": PMC:" + pmcid_json + " is in the database for another paper.\n")
    else:
        if pmcid_db:
            _update_pmcid(db_session, fw, pmid, reference_id, pmcid_db, pmcid_json)
        else:
            _insert_pmcid(db_session, fw, pmid, reference_id, pmcid_json, logger)

        update_log['pmcid'] = update_log['pmcid'] + 1
        update_log['pmids_updated'].append(pmid)


def insert_referencefile_mod_for_pmc(db_session, pmid, file_name_with_suffix, referencefile_id, logger):

    try:
        x = ReferencefileModAssociationModel(referencefile_id=referencefile_id)
        db_session.add(x)
        logger.info("PMID:" + pmid + ": pmc oa file = " + file_name_with_suffix + ": loaded into Referencefile_mod table")
    except Exception as e:
        logger.info("PMID:" + pmid + ": pmc oa file = " + file_name_with_suffix + ": an error occurred when loading data into Referencefile_modtable: " + str(e))


def insert_referencefile(db_session, pmid, file_class, file_publication_status, file_name_with_suffix, reference_id, md5sum, logger):

    file_extension = file_name_with_suffix.split(".")[-1].lower()
    display_name = file_name_with_suffix.replace("." + file_extension, "")
    pdf_type = None
    if file_extension == 'pdf':
        pdf_type = 'pdf'

    logger.info(file_name_with_suffix + " | " + display_name + " | " + file_extension + " | " + str(pdf_type))

    referencefile_id = None

    try:
        x = ReferencefileModel(display_name=display_name,
                               reference_id=reference_id,
                               md5sum=md5sum,
                               file_class=file_class,
                               file_publication_status=file_publication_status,
                               file_extension=file_extension,
                               pdf_type=pdf_type,
                               is_annotation=False)
        db_session.add(x)
        db_session.flush()
        db_session.refresh(x)
        referencefile_id = x.referencefile_id
        logger.info("PMID:" + pmid + ": pmc oa file = " + file_name_with_suffix + ": loaded into Referencefile table")
    except Exception as e:
        logger.info("PMID:" + pmid + ": pmc oa file = " + file_name_with_suffix + " an error occurred when loading data into Referencefile table. error: " + str(e))

    return referencefile_id
