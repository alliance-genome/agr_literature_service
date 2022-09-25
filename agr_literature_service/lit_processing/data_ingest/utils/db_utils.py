from fastapi.encoders import jsonable_encoder

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.api.models import ReferenceModel, AuthorModel, \
    CrossReferenceModel, ModCorpusAssociationModel, ModReferenceTypeModel

batch_size_for_commit = 250


def get_references_by_curies(db_session, curie_list):

    if len(curie_list) == 0:
        return {}

    ref_curie_to_reference = {}

    for x in db_session.query(ReferenceModel).filter(ReferenceModel.curie.in_(curie_list)).all():
        ref_curie_to_reference[x.curie] = jsonable_encoder(x)

    return ref_curie_to_reference


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
    for x in author_list_in_json:
        orcid = 'ORCID:' + x['orcid'] if x.get('orcid') else ''
        affiliations = x['affiliations'] if x.get('affiliations') else []
        if x.get('authorRank') is None:
            logger.info("The authors in json record for REFERENCE_ID:" + str(reference_id) + " has no authorRank")
            return
        authors_in_json.append((x.get('name', ''), x.get('firstName', ''), x.get('lastName', ''), x.get('authorRank'), '|'.join(affiliations), orcid))

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
