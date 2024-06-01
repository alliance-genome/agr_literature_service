from os import environ, makedirs, path
import json
from sqlalchemy import or_
import unicodedata

from agr_literature_service.api.crud.mod_reference_type_crud import insert_mod_reference_type_into_db
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_reference_id_by_curie, get_reference_id_by_pmid
from agr_literature_service.api.models import ReferenceModel, AuthorModel, \
    CrossReferenceModel, ModCorpusAssociationModel, ModModel, ReferenceRelationModel, \
    MeshDetailModel, ReferenceModReferencetypeAssociationModel, \
    ReferencefileModel, ReferencefileModAssociationModel, WorkflowTagModel
from agr_literature_service.api.crud.utils.patterns_check import check_pattern

batch_size_for_commit = 250


def move_mod_papers_into_corpus(db_session, mod, mod_id, mod_reference_id_set, logger=None):  # pragma: no cover

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


def change_mod_curie_status(db_session, mod, mod_curie_set, mod_curie_to_pmid, logger=None):  # pragma: no cover

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
                if _is_prepublication_pipeline(db_session, x.reference_id):
                    continue
                x.is_obsolete = True
                db_session.add(x)
                if logger:
                    logger.info(f"Changing {mod} curie to obsolete for {x.curie}")
    except Exception as e:
        if logger:
            logger.info(f"An error occurred when changing is_obsolete for {mod} curie. error={e}")

    db_session.commit()
    # db_session.rollback()

    mod_id = _get_mod_id_by_mod(db_session, mod)
    mod_curies_not_in_db = mod_curie_set - mod_curie_set_in_db

    if logger:
        logger.info("Moving mod papers into corpus...")
    move_mod_papers_into_corpus(db_session, mod, mod_id, mod_reference_id_set, logger)

    if logger:
        logger.info("Moving obsolete papers out of corpus...")
    move_obsolete_papers_out_of_corpus(db_session, mod, mod_id, curie_prefix, logger)

    if logger:
        logger.info("Adding mod PubMed papers with a conflict DOI/PMCID into corpus...")
    add_not_loaded_pubmed_papers(db_session, mod, mod_id, mod_curies_not_in_db,
                                 mod_curie_to_pmid, logger)


def add_not_loaded_pubmed_papers(db_session, mod, mod_id, mod_curies_to_load, mod_curie_to_pmid, logger):  # pragma: no cover

    mod_curies_not_in_db = set()
    for curie in mod_curies_to_load:
        if curie in mod_curie_to_pmid:
            pmid = mod_curie_to_pmid[curie]
            rows = db_session.query(CrossReferenceModel).filter_by(curie=pmid).all()
            if len(rows) == 0:
                mod_curies_not_in_db.add(curie)
            else:
                reference_id = None
                for x in rows:
                    if x.is_obsolete is False:
                        reference_id = x.reference_id
                        break
                if reference_id is None:
                    mod_curies_not_in_db.add(curie)
                    continue
                try:
                    cr = CrossReferenceModel(curie_prefix=curie.split(':')[0],
                                             curie=curie,
                                             is_obsolete=False,
                                             reference_id=reference_id)
                    db_session.add(cr)
                    mca = db_session.query(ModCorpusAssociationModel).filter_by(
                        mod_id=mod_id, reference_id=reference_id).one_or_none()
                    if mca is None:
                        mod_curies_not_in_db.add(curie)
                        continue
                    mca.corpus = True
                    mca.mod_corpus_sort_source = "dqm_files"
                    db_session.add(mca)
                    if logger:
                        logger.info(f"Adding {mod} {curie} into the database.")
                    db_session.commit()
                except Exception as e:
                    db_session.rollback()
                    mod_curies_not_in_db.add(curie)
                    if logger:
                        logger.info(f"An error occurred when adding {mod} {curie} into the database. error={e}")
        else:
            mod_curies_not_in_db.add(curie)

    if logger:
        logger.info(f"{mod} curies that are not loaded into the database: {mod_curies_not_in_db}")


def move_obsolete_papers_out_of_corpus(db_session, mod, mod_id, curie_prefix, logger=None):  # pragma: no cover

    rows = db_session.execute(f"SELECT mca.mod_corpus_association_id, cr.reference_id "
                              f"FROM mod_corpus_association mca, cross_reference cr, reference r "
                              f"WHERE mca.mod_id = {mod_id} "
                              f"AND mca.corpus is True "
                              f"AND mca.reference_id = cr.reference_id "
                              f"AND cr.curie_prefix = '{curie_prefix}' "
                              f"AND cr.is_obsolete is True "
                              f"AND cr.reference_id = r.reference_id "
                              f"AND r.prepublication_pipeline is False").fetchall()

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


def _is_prepublication_pipeline(db_session, reference_id):  # pragma: no cover

    rows = db_session.execute(f"SELECT prepublication_pipeline "
                              f"FROM   reference "
                              f"WHERE  reference_id = {reference_id}").fetchall()
    return rows[0][0]


def mark_not_in_mod_papers_as_out_of_corpus(mod, missing_papers_in_mod, logger=None):

    db_session = create_postgres_session(False)

    mod_id = _get_mod_id_by_mod(db_session, mod)

    # xref_id: MOD database ID, eg. SGD:00000000005
    for (xref_id, agr, pmid) in missing_papers_in_mod:
        try:
            cr = db_session.query(CrossReferenceModel).filter_by(curie=xref_id, is_obsolete=False).one_or_none()
            if cr and _is_prepublication_pipeline(db_session, cr.reference_id):
                continue
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

        prefix = entry["curie"].split(':')[0]
        status = check_pattern('reference', entry["curie"])
        if status is None:
            logger.info(f"Unable to find curie prefix {prefix} in pattern list for reference")
            continue
        if status is False:
            logger.info(f"The curie {entry['curie']} doesn't match the pattern for reference")
            continue
        try:
            x = CrossReferenceModel(reference_id=reference_id,
                                    curie_prefix=prefix,
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


def update_authors(db_session, reference_id, author_list_in_db, author_list_in_json, pub_status_changed, pmids_with_pub_status_changed, logger=None, fw=None, pmid=None, update_log=None):  # noqa: C901 # pragma: no cover
    """
    * Perform case-insensitive comparisons of author names after trimming leading and
      trailing whitespace.
    * Update existing author records to maintain author IDs as much as possible.
    * Handle updates to affiliations and ORCIDs without deleting the entire author record.
    * Update author names if the order remains the same to maintain author IDs.
    * Ensure that author order values are updated while respecting unique constraints.
    """

    if author_list_in_json is None:
        author_list_in_json = []

    # rename the field names in json to match the field names from database
    author_list_in_json_new = []
    author_order = 0
    for x in author_list_in_json:
        author_order += 1
        orcid = x.get('orcid')
        if orcid:
            orcid = f"ORCID:{orcid}" if not orcid.upper().startswith('ORCID') else orcid.upper()
        author_list_in_json_new.append({
            'name': x['name'],
            'first_name': (x['firstname'] if 'firstname' in x else x.get('firstName', '')).strip(),
            'last_name': (x['lastname'] if 'lastname' in x else x.get('lastName', '')).strip(),
            'first_initial': (x['firstinit'] if 'firstinit' in x else x.get('firstInit', '')).strip(),
            'order': x['authorRank'] if 'authorRank' in x else author_order,
            'affiliations': x['affiliations'] if x.get('affiliations') else [],
            'orcid': orcid
        })

    # normalize author details for comparison
    def normalize_author(x):
        return (
            x['name'].strip().lower(),
            x['first_name'].strip().lower() if x['first_name'] else '',
            x['last_name'].strip().lower() if x['last_name'] else '',
            x['first_initial'].strip().lower() if x['first_initial'] else '',
            x['order'],
            '|'.join(x['affiliations']).strip().lower() if x['affiliations'] else '',
            x['orcid'].strip().lower() if x['orcid'] else ''
        )

    # generate a unique key for each author based on specific attributes
    # using last_name, first_name, first_initial, and name (fullname)
    def author_key(x):
        return (x[2], x[1], x[3], x[0])

    normalized_author_list_in_db = [normalize_author(x) for x in author_list_in_db]
    normalized_author_list_in_json = [normalize_author(x) for x in author_list_in_json_new]

    if normalized_author_list_in_db == normalized_author_list_in_json:
        return []

    # create dictionaries of authors for comparison
    authors_in_db = {author_key(normalize_author(x)): x for x in author_list_in_db}
    authors_in_json = {author_key(normalize_author(x)): x for x in author_list_in_json_new}

    """
    print("PMID:" + pmid + " authors_in_db   =", authors_in_db)
    print("PMID:" + pmid + " authors_in_json =", authors_in_json)
    """

    """
    ('maita', 'nobuo', 'n', 'nobuo maita'): { 'orcid': None,
                                              'first_author': False,
                                              'order': 1,
                                              'corresponding_author': False,
                                              'name': 'Nobuo Maita',
                                              'affiliations': ['Dept of Molecular Biology, ..'],
                                              'first_name': 'Nobuo',
                                              'last_name': 'Maita',
                                              'first_initial': 'N'},
    ...
    """

    # check for authors to update or delete
    author_order_to_update_record = {}
    author_order_to_delete_record = {}
    for key, db_author in authors_in_db.items():
        if key not in authors_in_json:
            author_order_to_delete_record[db_author['order']] = db_author
        else:
            json_author = authors_in_json[key]
            author_order_to_update_record[db_author['order']] = json_author

    # Check for new authors to add
    author_order_to_add_record = {}
    for key, json_author in authors_in_json.items():
        if key not in authors_in_db:
            author_order_to_add_record[json_author['order']] = json_author

    if author_order_to_delete_record or author_order_to_add_record or author_order_to_update_record:
        if update_log:
            update_log['author_name'] = update_log.get('author_name', 0) + 1
            update_log['pmids_updated'].append(pmid)

    set_to_update = check_delete_add_rows(len(author_list_in_db), author_order_to_add_record, author_order_to_delete_record)
    if set_to_update:
        author_order_to_update_record = author_order_to_add_record
        author_order_to_add_record = {}
        author_order_to_delete_record = {}

    """
    orcid_to_add_record = {}
    for author_order in author_order_to_add_record:
        author = author_order_to_add_record[author_order]
        if author['orcid']:
            orcid_to_add_record[author['orcid']] = (author_order, author)
    for author_order in author_order_to_delete_record:
        author = author_order_to_add_record[author_order]
        if author['orcid'] and author['orcid'] in orcid_to_add_record:
            (author_order_to_add, author_to_add) = orcid_to_add_record[author['orcid']]
            author_order_to_update_record[author_order] = author_to_add
            author_order_to_add_record.pop(author_order_to_add)
            author_order_to_delete_record.pop(author_order)
    """

    """
    if author_order_to_update_record:
        print("PMID:" + pmid + " author_order_to_update_record =", author_order_to_update_record)
    if author_order_to_delete_record:
        print("PMID:" + pmid + " author_order_to_delete_record =", author_order_to_delete_record)
    if author_order_to_add_record:
        print("PMID:" + pmid + " author_order_to_add_record    =", author_order_to_add_record)
    """

    # update author rows & delete authors
    name_removed = []
    name_updated = []
    for x in db_session.query(AuthorModel).filter_by(reference_id=reference_id).order_by(AuthorModel.order).all():
        if x.order in author_order_to_update_record:
            json_author = author_order_to_update_record[x.order]
            if x.name != json_author['name']:
                name_updated.append((x.name, json_author['name']))
                x.name = json_author['name']
            if x.first_name != json_author['first_name']:
                x.first_name = json_author['first_name']
            if x.last_name != json_author['last_name']:
                x.last_name = json_author['last_name']
            if x.first_initial != json_author['first_initial']:
                x.first_initial = json_author['first_initial']
            if x.affiliations != json_author['affiliations']:
                x.affiliations = json_author['affiliations']
            if x.orcid != json_author['orcid']:
                x.orcid = json_author['orcid']
            db_session.add(x)
        elif x.order in author_order_to_delete_record:
            author = author_order_to_delete_record[x.order]
            name_removed.append(author['name'])
            db_session.delete(x)
        else:
            ## update author order to match what is in json
            key = (x.last_name.strip().lower(), x.first_name.strip().lower(), x.first_initial.strip().lower(), x.name.strip().lower())
            json_author = authors_in_json[key]
            if x.order != json_author['order']:
                x.order = json_author['order']
                db_session.add(x)

    # add new authors
    name_added = []
    for author_order in author_order_to_add_record:
        author = author_order_to_add_record[author_order]
        try:
            x = AuthorModel(
                reference_id=reference_id,
                name=author['name'],
                first_name=author['first_name'],
                last_name=author['last_name'],
                first_initial=author['first_initial'],
                order=author['order'],
                affiliations=author['affiliations'],
                orcid=author['orcid'],
                first_author=False,
                corresponding_author=False
            )
            db_session.add(x)
            log_message = f": INSERT AUTHOR: {author['name']} | '{author['affiliations']}'"
            _write_log_message(reference_id, log_message, pmid, logger, fw)
            name_added.append(author['name'])
        except Exception as e:
            log_message = f": INSERT AUTHOR: {author['name']} failed: {str(e)}"
            _write_log_message(reference_id, log_message, pmid, logger, fw)
    """
    print("PMID:" + pmid + " reference_id=" + str(reference_id) + " name_removed=", name_removed)
    print("PMID:" + pmid + " reference_id=" + str(reference_id) + " name_added  =", name_added)
    print("PMID:" + pmid + " reference_id=" + str(reference_id) + " name_updated=", name_updated)
    """

    ## comment out for now
    # db_session.commit()

    if name_added or name_updated or name_removed:
        author_update_messages = []
        if len(name_added) > 0 or len(name_removed) > 0:
            print("PMID:" + pmid + " reference_id=" + str(reference_id) + " Deleted: ", name_removed)
            print("PMID:" + pmid + " reference_id=" + str(reference_id) + " Inserted:", name_added)
            name_list_removed = ', '.join(name_removed)
            name_list_added = ', '.join(name_added)
            author_update_messages.append(f"Deleted: {name_list_removed} to Inserted: {name_list_added}")
        old_name_list = []
        new_name_list = []
        for (old_name, new_name) in name_updated:
            old_name_list.append(old_name)
            new_name_list.append(new_name)
            print("PMID:" + pmid + " reference_id=" + str(reference_id) + " Updated:", old_name, " to ", new_name)
        name_list_old = ', '.join(old_name_list)
        name_list_new = ', '.join(new_name_list)
        author_update_messages.append(f"'{name_list_old}' to '{name_list_new}'")
        status_changed = pmids_with_pub_status_changed.get(pub_status_changed, {})
        data_changed = status_changed.get(pmid, {})
        data_changed['authors'] = author_update_messages
        status_changed[pmid] = data_changed
        pmids_with_pub_status_changed[pub_status_changed] = status_changed

    return []


def compare_author_lists(old_list, new_list):
    """
    Convert both lists to 'LastName FirstInitial' format, then compare.
    """

    def is_lastname_firstinitial(name_list):
        """check if the name list is in 'LastName FirstInitial' format."""
        for name in name_list:
            parts = name.split()
            if len(parts) != 2:
                return False
            last_name, first_initial = parts
            if len(first_initial) > 2 or not first_initial[0].isupper():
                return False
        return True

    def normalize_name(name):
        """Normalize names in various formats to 'LastName FirstInitials'."""
        # Replace commas, dots, hyphens, and multiple spaces with a single space, then split by space
        name = name.replace(',', ' ').replace('.', ' ').replace('-', ' ')
        # Replace multiple spaces with a single space
        while '  ' in name:
            name = name.replace('  ', ' ')
        parts = [part.strip() for part in name.split() if part]

        if len(parts) >= 2:
            # Last name is the last part
            last_name = parts[-1]

            # Handle multi-part first names
            first_name_parts = parts[:-1]
            first_initials = ''.join([part[0].upper() for part in first_name_parts if part])

            return f"{last_name} {first_initials}"
        return name.strip()

    def normalize_list(name_list):
        """normalize a list of 'FirstName LastName' to 'LastName FirstInitial'"""
        return [normalize_name(name) for name in name_list]

    old_list_format = is_lastname_firstinitial(old_list)
    new_list_format = is_lastname_firstinitial(new_list)

    normalized_old_list = old_list if old_list_format else normalize_list(old_list)
    normalized_new_list = new_list if new_list_format else normalize_list(new_list)

    return normalized_old_list == normalized_new_list


def check_delete_add_rows(author_count_db, author_order_to_add_record, author_order_to_delete_record):
    """
    check if every pair of corresponding old and new name has the same last name.
    normalize every name to remove accents and convert to lowercase before comparing.
    """

    """
    old: ['Guadalupe-Medina V', 'Wisselink HW', 'Luttik MA', 'de Hulster E',
          'Daran JM', 'Pronk JT', 'van Maris AJ']
    new: ['Víctor Guadalupe-Medina', 'H Wouter Wisselink', 'Marijke Ah Luttik', 'Erik de Hulster',
          'Jean-Marc Daran', 'Jack T Pronk', 'Antonius Ja van Maris']

    old: ['Pillai RS', ' Will CL', ' Luhrmann R', ' Schumperli D', ' Muller B']
    new: ['R S Pillai', 'C L Will', 'R Lührmann', 'D Schümperli', 'B Müller']

    old: ['Keller R', 'Schneider D']
    new: ['Rebecca Keller', 'Dirk Schneider']

    old: ['Kurat CF', 'Recht J', 'Radovani E', 'Durbic T', 'Andrews B', 'Fillingham J']
    new: ['Christoph F Kurat', 'Judith Recht', 'Ernest Radovani', 'Tanja Durbic', 'Brenda Andrews',
          'Jeffrey Fillingham']

    old: ['Strahl T', 'Thorner J']
    new: ['Thomas Strahl', 'Jeremy Thorner']

    old: ['T Kutateladze']
    new: ['T G Kutateladze']

    old: ['Santos AL', 'Preta G']
    new: ['Ana L Santos', 'Giulio Preta']

    old: ['Zhu YH', 'Zhang C', 'Liu Y', 'Omenn GS', 'Freddolino PL', 'Yu DJ', 'Zhang Y']
    new: ['Yi-Heng Zhu', 'Chengxin Zhang', 'Yan Liu', 'Gilbert S Omenn', 'Peter L Freddolino',
          'Dong-Jun Yu', 'Yang Zhang']
    """

    def normalize_string(s):
        """
        normalize a string by removing accents
        in Unicode normalization, 'NFKD' stands for "Normalization Form KD"
        """
        normalized = unicodedata.normalize('NFKD', s)
        return ''.join([c for c in normalized if not unicodedata.combining(c)])

    set_to_update = True
    old_list = []
    new_list = []
    sorted_author_orders = sorted(author_order_to_add_record.keys())
    if author_count_db == len(author_order_to_add_record) and author_count_db == len(author_order_to_delete_record):
        for author_order in sorted_author_orders:
            json_author = author_order_to_add_record[author_order]
            db_author = author_order_to_delete_record.get(author_order)
            if db_author is None:
                set_to_update = False
                break
            name_json = json_author['name'].strip()
            name_db = db_author['name'].strip()
            old_list.append(name_db)
            new_list.append(name_json)
            name_match = False
            for word_db in name_db.split(' '):
                for word_json in name_json.split(' '):
                    if normalize_string(word_db).lower() == normalize_string(word_json).lower() and len(word_db) >= 4:
                        name_match = True
            if not name_match:
                set_to_update = False
                break
    if set_to_update:
        return True
    return compare_author_lists(old_list, new_list)


def update_workflow_tags(db_session, mod_id, reference_id, workflow_tag_rows_db, workflow_tags_json, logger):

    workflow_tags_db = [x['workflow_tag_id'] for x in workflow_tag_rows_db]

    if sorted(workflow_tags_db) == sorted(workflow_tags_json):
        return

    for atp in workflow_tags_json:
        if atp not in workflow_tags_db:
            try:
                x = WorkflowTagModel(reference_id=reference_id,
                                     mod_id=mod_id,
                                     workflow_tag_id=atp)
                db_session.add(x)
                logger.info(f"The workflow_tag row for reference_id = {reference_id}, mod_id = {mod_id}, and workflow_tag_id = {atp} has been added into database.")
            except Exception as e:
                logger.info(f"An error occurred when adding workflow_tag row for reference_id = {reference_id}, mod_id = {mod_id}, and workflow_tag_id = {atp}. {e}")

    for atp in workflow_tags_db:
        if atp not in workflow_tags_json:
            try:
                x = db_session.query(WorkflowTagModel).filter_by(
                    reference_id=reference_id, mod_id=mod_id, workflow_tag_id=atp).one_or_none()
                if x:
                    db_session.delete(x)
                    logger.info(f"The workflow_tag row for reference_id = {reference_id}, mod_id = {mod_id}, and workflow_tag_id = {atp} has been removed from database.")
            except Exception as e:
                logger.info(f"An error occurred when deleting workflow_tag row for reference_id = {reference_id}, mod_id = {mod_id}, and workflow_tag_id = {atp}. {e}")


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


def check_handle_duplicate(db_session, mod, pmids, xref_ref, ref_xref_valid, logger):  # noqa: C901 # pragma: no cover

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
        xref_ids = []
        for c in cross_references:
            if c['id'].startswith('DOI:') or c['id'].startswith('PMCID:'):
                xref_ids.append(c['id'])
        for xref_id in xref_ids:
            is_doi = False
            is_pmcid = False
            if xref_id.startswith('DOI:'):
                is_doi = True
                xref_id = xref_id.replace("DOI:", '')
            if xref_id.startswith('PMCID:'):
                is_pmcid = True
                xref_id = xref_id.replace("PMCID:", '')
            agr = None
            if is_doi is True and xref_id in xref_ref['DOI']:
                agr = xref_ref['DOI'][xref_id]
            elif is_pmcid is True and xref_id in xref_ref['PMCID']:
                agr = xref_ref['PMCID'][xref_id]
            if agr is None:
                continue
            ## this xref_id (DOI or PMCID) is in the database
            all_ref_xref = ref_xref_valid[agr] if agr in ref_xref_valid else {}
            # found pmids for this DOI or PMCID
            found_pmids_for_this_xref_id = []
            for prefix in all_ref_xref:
                if prefix == 'PMID':
                    if type(all_ref_xref[prefix]) is set:
                        for x in all_ref_xref[prefix]:
                            found_pmids_for_this_xref_id.append(x)
                    else:
                        found_pmids_for_this_xref_id.append(all_ref_xref[prefix])
            if len(found_pmids_for_this_xref_id) == 0:
                reference_id = get_reference_id_by_curie(db_session, agr)
                if reference_id is None:
                    logger.info("The reference curie: " + agr + " is not in the database.")
                try:
                    cross_ref = CrossReferenceModel(curie="PMID:" + pmid,
                                                    curie_prefix='PMID',
                                                    reference_id=reference_id)
                    db_session.add(cross_ref)
                    fw.write(str(datetime.now()) + ": adding PMID:" + pmid + " to the row with XREF_ID = " + xref_id + " in the database\n")
                except Exception as e:
                    logger.info(str(datetime.now()) + ": adding " + pmid + " to the row with " + xref_id + " is failed: " + str(e) + "\n")
            else:
                fw.write(str(datetime.now()) + ": " + xref_id + " for PMID:" + pmid + " is associated with PMID(s) in the database: " + ",".join(found_pmids_for_this_xref_id) + "\n")
                not_loaded_pmids.append((pmid, xref_id, ",".join(found_pmids_for_this_xref_id)))
            if pmid in pmids:
                pmids.remove(pmid)
    fw.close()

    return (log_path, log_url, not_loaded_pmids)


def _insert_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type):  # pragma: no cover

    ## check to see if any newly added ones matches this entry
    rows = db_session.query(ReferenceRelationModel).filter_by(reference_id_from=reference_id_from, reference_id_to=reference_id_to).all()
    if len(rows) > 0:
        return

    data = {"reference_id_from": reference_id_from,
            "reference_id_to": reference_id_to,
            "reference_relation_type": type}
    try:
        x = ReferenceRelationModel(**data)
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": INSERT reference_relations: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": INSERT reference_relations: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + " failed: " + str(e) + "\n")


def _update_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type):  # pragma: no cover

    all = db_session.query(ReferenceRelationModel).filter_by(reference_id_from=reference_id_from, reference_id_to=reference_id_to).all()

    if len(all) == 0:
        return

    for x in all:
        db_session.delete(x)

    _insert_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type)


def _delete_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type):  # pragma: no cover

    for x in db_session.query(ReferenceRelationModel).filter_by(reference_id_from=reference_id_from, reference_id_to=reference_id_to, reference_relation_type=type).all():
        try:
            db_session.delete(x)
            fw.write("PMID:" + str(pmid) + ": DELETE reference_relations: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + "\n")
        except Exception as e:
            fw.write("PMID:" + str(pmid) + ": DELETE reference_relations: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + " failed: " + str(e) + "\n")


def _get_curator_email_who_added_reference_relation(db_session, reference_id_from, reference_id_to, type):  # pragma: no cover

    rows = db_session.execute(f"SELECT u.email "
                              f"FROM reference_relation_version rcc, transaction t, users u "
                              f"WHERE rcc.reference_id_from = {reference_id_from} "
                              f"AND rcc.reference_id_to = {reference_id_to} "
                              f"AND rcc.reference_relation_type = '{type}' "
                              f"AND rcc.transaction_id = t.id "
                              f"AND u.id = t.user_id").fetchall()
    if len(rows) == 0:
        return None
    for row in rows:
        if row['email'] and '@' in row['email']:
            return row['email']
    return None


def _is_reference_relation_added_by_mod_dqm(db_session, reference_id_from, reference_id_to, type):  # pragma: no cover

    user_id = "sort_dqm_json_reference_updates"
    rows = db_session.execute(f"SELECT * "
                              f"FROM reference_relation_version rcc, transaction t "
                              f"WHERE rcc.reference_id_from = {reference_id_from} "
                              f"AND rcc.reference_id_to = {reference_id_to} "
                              f"AND rcc.reference_relation_type = '{type}' "
                              f"AND rcc.transaction_id = t.id "
                              f"AND t.user_id = '{user_id}'").fetchall()
    if len(rows) > 0:
        return True
    return False


def update_reference_relations(db_session, fw, pmid, reference_id, pmid_to_reference_id, reference_ids_to_reference_relation_type, reference_relation_in_json, update_log):  # noqa: C901

    type_mapping = {'ErratumIn': 'ErratumFor',
                    'CommentIn': 'CommentOn',
                    'RepublishedIn': 'RepublishedFrom',
                    'RetractionIn': 'RetractionOf',
                    'ExpressionOfConcernIn': 'ExpressionOfConcernFor',
                    'ReprintIn': 'ReprintOf',
                    'UpdateIn': 'UpdateOf'}

    if reference_relation_in_json is None or str(reference_relation_in_json) == '{}':
        reference_relation_in_json = {}

    new_reference_ids_to_reference_relation_type = {}
    with db_session.no_autoflush:
        for original_type in reference_relation_in_json:
            other_pmids = reference_relation_in_json[original_type]
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
            if any(original_type.endswith(suffix) for suffix in ['For', 'From', 'Of', 'On']):
                reference_id_from = reference_id
                for reference_id_to in other_reference_ids:
                    if reference_id_from != reference_id_to:
                        new_reference_ids_to_reference_relation_type[(reference_id_from, reference_id_to)] = original_type
            else:
                type = type_mapping.get(original_type)
                if type is None:
                    continue
                reference_id_to = reference_id
                for reference_id_from in other_reference_ids:
                    if reference_id_from != reference_id_to:
                        new_reference_ids_to_reference_relation_type[(reference_id_from, reference_id_to)] = type

    # process new and existing reference relations outside the no_autoflush block
    for key in new_reference_ids_to_reference_relation_type:
        type = new_reference_ids_to_reference_relation_type[key]
        # assuming _update_reference_relation and _insert_reference_relation might involve queries
        with db_session.no_autoflush:
            if key in reference_ids_to_reference_relation_type:
                if reference_ids_to_reference_relation_type[key] == new_reference_ids_to_reference_relation_type[key]:
                    continue
                (reference_id_from, reference_id_to) = key
                _update_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type)
                update_log['comment_erratum'] = update_log['comment_erratum'] + 1
                update_log['pmids_updated'].append(pmid)
            else:
                _insert_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type)
                update_log['comment_erratum'] = update_log['comment_erratum'] + 1
                update_log['pmids_updated'].append(pmid)

    # Assuming deletion might also involve queries
    for key in reference_ids_to_reference_relation_type:
        type = reference_ids_to_reference_relation_type[key]
        if key not in new_reference_ids_to_reference_relation_type:
            (reference_id_from, reference_id_to) = key
            with db_session.no_autoflush:
                if reference_id in [reference_id_from, reference_id_to]:
                    ## only remove the ones that are coming from PubMed
                    email = _get_curator_email_who_added_reference_relation(db_session,
                                                                            reference_id_from,
                                                                            reference_id_to,
                                                                            type)
                    is_added_by_dqm_script = _is_reference_relation_added_by_mod_dqm(db_session,
                                                                                     reference_id_from,
                                                                                     reference_id_to,
                                                                                     type)
                    if email or is_added_by_dqm_script:
                        continue
                    _delete_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type)
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
        mesh_terms_in_json_data = []

    mesh_terms_in_json = []

    for m in mesh_terms_in_json_data:
        heading_term = m.get('meshHeadingTerm')
        qualifier_term = m.get('meshQualifierTerm', '')
        if heading_term is None:
            continue
        mesh_terms_in_json.append((heading_term, qualifier_term))

    if mesh_terms_in_db is None:
        mesh_terms_in_db = []

    if set(mesh_terms_in_json) == set(mesh_terms_in_db):
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


def _prefix_xref_identifier(identifier, prefix):  # pragma: no cover
    if identifier and not identifier.startswith(prefix):
        return f"{prefix}:{identifier}"
    return identifier


def _check_xref_existence(db_session, model, curie):  # pragma: no cover
    """Checks if an entry exists in the database."""
    return db_session.query(model).filter_by(curie=curie, is_obsolete=False).one_or_none()


def _update_doi(db_session, fw, pmid, reference_id, old_doi, new_doi):  # pragma: no cover

    try:
        new_doi_curie = None
        old_doi_curie = None
        if new_doi:
            new_doi_curie = _prefix_xref_identifier(new_doi, 'DOI')
            x = _check_xref_existence(db_session, CrossReferenceModel, new_doi_curie)
            if x:
                fw.write(f"{new_doi_curie} is already in the database.\n")
                return
        if old_doi:
            old_doi_curie = _prefix_xref_identifier(old_doi, 'DOI')
            x = db_session.query(CrossReferenceModel).filter_by(
                reference_id=reference_id, is_obsolete=False, curie=old_doi_curie).one_or_none()
            if x is None:
                return
            if new_doi_curie:
                x.curie = new_doi_curie
                db_session.add(x)
                fw.write(f"PMID:{pmid}: UPDATE DOI from {old_doi}  to {new_doi}\n")
            else:
                x.is_obsolete = True
                db_session.add(x)
                fw.write(f"PMID:{pmid}: SET DOI:{old_doi} to invalid\n")
    except Exception as e:
        fw.write(f"PMID:{pmid}: UPDATE DOI from {old_doi} to {new_doi} failed: {e}\n")


def _insert_doi(db_session, fw, pmid, reference_id, doi, logger=None):  # pragma: no cover

    ## for some reason, we need to add this check to make sure it is not in db
    doi_curie = _prefix_xref_identifier(doi, 'DOI')
    x = _check_xref_existence(db_session, CrossReferenceModel, doi_curie)
    if x:
        if x.reference_id != reference_id:
            if logger:
                logger.info(f"new {doi_curie} for PMID:{pmid} is associated with another paper in the database: reference_id={x.reference_id}")
        return
    x = db_session.query(CrossReferenceModel).filter_by(
        curie_prefix="DOI", reference_id=reference_id, is_obsolete=False).one_or_none()
    if x:
        if logger:
            logger.info(f"Key (curie_prefix, reference_id)=(DOI, {reference_id}) already exists")
            return
    data = {"curie": "DOI:" + doi,
            "curie_prefix": "DOI",
            "reference_id": reference_id,
            "is_obsolete": False}
    try:
        x = CrossReferenceModel(**data)
        db_session.add(x)
        fw.write(f"PMID:{pmid}: INSERT DOI:{doi}\n")
    except Exception as e:
        fw.write(f"PMID:{pmid}: INSERT DOI:{doi} failed: {e}\n")


def _update_pmcid(db_session, fw, pmid, reference_id, old_pmcid, new_pmcid, logger):  # pragma: no cover

    new_pmcid_curie = None
    old_pmcid_curie = None
    if new_pmcid:
        new_pmcid_curie = _prefix_xref_identifier(new_pmcid, 'PMCID')
        x = _check_xref_existence(db_session, CrossReferenceModel, new_pmcid_curie)
        if x:
            if logger:
                logger.info(f"Key (curie)=(PMCID:{new_pmcid}) already exists for another paper in the database: reference_id={x.reference_id}")
            return
    if old_pmcid:
        old_pmcid_curie = _prefix_xref_identifier(old_pmcid, 'PMCID')
        x = db_session.query(CrossReferenceModel).filter_by(
            reference_id=reference_id, curie=old_pmcid_curie).one_or_none()
        if x is None:
            return
        try:
            if new_pmcid_curie:
                x.curie = new_pmcid_curie
                db_session.add(x)
                fw.write(f"PMID:{pmid}: UPDATE PMCID from {old_pmcid} to {new_pmcid}\n")
            else:
                x.is_obsolete = True
                db_session.add(x)
                fw.write(f"PMID:{pmid}: SET PMCID{old_pmcid} to invalid\n")
        except Exception as e:
            fw.write(f"PMID:{pmid}: UPDATE PMCID from {old_pmcid} to {new_pmcid} failed: {e}\n")


def _insert_pmcid(db_session, fw, pmid, reference_id, pmcid, logger=None):  # pragma: no cover

    ## for some reason, we need to add this check to make sure it is not in db
    curie = _prefix_xref_identifier(pmcid, 'PMCID')
    x = _check_xref_existence(db_session, CrossReferenceModel, curie)
    if x:
        if x.reference_id != reference_id:
            if logger:
                logger.info(f"The new PMCID:{pmcid} for PMID:{pmid} is associated with another paper in the database: reference_id={x.reference_id}")
            return
        if x.reference_id == reference_id:
            return
    x = db_session.query(CrossReferenceModel).filter_by(
        curie_prefix="PMCID", reference_id=reference_id, is_obsolete=False).one_or_none()
    if x:
        if logger:
            logger.info(f"Key (curie_prefix, reference_id)=(PMCID, {reference_id}) already exists")
        return
    x = db_session.query(CrossReferenceModel).filter_by(curie="PMCID:" + pmcid, reference_id=reference_id).one_or_none()
    if x and x.is_obsolete is True:
        x.is_obsolete = False
        db_session.add(x)
        return
    data = {"curie": "PMCID:" + pmcid,
            "curie_prefix": "PMCID",
            "reference_id": reference_id,
            "is_obsolete": False}
    try:
        x = CrossReferenceModel(**data)
        db_session.add(x)
        fw.write(f"PMID:{pmid}: INSERT PMCID:{pmcid}\n")
    except Exception as e:
        fw.write(f"PMID:{pmid}: INSERT PMCID:{pmcid} failed: {e}\n")


def update_cross_reference(db_session, fw, pmid, reference_id, doi_db, doi_list_in_db, doi_json, pmcid_db, pmcid_list_in_db, pmcid_json, pub_status_changed, pmids_with_pub_status_changed, update_log, logger=None):  # pragma: no cover

    if doi_json:
        if doi_json.startswith('10.'):
            doi_json = "DOI:" + doi_json
        status = check_pattern('reference', doi_json)
        if not status:
            logger.info(f"The curie {doi_json} doesn't match the pattern for reference")
            return
    if pmcid_json:
        if pmcid_json.startswith('PMC'):
            pmcid_json = "PMCID:" + pmcid_json
        status = check_pattern('reference', pmcid_json)
        if not status:
            logger.info(f"The curie {pmcid_json} doesn't match the pattern for reference")
            return
    doi_json = doi_json.replace("DOI:", "") if doi_json else None
    pmcid_json = pmcid_json.replace("PMCID:", "") if pmcid_json else None

    ## take care of DOI
    if doi_json and (doi_db is None or doi_json != doi_db) and doi_json in doi_list_in_db:
        fw.write(f"PMID:{pmid}: DOI:{doi_json} is in the database for another paper.\n")
    else:
        if doi_json != doi_db:
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

    if pmcid_db == pmcid_json:
        return

    if pmcid_json and (pmcid_db is None or pmcid_json != pmcid_db) and pmcid_json in pmcid_list_in_db:
        fw.write(f"PMID:{pmid}: PMCID:{pmcid_json} is in the database for another paper.\n")
    else:
        if pmcid_db:
            _update_pmcid(db_session, fw, pmid, reference_id, pmcid_db, pmcid_json, logger)
        else:
            _insert_pmcid(db_session, fw, pmid, reference_id, pmcid_json, logger)

        update_log['pmcid'] = update_log['pmcid'] + 1
        update_log['pmids_updated'].append(pmid)

        message = f"from '{pmcid_db}' to '{pmcid_json}'"
        status_changed = pmids_with_pub_status_changed.get(pub_status_changed, {})
        data_changed = status_changed.get(pmid, {})
        data_changed['PMCID'] = message
        status_changed[pmid] = data_changed
        pmids_with_pub_status_changed[pub_status_changed] = status_changed


def insert_referencefile_mod_for_pmc(db_session, pmid, file_name_with_suffix, referencefile_id, logger):  # pragma: no cover

    try:
        x = ReferencefileModAssociationModel(referencefile_id=referencefile_id)
        db_session.add(x)
        logger.info("PMID:" + pmid + ": pmc oa file = " + file_name_with_suffix + ": loaded into Referencefile_mod table")
    except Exception as e:
        logger.info("PMID:" + pmid + ": pmc oa file = " + file_name_with_suffix + ": an error occurred when loading data into Referencefile_modtable: " + str(e))


def insert_referencefile(db_session, pmid, file_class, file_publication_status, file_name_with_suffix, reference_id, md5sum, logger):  # pragma: no cover

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
