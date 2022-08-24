from sqlalchemy import or_
import argparse
import logging
from os import environ, makedirs, path
from dotenv import load_dotenv
from datetime import datetime, date
import json
import time

from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel, \
    ModModel, ModCorpusAssociationModel, ReferenceCommentAndCorrectionModel, \
    AuthorModel, MeshDetailModel, ResourceModel
from agr_literature_service.api.crud.reference_crud import get_citation_from_args
from agr_literature_service.lit_processing.helper_sqlalchemy import create_postgres_session, \
    create_postgres_engine
from agr_literature_service.lit_processing.update_resource_pubmed_nlm import update_resource_pubmed_nlm
from agr_literature_service.lit_processing.get_pubmed_xml import download_pubmed_xml
from agr_literature_service.lit_processing.xml_to_json import generate_json
from agr_literature_service.lit_processing.filter_dqm_md5sum import load_s3_md5data
from agr_literature_service.lit_processing.helper_s3 import upload_xml_file_to_s3
from agr_literature_service.lit_processing.helper_email import send_email

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

refColName_to_update = ['title', 'volume', 'issue_name', 'page_range', 'citation',
                        'abstract', 'pubmed_types', 'pubmed_publication_status',
                        'keywords', 'category', 'plain_language_abstract',
                        'pubmed_abstract_languages', 'language', 'date_published',
                        'date_arrived_in_pubmed', 'date_last_modified_in_pubmed',
                        'publisher', 'resource_id']

field_names_to_report = refColName_to_update + ['doi', 'pmcid', 'author_name', 'journal',
                                                'comment_erratum', 'mesh_term',
                                                'pmids_updated']
# limit = 1000
limit = 500
max_rows_per_commit = 250
download_xml_max_size = 150000
query_cutoff = 500
sleep_time = 60


def update_data(mod, pmids, md5dict=None, newly_added_pmids=None):  # noqa: C901

    if md5dict is None and mod:
        update_resource_pubmed_nlm

    db_session = create_postgres_session(False)

    # datestamp = str(date.today()).replace("-", "")

    (xml_path, json_path, old_xml_path, old_json_path, log_path, log_url,
     email_recipients, sender_email, sender_password, reply_to) = set_paths()

    email_subject = "PubMed Paper Update Report"
    if mod and mod != 'NONE':
        email_subject = mod + " " + email_subject

    log_file = log_path + "update_pubmed_papers_"
    if mod:
        # log_file = log_file + mod + "_" + datestamp + ".log"
        log_file = log_file + mod + ".log"
    else:
        # log_file = log_file + datestamp + ".log"
        log_file = log_file + ".log"

    fw = open(log_file, "w")

    fw.write(str(datetime.now()) + "\n")
    fw.write("Getting data from the database...\n")
    log.info("Getting data from the database...")

    pmid_to_reference_id = {}
    reference_id_to_pmid = {}
    pmids_all = []
    if mod:
        if mod == 'NONE':
            get_pmid_to_reference_id_for_papers_not_associated_with_mod(db_session,
                                                                        pmid_to_reference_id,
                                                                        reference_id_to_pmid)
        else:
            get_pmid_to_reference_id(db_session, mod, pmid_to_reference_id, reference_id_to_pmid)
        pmids_all = list(pmid_to_reference_id.keys())
    else:
        get_reference_ids_by_pmids(db_session, pmids, pmid_to_reference_id, reference_id_to_pmid)
        pmids_all = pmids.split('|')
    pmids_all.sort()

    db_session.close()

    update_log = {}
    for field_name in field_names_to_report:
        if field_name == 'pmids_updated':
            update_log[field_name] = []
        else:
            update_log[field_name] = 0

    if md5dict is None:
        fw.write(str(datetime.now()) + "\n")
        fw.write("Downloading pubmed xml files for " + str(len(pmids_all)) + " PMIDs...\n")
        log.info("Downloading pubmed xml files for " + str(len(pmids_all)) + " PMIDs...")

        if len(pmids_all) > download_xml_max_size:
            for index in range(0, len(pmids_all), download_xml_max_size):
                pmids_slice = pmids_all[index:index + download_xml_max_size]
                download_pubmed_xml(pmids_slice)
                time.sleep(sleep_time)
        else:
            download_pubmed_xml(pmids_all)

        fw.write(str(datetime.now()) + "\n")
        fw.write("Downloading PMID_md5sum from s3...\n")
        log.info("Downloading PMID_md5sum from s3...")
        md5dict = load_s3_md5data(['PMID'])

    old_md5sum = md5dict['PMID']

    ## for testing purpose, test run for SGD
    # old_md5sum.pop('8460134')
    # old_md5sum.pop('9489999')
    # old_md5sum.pop('9334203')
    # old_md5sum.pop('2506425')
    # old_md5sum.pop('10525964')
    ## for testing purpose, test run for WB
    # old_md5sum.pop('15279955')
    # old_md5sum.pop('15302406')
    # old_md5sum.pop('19167330')
    # old_md5sum.pop('18931687')
    # old_md5sum.pop('19116311')
    # old_md5sum.pop('17276139')
    ## end testing

    fw.write(str(datetime.now()) + "\n")
    fw.write("Generating json files...\n")
    log.info("Generating json files...")

    not_found_xml_set = set()
    generate_json(pmids_all, [], not_found_xml_set)

    if newly_added_pmids:
        for pmid in newly_added_pmids:
            not_found_xml_set.discard(pmid)
    not_found_xml_list = list(not_found_xml_set)

    new_md5sum = get_md5sum(json_path)

    reference_id_list = []
    if mod:
        reference_id_list = generate_pmids_with_info(pmids_all, old_md5sum, new_md5sum, pmid_to_reference_id)
    else:
        reference_id_list = list(reference_id_to_pmid.keys())

    if len(reference_id_list) == 0:
        close_no_update(fw, mod, email_subject, email_recipients, sender_email,
                        sender_password, reply_to, log_path)
        return

    fw.write(str(datetime.now()) + "\n")
    fw.write("Updating database...\n")
    log.info("Updating database...")

    pmids_with_json_updated = []
    authors_with_first_or_corresponding_flag = update_database(fw, mod,
                                                               reference_id_list,
                                                               reference_id_to_pmid,
                                                               pmid_to_reference_id,
                                                               update_log, new_md5sum,
                                                               old_md5sum, json_path,
                                                               pmids_with_json_updated)

    write_summary(fw, mod, update_log, authors_with_first_or_corresponding_flag,
                  not_found_xml_list, log_url, log_path, email_subject,
                  email_recipients, sender_email, sender_password, reply_to)

    if environ.get('ENV_STATE') and environ['ENV_STATE'] == 'prod':
        fw.write(str(datetime.now()) + "\n")
        fw.write("Uploading xml files to s3...\n")
        log.info("Uploading xml files to s3...")
        for pmid in pmids_with_json_updated:
            log.info("uploading xml file for PMID:" + pmid + " to s3")
            upload_xml_file_to_s3(pmid, 'latest')

    log.info("DONE!\n\n")
    fw.write(str(datetime.now()) + "\n")
    fw.write("DONE!\n")
    fw.close()


def update_database(fw, mod, reference_id_list, reference_id_to_pmid, pmid_to_reference_id, update_log, new_md5sum, old_md5sum, json_path, pmids_with_json_updated):   # noqa: C901

    ## 1. do nothing if a field has no value in pubmed xml/json
    ##    so won't delete whatever in the database
    ## 2. update/add the field if a field has a new value in pubmed xml/json

    ## start a database session
    db_session = create_postgres_session(False)

    engine = create_postgres_engine(False)
    db_connection = engine.connect()

    ## reference_id => a list of author name in order
    fw.write("Getting author info from database...\n")
    log.info("Getting author info from database...")
    reference_id_to_authors = get_author_data(db_connection, mod, reference_id_list)

    ## ORCID ID => is_obsolete
    fw.write("Getting ORCID info from database...\n")
    log.info("Getting ORCID info from database...")
    orcid_dict = get_orcid_data(db_session)

    ## (reference_id_from, reference_id_to) => a list of reference_comment_and_correction_type
    fw.write("Getting comment/correction info from database...\n")
    log.info("Getting comment/correction info from database...")
    reference_ids_to_comment_correction_type = get_comment_correction_data(db_session, mod,
                                                                           reference_id_list)

    ## reference_id => a list of mesh_terms in order
    fw.write("Getting mesh_term info from database...\n")
    log.info("Getting mesh_term info from database...")
    reference_id_to_mesh_terms = get_mesh_term_data(db_connection, mod, reference_id_list)

    ## reference_id => doi, reference_id =>pmcid
    fw.write("Getting DOI/PMCID info from database...\n")
    log.info("Getting DOI/PMCID info from database...")
    (reference_id_to_doi, reference_id_to_pmcid) = get_cross_reference_data(db_session, mod,
                                                                            reference_id_list)

    ## journal => resource_id
    fw.write("Getting journal info from database...\n")
    log.info("Getting journal info from database...")
    journal_to_resource_id = get_journal_data(db_session)

    ## resource_id => issn, resource_id => nlm
    fw.write("Getting ISSN/NLM info from database...\n")
    log.info("Getting ISSN/NLM info from database...")
    (resource_id_to_issn, resource_id_to_nlm) = get_cross_reference_data_for_resource(db_session)

    db_session.close()
    db_connection.close()
    engine.dispose()

    newly_added_orcid = []
    count = 0
    offset = 0

    ## for some reason, it needs to return from recursive function...
    authors_with_first_or_corresponding_flag = []

    authors_with_first_or_corresponding_flag = update_reference_data_batch(fw, mod, reference_id_list,
                                                                           reference_id_to_pmid,
                                                                           pmid_to_reference_id,
                                                                           reference_id_to_authors,
                                                                           reference_ids_to_comment_correction_type,
                                                                           reference_id_to_mesh_terms,
                                                                           reference_id_to_doi,
                                                                           reference_id_to_pmcid,
                                                                           journal_to_resource_id,
                                                                           resource_id_to_issn,
                                                                           resource_id_to_nlm,
                                                                           orcid_dict,
                                                                           old_md5sum,
                                                                           new_md5sum,
                                                                           count,
                                                                           newly_added_orcid,
                                                                           authors_with_first_or_corresponding_flag,
                                                                           json_path,
                                                                           pmids_with_json_updated,
                                                                           update_log,
                                                                           offset)

    return authors_with_first_or_corresponding_flag


def update_reference_data_batch(fw, mod, reference_id_list, reference_id_to_pmid, pmid_to_reference_id, reference_id_to_authors, reference_ids_to_comment_correction_type, reference_id_to_mesh_terms, reference_id_to_doi, reference_id_to_pmcid, journal_to_resource_id, resource_id_to_issn, resource_id_to_nlm, orcid_dict, old_md5sum, new_md5sum, count, newly_added_orcid, authors_with_first_or_corresponding_flag, json_path, pmids_with_json_updated, update_log, offset):

    ## only update 3000 references per session (set in max_rows_per_db_session)
    ## just in case the database get disconnected during the update process
    db_session = create_postgres_session(False)

    fw.write("Getting data from Reference table...\n")
    if mod and len(reference_id_list) > query_cutoff:
        log.info("Getting data from Reference table...limit=" + str(limit) + ", offset=" + str(offset))
    else:
        log.info("Getting data from Reference table...")

    doi_list_in_db = list(reference_id_to_doi.values())
    pmcid_list_in_db = list(reference_id_to_pmcid.values())

    all = None
    if mod and len(reference_id_list) > query_cutoff:
        all = db_session.query(
            ReferenceModel
        ).join(
            ReferenceModel.mod_corpus_association
        ).outerjoin(
            ModCorpusAssociationModel.mod
        ).filter(
            ModModel.abbreviation == mod
        ).order_by(
            ReferenceModel.reference_id
        ).offset(
            offset
        ).limit(
            limit
        ).all()
    else:
        all = db_session.query(
            ReferenceModel
        ).filter(
            ReferenceModel.reference_id.in_(reference_id_list)
        ).all()

    if len(all) == 0:
        return authors_with_first_or_corresponding_flag

    i = 0

    for x in all:

        if x.category in ['Obsolete', 'obsolete']:
            continue

        pmid = reference_id_to_pmid.get(x.reference_id)
        if pmid is None:
            continue

        count = count + 1

        if i > max_rows_per_commit:
            # db_session.rollback()
            db_session.commit()
            i = 0

        json_file = json_path + pmid + ".json"
        if not path.exists(json_file):
            continue

        if x.reference_id not in reference_id_list:
            continue

        pmids_with_json_updated.append(pmid)

        i = i + 1

        f = open(json_file)
        json_data = json.load(f)
        f.close()

        new_resource_id = None
        journal_title = None
        if json_data.get('journal'):
            if json_data.get('journal') in journal_to_resource_id:
                (new_resource_id, journal_title) = journal_to_resource_id[json_data.get('journal')]

        update_reference_table(db_session, fw, pmid, x, json_data, new_resource_id,
                               journal_title, reference_id_to_authors.get(x.reference_id),
                               update_log, count)

        ## update cross_reference table for reference
        if json_data.get('doi') or json_data.get('pmc'):
            update_cross_reference(db_session, fw, pmid, x.reference_id,
                                   reference_id_to_doi.get(x.reference_id),
                                   doi_list_in_db,
                                   json_data.get('doi'),
                                   reference_id_to_pmcid.get(x.reference_id),
                                   pmcid_list_in_db,
                                   json_data.get('pmc'), update_log)

        ## update author table
        # author_list_with_first_or_corresponding_author =
        # a list of (pmid, name, first_author, corresponding_author)
        authors = update_authors(db_session, fw, pmid, x.reference_id,
                                 reference_id_to_authors.get(x.reference_id),
                                 json_data.get('authors'), orcid_dict,
                                 newly_added_orcid, update_log)

        authors_with_first_or_corresponding_flag = authors_with_first_or_corresponding_flag + authors

        ## update reference_comment_and_correction table
        update_comment_corrections(db_session, fw, pmid, x.reference_id, pmid_to_reference_id,
                                   reference_ids_to_comment_correction_type,
                                   json_data.get('commentsCorrections'), update_log)

        ## update mesh_detail table
        update_mesh_terms(db_session, fw, pmid, x.reference_id,
                          reference_id_to_mesh_terms.get(x.reference_id),
                          json_data.get('meshTerms'), update_log)

    # db_session.rollback()
    db_session.commit()
    db_session.close()

    if mod and len(reference_id_list) > query_cutoff:
        ## call itself until all rows have been retrieved from the database for the given mod
        offset = offset + limit
        authors_with_first_or_corresponding_flag = update_reference_data_batch(fw,
                                                                               mod,
                                                                               reference_id_list,
                                                                               reference_id_to_pmid,
                                                                               pmid_to_reference_id,
                                                                               reference_id_to_authors,
                                                                               reference_ids_to_comment_correction_type,
                                                                               reference_id_to_mesh_terms,
                                                                               reference_id_to_doi,
                                                                               reference_id_to_pmcid,
                                                                               journal_to_resource_id,
                                                                               resource_id_to_issn,
                                                                               resource_id_to_nlm,
                                                                               orcid_dict,
                                                                               old_md5sum,
                                                                               new_md5sum,
                                                                               count,
                                                                               newly_added_orcid,
                                                                               authors_with_first_or_corresponding_flag,
                                                                               json_path,
                                                                               pmids_with_json_updated,
                                                                               update_log,
                                                                               offset)

    return authors_with_first_or_corresponding_flag


def create_new_citation(authors, date_published, title, journal, volume, issue, page_range):

    author_list = []
    if authors:
        for x in authors:
            if x['name']:
                author_list.append(x['name'])

    citation = get_citation_from_args(author_list, date_published, title, journal, volume, issue, page_range)

    return citation


def update_reference_table(db_session, fw, pmid, x, json_data, new_resource_id, journal_title, authors, update_log, count):   # noqa: C901

    colName_to_json_key = {'issue_name': 'issueName',
                           'page_range': 'pages',
                           'date_published': 'datePublished',
                           'pubmed_publication_status': 'publicationStatus',
                           'date_last_modified_in_pubmed': 'dateLastModified',
                           'date_arrived_in_pubmed': 'dateArrivedInPubmed',
                           'plain_language_abstract': 'plainLanguageAbstract',
                           'pubmed_abstract_languages': 'pubmedAbstractLanguages',
                           'pubmed_types': 'pubMedType',
                           'category': 'allianceCategory'}

    has_update = 0
    for colName in refColName_to_update:
        if colName == 'citation':
            new_citation = create_new_citation(authors, str(json_data.get('datePublished', '')),
                                               json_data.get('title', ''),
                                               journal_title,
                                               json_data.get('volume', ''),
                                               json_data.get('issueName', ''),
                                               json_data.get('pages', ''))
            # print("PMID:" + str(pmid) + ": old citation: " + x.citation)
            # print("PMID:" + str(pmid) + ": new citation: " + new_citation)
            if x.citation != new_citation:
                fw.write("PMID:" + str(pmid) + ": citation is updated from " + x.citation + " to " + new_citation + "\n")
                x.citation = new_citation
                has_update = has_update + 1
                update_log['citation'] = update_log['citation'] + 1
        elif colName == 'resource_id' and new_resource_id and new_resource_id != x.resource_id:
            x.resource_id = new_resource_id
            has_update = has_update + 1
            update_log['journal'] = update_log['journal'] + 1
            fw.write("PMID:" + str(pmid) + ": resource_id is updated from " + str(x.resource_id) + " to " + str(new_resource_id) + "\n")
            # PMID:22479268: resource_id is updated from 41570 to 41570
            # is it possible that this resource_id in database is a string?
        elif colName in ['date_last_modified_in_pubmed', 'date_arrived_in_pubmed']:
            j_key = colName_to_json_key[colName]
            if json_data.get('j_key'):
                if getattr(x, colName) is None or json_data[j_key]['date_string'] != str(getattr(x, colName))[0:10]:
                    old_value = getattr(x, colName)
                    setattr(x, colName, json_data[j_key])
                    has_update = has_update + 1
                    update_log[colName] = update_log[colName] + 1
                    fw.write("PMID:" + str(pmid) + ": " + colName + " is updated from '" + str(old_value) + "' to '" + str(json_data[j_key]) + "'\n")
        elif colName in ['pubmed_abstract_languages', 'pubmed_types']:
            j_key = colName_to_json_key[colName]
            if json_data.get(j_key) and len(json_data[j_key]) > 0:
                if getattr(x, colName) and set(getattr(x, colName)) != set(json_data[j_key]):
                    old_value = getattr(x, colName)
                    setattr(x, colName, json_data[j_key])
                    has_update = has_update + 1
                    update_log[colName] = update_log[colName] + 1
                    fw.write("PMID:" + str(pmid) + ": " + colName + " is updated from " + str(old_value) + " to " + str(json_data[j_key]) + "\n")
        elif colName == 'keywords':
            if json_data.get('keywords'):
                # never delete keywords - only add new one(s)
                old_keywords = x.keywords
                if old_keywords is None:
                    old_keywords = []
                new_keywords = list(set(old_keywords + json_data['keywords']))
                old_keywords.sort()
                new_keywords.sort()
                if old_keywords != new_keywords:
                    setattr(x, colName, new_keywords)
                    has_update = has_update + 1
                    update_log[colName] = update_log[colName] + 1
                    fw.write("PMID:" + str(pmid) + ": " + colName + " is updated from " + str(old_keywords) + " to " + str(new_keywords) + "\n")
        else:
            j_key = colName_to_json_key[colName] if colName_to_json_key.get(colName) else colName
            old_value = getattr(x, colName)
            new_value = json_data.get(j_key)
            if new_value is None:
                continue
            if colName == 'category':
                if old_value:
                    old_value = old_value.replace("ReferenceCategory.", "")
                if new_value.lower() != old_value.lower():
                    setattr(x, colName, new_value)
                    has_update = has_update + 1
                    update_log[colName] = update_log[colName] + 1
                    fw.write("PMID:" + str(pmid) + ": " + colName + " is updated from '" + str(old_value) + "' to '" + str(new_value) + "'\n")
                continue
            if colName == 'pubmed_publication_status' and old_value:
                old_value = old_value.replace("PubMedPublicationStatus.", "")
            if str(new_value) != str(old_value):
                setattr(x, colName, new_value)
                has_update = has_update + 1
                update_log[colName] = update_log[colName] + 1
                fw.write("PMID:" + str(pmid) + ": " + colName + " is updated from '" + str(old_value) + "' to '" + str(new_value) + "'\n")

    if has_update:
        # x.date_updated = date.today()
        db_session.add(x)
        update_log['pmids_updated'].append(pmid)
        log.info(str(count) + " PMID:" + str(pmid) + " Reference table has been updated")
        fw.write("PMID:" + str(pmid) + " Reference table has been updated\n")
    else:
        fw.write("PMID:" + str(pmid) + " No Change in Reference table\n")
        log.info(str(count) + " PMID:" + str(pmid) + " No Change in Reference table")


def update_authors(db_session, fw, pmid, reference_id, author_list_in_db, author_list_in_json, orcid_dict, newly_added_orcid, update_log):
    # If any of the author fields are different, check if any of our ABC authors
    # have a different corresponding_author or first_author, in which case do not
    # update the authors at all (in theory, eventually send a message to a curator,
    # but for now don't do that).  If the first/corr flags have not been changed,
    # then get rid of all the ABC authors, and repopulate them from the PubMed data

    if author_list_in_json is None:
        return []

    authors_in_db = []
    author_list_with_first_or_corresponding_author = []
    if author_list_in_db:
        for x in author_list_in_db:
            if x['first_author'] or x['corresponding_author']:
                author_list_with_first_or_corresponding_author.append((pmid, x['name'], "first_author = " + str(x['first_author']), "corresponding_author = " + str(x['corresponding_author'])))
            affiliations = x['affiliations'] if x['affiliations'] else []
            orcid = x['orcid'] if x['orcid'] else ''
            authors_in_db.append((x['name'], x['first_name'], x['last_name'], x['order'], '|'.join(affiliations), orcid))

    authors_in_json = []
    for x in author_list_in_json:
        orcid = 'ORCID:' + x['orcid'] if x.get('orcid') else ''
        affiliations = x['affiliations'] if x.get('affiliations') else []
        authors_in_json.append((x.get('name', ''), x.get('firstname', ''), x.get('lastname', ''), x.get('authorRank', 0), '|'.join(affiliations), orcid))

    # print ("authors_in_db=", authors_in_db)
    # print ("authors_in_json", authors_in_json)

    if set(authors_in_db) == set(authors_in_json):
        return []

    ## only return / notify if there is any other author info changed
    if len(author_list_with_first_or_corresponding_author) > 0:
        return author_list_with_first_or_corresponding_author

    update_log['author_name'] = update_log['author_name'] + 1
    update_log['pmids_updated'].append(pmid)

    ## deleting authors from database for the given pmid
    for x in db_session.query(AuthorModel).filter_by(reference_id=reference_id).order_by(AuthorModel.order).all():
        name = x.name
        affiliations = x.affiliations if x.affiliations else []
        try:
            db_session.delete(x)
            fw.write("PMID:" + str(pmid) + ": DELETE AUTHOR: " + name + " | '" + '|'.join(affiliations) + "'\n")
        except Exception as e:
            fw.write("PMID:" + str(pmid) + ": DELETE AUTHOR: " + name + " failed: " + str(e) + "\n")

    ## adding authors from pubmed into database
    for x in authors_in_json:
        (name, firstname, lastname, authorRank, affiliations, orcid) = x
        if orcid and orcid not in orcid_dict and orcid not in newly_added_orcid:
            ## not in cross_reference table
            data = {"curie": orcid, "is_obsolete": False}
            try:
                c = CrossReferenceModel(**data)
                db_session.add(c)
                fw.write("PMID:" + str(pmid) + ": INSERT CROSS_REFERENCE: " + orcid + "\n")
                newly_added_orcid.append(orcid)
            except Exception as e:
                fw.write("PMID:" + str(pmid) + ": INSERT CROSS_REFERENCE: " + orcid + " failed: " + str(e) + "\n")
        data = {"reference_id": reference_id,
                "name": name,
                "first_name": firstname,
                "last_name": lastname,
                "order": authorRank,
                "affiliations": affiliations.split('|'),
                "orcid": orcid if orcid else None,
                "first_author": False,
                "corresponding_author": False}
        try:
            x = AuthorModel(**data)
            db_session.add(x)
            fw.write("PMID:" + str(pmid) + ": INSERT AUTHOR: " + name + " | '" + affiliations + "'\n")
        except Exception as e:
            fw.write("PMID:" + str(pmid) + ": INSERT AUTHOR: " + name + " failed: " + str(e) + "\n")

    return []


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
            update_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type)
            update_log['comment_erratum'] = update_log['comment_erratum'] + 1
            update_log['pmids_updated'].append(pmid)
        else:
            insert_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type)
            update_log['comment_erratum'] = update_log['comment_erratum'] + 1
            update_log['pmids_updated'].append(pmid)

    for key in reference_ids_to_comment_correction_type:
        if key in new_reference_ids_to_comment_correction_type:
            continue
        (reference_id_from, reference_id_to) = key
        if reference_id in [reference_id_from, reference_id_to]:
            ## only remove the ones that are associated with given PMIDs
            delete_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type)
            update_log['comment_erratum'] = update_log['comment_erratum'] + 1
            update_log['pmids_updated'].append(pmid)


def insert_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type):

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


def update_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type):

    all = db_session.query(ReferenceCommentAndCorrectionModel).filter_by(reference_id_from=reference_id_from, reference_id_to=reference_id_to).all()

    if len(all) == 0:
        return

    for x in all:
        db_session.delete(x)

    insert_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type)


def delete_comment_correction(db_session, fw, pmid, reference_id_from, reference_id_to, type):

    for x in db_session.query(ReferenceCommentAndCorrectionModel).filter_by(reference_id_from=reference_id_from, reference_id_to=reference_id_to, reference_comment_and_correction_type=type).all():
        try:
            db_session.delete(x)
            fw.write("PMID:" + str(pmid) + ": DELETE CommentsAndCorrections: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + "\n")
        except Exception as e:
            fw.write("PMID:" + str(pmid) + ": DELETE CommentsAndCorrections: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + " failed: " + str(e) + "\n")


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
            insert_mesh_term(db_session, fw, pmid, reference_id, m)

    for m in mesh_terms_in_db:
        if m in mesh_terms_in_json:
            continue
        else:
            delete_mesh_term(db_session, fw, pmid, reference_id, m)

    update_log['mesh_term'] = update_log['mesh_term'] + 1
    update_log['pmids_updated'].append(pmid)


def insert_mesh_term(db_session, fw, pmid, reference_id, terms):

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


def delete_mesh_term(db_session, fw, pmid, reference_id, terms):

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


def update_cross_reference(db_session, fw, pmid, reference_id, doi_db, doi_list_in_db, doi_json, pmcid_db, pmcid_list_in_db, pmcid_json, update_log):

    if doi_json is None and pmcid_json is None:
        return

    ## take care of DOI
    if doi_json and (doi_db is None or doi_json != doi_db) and doi_json in doi_list_in_db:
        fw.write("PMID:" + str(pmid) + ": DOI:" + doi_json + " is in the database for another paper.\n")
    else:
        if doi_json and doi_json != doi_db:
            if doi_db is None:
                insert_doi(db_session, fw, pmid, reference_id, doi_json)
            else:
                update_doi(db_session, fw, pmid, reference_id, doi_db, doi_json)
            update_log['doi'] = update_log['doi'] + 1
            update_log['pmids_updated'].append(pmid)

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
            update_pmcid(db_session, fw, pmid, reference_id, pmcid_db, pmcid_json)
        else:
            insert_pmcid(db_session, fw, pmid, reference_id, pmcid_json)

        update_log['pmcid'] = update_log['pmcid'] + 1
        update_log['pmids_updated'].append(pmid)


def update_doi(db_session, fw, pmid, reference_id, old_doi, new_doi):

    try:
        x = db_session.query(CrossReferenceModel).filter_by(reference_id=reference_id).filter(CrossReferenceModel.curie == 'DOI:' + old_doi).one_or_none()
        if x is None:
            return
        x.curie = new_doi
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": UPDATE DOI from " + old_doi + " to " + new_doi + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": UPDATE DOI from " + old_doi + " to " + new_doi + " failed: " + str(e) + "\n")


def insert_doi(db_session, fw, pmid, reference_id, doi):

    ## for some reason, we need to add this check to make sure it is not in db
    x = db_session.query(CrossReferenceModel).filter_by(curie="DOI:" + doi).one_or_none()
    if x:
        if x.reference_id != reference_id:
            log.info("The DOI:" + doi + " is associated with two papers: reference_ids=" + str(reference_id) + ", " + str(x.reference_id))
        return

    data = {"curie": "DOI:" + doi, "reference_id": reference_id, "is_obsolete": False}
    try:
        x = CrossReferenceModel(**data)
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": INSERT DOI:" + doi + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": INSERT DOI:" + doi + " failed: " + str(e) + "\n")


def update_pmcid(db_session, fw, pmid, reference_id, old_pmcid, new_pmcid):

    try:
        x = db_session.query(CrossReferenceModel).filter_by(reference_id=reference_id).filter(CrossReferenceModel.curie == 'PMCID:' + old_pmcid).one_or_none()
        if x is None:
            return
        x.curie = new_pmcid
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": UPDATE PMCID from " + old_pmcid + " to " + new_pmcid + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": UPDATE PMCID from " + old_pmcid + " to " + new_pmcid + " failed: " + str(e) + "\n")


def insert_pmcid(db_session, fw, pmid, reference_id, pmcid):

    ## for some reason, we need to add this check to make sure it is not in db
    x = db_session.query(CrossReferenceModel).filter_by(curie="PMCID:" + pmcid).one_or_none()
    if x:
        if x.reference_id != reference_id:
            log.info("The PMCID:" + pmcid + " is associated with two papers: reference_ids=" + str(reference_id) + ", " + str(x.reference_id))
        return

    data = {"curie": "PMCID:" + pmcid, "reference_id": reference_id, "is_obsolete": False}
    try:
        x = CrossReferenceModel(**data)
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": INSERT PMCID:" + pmcid + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": INSERT PMCID:" + pmcid + " failed: " + str(e) + "\n")


def get_md5sum(md5sum_path):

    file = md5sum_path + "md5sum"
    pmid_to_md5sum = {}
    if path.exists(file):
        f = open(file)
        for line in f:
            pieces = line.strip().split("\t")
            pmid_to_md5sum[pieces[0]] = pieces[1]
    return pmid_to_md5sum


def set_paths():

    load_dotenv()

    base_path = environ.get('XML_PATH', "")
    xml_path = base_path + "pubmed_xml/"
    json_path = base_path + "pubmed_json/"
    old_xml_path = base_path + "pubmed_xml/old/"
    old_json_path = base_path + "pubmed_json/old/"
    log_path = base_path + 'pubmed_search_logs/'
    if environ.get('LOG_PATH'):
        log_path = path.join(environ['LOG_PATH'], 'pubmed_update/')
    log_url = None
    if environ.get('LOG_URL'):
        log_url = path.join(environ['LOG_URL'], 'pubmed_update/')
    email_recipients = None
    if environ.get('CRONTAB_EMAIL'):
        email_recipients = environ['CRONTAB_EMAIL']
    sender_email = None
    if environ.get('SENDER_EMAIL'):
        sender_email = environ['SENDER_EMAIL']
    sender_password = None
    if environ.get('SENDER_PASSWORD'):
        sender_password = environ['SENDER_PASSWORD']
    reply_to = sender_email
    if environ.get('REPLY_TO'):
        reply_to = environ['REPLY_TO']
    if not path.exists(xml_path):
        makedirs(xml_path)
    if not path.exists(json_path):
        makedirs(json_path)
    if not path.exists(old_xml_path):
        makedirs(old_xml_path)
    if not path.exists(old_json_path):
        makedirs(old_json_path)
    if not path.exists(log_path):
        makedirs(log_path)

    return (xml_path, json_path, old_xml_path, old_json_path, log_path, log_url,
            email_recipients, sender_email, sender_password, reply_to)


def close_no_update(fw, mod, email_subject, email_recipients, sender_email, sender_password, reply_to, log_dir):

    log.info("No new update in PubMed.")
    fw.write("No new update in PubMed.\n")
    log.info("DONE!\n")
    fw.write("DONE!\n")

    if mod is None:
        return

    email_message = None
    if mod:
        if mod == "NONE":
            email_message = "No new update found in PubMed for the papers that are not associated with a mod"
        else:
            email_message = "No new update found in PubMed for " + mod + " papers"
    else:
        email_message = "No new update found in PubMed"
    email_message = "<strong>" + email_message + "</strong>"

    (status, message) = send_email(email_subject, email_recipients, email_message, sender_email,
                                   sender_password, reply_to)
    if status == 'error':
        fw.write("Failed sending email to slack: " + message + "\n")
        log.info("Failed sending email to slack: " + message + "\n")


def write_summary(fw, mod, update_log, authors_with_first_or_corresponding_flag, not_found_xml_list, log_url, log_dir, email_subject, email_recipients, sender_email, sender_password, reply_to):

    message = None
    if mod:
        if mod == 'NONE':
            message = "Updating Summary for pubmed papers that are not associated with a mod..."
        else:
            message = "Updating Summary for " + mod + " pubmed papers..."
    else:
        message = "Updating Summary..."

    fw.write(message + "\n")
    log.info(message)
    email_message = "<h3>" + message + "</h3>"

    for field_name in field_names_to_report:
        if field_name == 'pmids_updated':
            continue
        log.info("Paper(s) with " + field_name + " updated:" + str(update_log[field_name]))
        fw.write("Paper(s) with " + field_name + " updated:" + str(update_log[field_name]) + "\n")
        email_message = email_message + "Paper(s) with <b>" + field_name + "</b> updated:" + str(update_log[field_name]) + "<br>"
        # if field_name == 'author_name':
        #    email_message = email_message + "<br>"

    pmids_updated = list(set(update_log['pmids_updated']))

    if len(pmids_updated) == 0:
        email_message = email_message + "<strong>No papers updated.</strong><p>"
    else:
        if len(pmids_updated) <= 100:
            email_message = email_message + "<strong>Total " + str(len(pmids_updated)) + " pubmed paper(s) have been updated</strong>. PMID(s):<br>" + ", ".join(pmids_updated) + "<p>"
        else:
            email_message = email_message + "<strong>Total " + str(len(pmids_updated)) + " pubmed paper(s) have been updated</strong>. PMID(s):<br>" + ", ".join(pmids_updated[0:100]) + "<br>See log file for the full updated PMID list and update details.<p>"
        if log_url:
            email_message = email_message + "<b>The log files are available at: </b><a href=" + log_url + ">" + log_url + "</a><p>"

        fw.write("Total " + str(len(pmids_updated)) + " pubmed paper(s) have been updated. See the following PMID list:\n" + ", ".join(pmids_updated) + "\n")

    if len(authors_with_first_or_corresponding_flag) > 0:

        log.info("Following PMID(s) with author info updated in PubMed, but they have first_author or corresponding_author flaged in the database")
        fw.write("Following PMID(s) with author info updated in PubMed, but they have first_author or corresponding_author flaged in the database\n")
        email_message = email_message + "Following PMID(s) with author info updated in PubMed, but they have first_author or corresponding_author flaged in the database<p>"

        for x in authors_with_first_or_corresponding_flag:
            (pmid, name, first_author, corresponding_author) = x
            log.info("PMID:" + str(pmid) + ": name = " + name + ", " + first_author + ", " + corresponding_author)
            fw.write("PMID:" + str(pmid) + ": name = " + name + ", " + first_author + ", " + corresponding_author + "\n")
            email_message = email_message + "PMID:" + str(pmid) + ": name =" + name + ", first_author=" + first_author + ", corresponding_author=" + corresponding_author + "<br>"

    if len(not_found_xml_list) > 0:
        i = 0
        for pmid in not_found_xml_list:
            if not str(pmid).isdigit():
                continue
            if i == 0:
                log.info("Following PMID(s) are missing while updating pubmed data")
                fw.write("Following PMID(s) are missing while updating pubmed data")
                email_message = email_message + "<p>Following PMID(s) are missing while updating pubmed data:<p>"
            i += 1
            log.info("PMID:" + str(pmid))
            fw.write("PMID:" + str(pmid) + "\n")
            email_message = email_message + "PMID:" + str(pmid) + "<br>"
        email_message = email_message + "<p>"

    if mod:
        email_message = email_message + "DONE!<p>"
        (status, message) = send_email(email_subject, email_recipients, email_message,
                                       sender_email, sender_password, reply_to)
        if status == 'error':
            fw.write("Failed sending email to slack: " + message + "\n")
            log.info("Failed sending email to slack: " + message + "\n")


def generate_pmids_with_info(pmids_all, old_md5sum, new_md5sum, pmid_to_reference_id):

    reference_id_list = []
    for pmid in pmids_all:
        if pmid not in new_md5sum:
            continue
        if pmid not in old_md5sum:
            if pmid in pmid_to_reference_id:
                reference_id_list.append(pmid_to_reference_id[pmid])
        elif new_md5sum[pmid] != old_md5sum[pmid]:
            if pmid in pmid_to_reference_id:
                reference_id_list.append(pmid_to_reference_id[pmid])
    return reference_id_list


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


def get_author_data(db_connection, mod, reference_id_list):

    reference_id_to_authors = {}

    if mod and len(reference_id_list) > query_cutoff:
        author_limit = 500000
        for index in range(500):
            offset = index * author_limit
            rs = db_connection.execute("select a.reference_id, a.orcid, a.first_author, a.order, a.corresponding_author, a.name, a.affiliations, a.first_name, a.last_name from author a, mod_corpus_association mca, mod m where a.reference_id = mca.reference_id and mca.mod_id = m.mod_id and m.abbreviation = '" + mod + "' order by a.reference_id, a.order limit " + str(author_limit) + " offset " + str(offset))
            rows = rs.fetchall()
            if len(rows) == 0:
                break
            for x in rows:
                adding_author_row(x, reference_id_to_authors)
    elif reference_id_list and len(reference_id_list) > 0:
        # name & order are keywords in postgres so have use alias 'a' for table name
        ref_ids = ", ".join([str(x) for x in reference_id_list])
        raw_sql = "SELECT a.reference_id, a.orcid, a.first_author, a.order, a.corresponding_author, a.name, a.affiliations, a.first_name, a.last_name FROM author a WHERE reference_id IN (" + ref_ids + ") order by a.reference_id, a.order"
        rs = db_connection.execute(raw_sql)
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


def get_mesh_term_data(db_connection, mod, reference_id_list):

    reference_id_to_mesh_terms = {}

    if mod and len(reference_id_list) > query_cutoff:
        # the query is taking too long to return so break up to query database
        # multiple times to keep session alive
        mesh_limit = 1000000
        for index in range(50):
            offset = index * mesh_limit
            rs = db_connection.execute("select md.reference_id, md.heading_term, md.qualifier_term from mesh_detail md, mod_corpus_association mca, mod m where md.reference_id = mca.reference_id and mca.mod_id = m.mod_id and m.abbreviation = '" + mod + "' order by md.mesh_detail_id limit " + str(mesh_limit) + " offset " + str(offset))
            rows = rs.fetchall()
            if len(rows) == 0:
                break
            for x in rows:
                adding_mesh_term_row(x, reference_id_to_mesh_terms)
    elif reference_id_list and len(reference_id_list) > 0:
        ref_ids = ", ".join([str(x) for x in reference_id_list])
        raw_sql = "SELECT reference_id, heading_term, qualifier_term FROM mesh_detail WHERE reference_id IN (" + ref_ids + ")"
        rs = db_connection.execute(raw_sql)
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


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()

    group.add_argument('-m', '--mod', action='store', type=str, help='MOD to update',
                       choices=['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XB', 'NONE'])
    group.add_argument('-p', '--pmids', action='store', help="a list of '|' delimited pmid list")

    args = vars(parser.parse_args())
    if not any(args.values()):
        parser.error('No arguments provided.')
    update_data(args['mod'], args['pmids'])
