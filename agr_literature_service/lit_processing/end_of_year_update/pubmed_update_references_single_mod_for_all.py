import argparse
import logging
from os import environ, makedirs, path
from dotenv import load_dotenv
from datetime import datetime
import json
import time

from agr_literature_service.api.models import ModModel, ReferenceModel, ModCorpusAssociationModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import \
    generate_json
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    load_database_md5data, save_database_md5data
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_author_data, get_mesh_term_data, get_cross_reference_data, \
    get_cross_reference_data_for_resource, get_reference_relation_data, get_journal_data, \
    get_pmid_to_reference_id_for_papers_not_associated_with_mod, get_pmid_to_reference_id
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    update_authors, update_reference_relations, update_mesh_terms, update_cross_reference
from agr_literature_service.lit_processing.utils.report_utils import \
    write_log_and_send_pubmed_update_report, \
    write_log_and_send_pubmed_no_update_report
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

refColName_to_update = ['title', 'volume', 'issue_name', 'page_range', 'citation',
                        'abstract', 'pubmed_types', 'pubmed_publication_status',
                        'keywords', 'category', 'plain_language_abstract',
                        'pubmed_abstract_languages', 'language', 'date_published',
                        'date_published_start', 'date_published_end',
                        'date_arrived_in_pubmed', 'date_last_modified_in_pubmed',
                        'publisher', 'resource_id']

field_names_to_report = refColName_to_update + ['doi', 'pmcid', 'author_name', 'journal',
                                                'comment_erratum', 'mesh_term',
                                                'pmids_updated']
# limit = 1000
limit = 500
max_rows_per_commit = 250
download_xml_max_size = 5000
batch_update_size = 25000
query_cutoff = 500
sleep_time = 10

init_tmp_dir()


def update_data(mod, start_offset):  # noqa: C901 pragma: no cover

    db_session = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    # datestamp = str(date.today()).replace("-", "")

    (xml_path, json_path, old_xml_path, old_json_path, log_path, log_url,
     email_recipients, sender_email, sender_password, reply_to) = set_paths()

    email_subject = "PubMed Paper Update Report"
    if mod and mod != 'NONE':
        email_subject = mod + " " + email_subject

    log_file = log_path + "update_pubmed_papers_"
    if mod:
        log_file = log_file + mod + ".log"
    else:
        log_file = log_file + ".log"

    fw = open(log_file, "w")

    fw.write(str(datetime.now()) + "\n")
    fw.write("Getting data from the database...\n")
    logger.info("Getting data from the database...")

    pmid_to_reference_id = {}
    reference_id_to_pmid = {}
    pmids_all = []
    if mod == 'NONE':
        get_pmid_to_reference_id_for_papers_not_associated_with_mod(db_session,
                                                                    pmid_to_reference_id,
                                                                    reference_id_to_pmid)
    else:
        get_pmid_to_reference_id(db_session, mod, pmid_to_reference_id, reference_id_to_pmid)
    pmids_all = list(pmid_to_reference_id.keys())

    pmids_all.sort()

    db_session.close()

    update_log = {}
    for field_name in field_names_to_report:
        if field_name == 'pmids_updated':
            update_log[field_name] = []
        else:
            update_log[field_name] = 0

    if start_offset == 0:

        fw.write(str(datetime.now()) + "\n")
        fw.write("Downloading pubmed xml files for " + str(len(pmids_all)) + " PMIDs...\n")
        logger.info("Downloading pubmed xml files for " + str(len(pmids_all)) + " PMIDs...")

        if len(pmids_all) > download_xml_max_size:
            for index in range(0, len(pmids_all), download_xml_max_size):
                pmids_slice = pmids_all[index:index + download_xml_max_size]
                download_pubmed_xml(pmids_slice)
                time.sleep(sleep_time)
        else:
            download_pubmed_xml(pmids_all)

        fw.write(str(datetime.now()) + "\n")
        fw.write("Generating json files...\n")
        logger.info("Generating json files...")

        not_found_xml_set = set()
        generate_json(pmids_all, [], not_found_xml_set)

    md5dict = load_database_md5data(['PMID'])
    old_md5sum = md5dict['PMID']
    new_md5sum = get_md5sum(json_path)

    reference_id_list = []
    pmid_to_md5sum = {}
    if start_offset == 0:
        pmid_to_md5sum = generate_pmids_with_info(pmids_all, old_md5sum, new_md5sum, pmid_to_reference_id)
    reference_id_list = list(reference_id_to_pmid.keys())

    if len(reference_id_list) == 0:
        write_log_and_send_pubmed_no_update_report(fw, mod, email_subject)
        return

    fw.write(str(datetime.now()) + "\n")
    fw.write("Updating database...\n")
    logger.info("Updating database...")

    pmids_with_json_updated = []
    bad_date_published = {}
    authors_with_first_or_corresponding_flag = update_database(fw, mod, start_offset,
                                                               reference_id_list,
                                                               reference_id_to_pmid,
                                                               pmid_to_reference_id,
                                                               update_log, new_md5sum,
                                                               old_md5sum, json_path,
                                                               pmids_with_json_updated,
                                                               bad_date_published)

    # to not report not_found_xml_list for now, but log it
    # logger.info("not_found_xml_list count = " + str(len(not_found_xml_list)))
    not_found_xml_list = []
    write_log_and_send_pubmed_update_report(fw, mod, field_names_to_report, update_log,
                                            bad_date_published,
                                            authors_with_first_or_corresponding_flag,
                                            not_found_xml_list, log_url, log_path,
                                            email_subject)

    if start_offset == 0:
        md5dict = {'PMID': pmid_to_md5sum}
        save_database_md5data(md5dict)

        """
        if environ.get('ENV_STATE') and environ['ENV_STATE'] == 'prod':
            fw.write(str(datetime.now()) + "\n")
            fw.write("Uploading xml files to s3...\n")
            logger.info("Uploading xml files to s3...")
            for pmid in pmids_with_json_updated:
                logger.info("uploading xml file for PMID:" + pmid + " to s3")
                upload_xml_file_to_s3(pmid, 'latest')
        """

    logger.info("DONE!\n\n")
    fw.write(str(datetime.now()) + "\n")
    fw.write("DONE!\n")
    fw.close()


def update_database(fw, mod, start_offset, reference_id_list, reference_id_to_pmid, pmid_to_reference_id,
                    update_log, new_md5sum, old_md5sum, json_path, pmids_with_json_updated,
                    bad_date_published):   # noqa: C901

    ## 1. do nothing if a field has no value in pubmed xml/json
    ##    so won't delete whatever in the database
    ## 2. update/add the field if a field has a new value in pubmed xml/json

    ## start a database session
    db_session = create_postgres_session(False)

    ## reference_id => a list of author name in order
    fw.write("Getting author info from database...\n")
    logger.info("Getting author info from database...")
    reference_id_to_authors = get_author_data(db_session, mod, reference_id_list, query_cutoff)

    ## (reference_id_from, reference_id_to) => a list of reference_reference_relation_type
    fw.write("Getting reference_relation info from database...\n")
    logger.info("Getting reference_relation info from database...")
    reference_ids_to_reference_relation_type = get_reference_relation_data(db_session, mod,
                                                                           reference_id_list)

    ## reference_id => a list of mesh_terms in order
    fw.write("Getting mesh_term info from database...\n")
    logger.info("Getting mesh_term info from database...")
    reference_id_to_mesh_terms = get_mesh_term_data(db_session, mod, reference_id_list, query_cutoff)

    ## reference_id => doi, reference_id =>pmcid
    fw.write("Getting DOI/PMCID info from database...\n")
    logger.info("Getting DOI/PMCID info from database...")
    (reference_id_to_doi, reference_id_to_pmcid) = get_cross_reference_data(db_session, mod,
                                                                            reference_id_list)

    ## journal => resource_id
    fw.write("Getting journal info from database...\n")
    logger.info("Getting journal info from database...")
    journal_to_resource_id = get_journal_data(db_session)

    ## resource_id => issn, resource_id => nlm
    fw.write("Getting ISSN/NLM info from database...\n")
    logger.info("Getting ISSN/NLM info from database...")
    (resource_id_to_issn, resource_id_to_nlm) = get_cross_reference_data_for_resource(db_session)

    db_session.close()

    count = 0
    offset = start_offset

    ## for some reason, it needs to return from recursive function...
    authors_with_first_or_corresponding_flag = []

    authors_with_first_or_corresponding_flag = \
        update_reference_data_batch(fw, mod, reference_id_list, reference_id_to_pmid,
                                    pmid_to_reference_id, reference_id_to_authors,
                                    reference_ids_to_reference_relation_type,
                                    reference_id_to_mesh_terms, reference_id_to_doi,
                                    reference_id_to_pmcid, journal_to_resource_id,
                                    resource_id_to_issn, resource_id_to_nlm, count,
                                    authors_with_first_or_corresponding_flag, json_path,
                                    pmids_with_json_updated, bad_date_published,
                                    update_log, offset, start_offset)

    return authors_with_first_or_corresponding_flag


def update_reference_data_batch(fw, mod, reference_id_list, reference_id_to_pmid, pmid_to_reference_id,
                                reference_id_to_authors, reference_ids_to_reference_relation_type,
                                reference_id_to_mesh_terms, reference_id_to_doi, reference_id_to_pmcid,
                                journal_to_resource_id, resource_id_to_issn, resource_id_to_nlm, count,
                                authors_with_first_or_corresponding_flag, json_path,
                                pmids_with_json_updated, bad_date_published, update_log,
                                offset, start_offset):  # noqa: C901 pragma: no cover

    if offset >= start_offset + batch_update_size:
        return authors_with_first_or_corresponding_flag

    ## only update 3000 references per session (set in max_rows_per_db_session)
    ## just in case the database get disconnected during the update process
    db_session = create_postgres_session(False)

    fw.write("Getting data from Reference table...\n")
    if mod and len(reference_id_list) > query_cutoff:
        logger.info("Getting data from Reference table...limit=" + str(limit) + ", offset=" + str(offset))
    else:
        logger.info("Getting data from Reference table...")

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
                               bad_date_published, update_log, count)

        ## update cross_reference table for reference
        if json_data.get('doi') or json_data.get('pmc'):
            update_cross_reference(db_session, fw, pmid, x.reference_id,
                                   reference_id_to_doi.get(x.reference_id),
                                   doi_list_in_db,
                                   json_data.get('doi'),
                                   reference_id_to_pmcid.get(x.reference_id),
                                   pmcid_list_in_db,
                                   json_data.get('pmc'), update_log, logger)

        ## update author table
        # author_list_with_first_or_corresponding_author =
        # a list of (pmid, name, first_author, corresponding_author)
        authors = update_authors(db_session, x.reference_id,
                                 reference_id_to_authors.get(x.reference_id),
                                 json_data.get('authors'),
                                 None, fw, pmid, update_log)

        authors_with_first_or_corresponding_flag = authors_with_first_or_corresponding_flag + authors

        ## update erences_single_mod.py:    get_cross_reference_data_for_resource, get_reference_relation_data, get_journal_data, \ table
        update_reference_relations(db_session, fw, pmid, x.reference_id, pmid_to_reference_id,
                                   reference_ids_to_reference_relation_type,
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
        authors_with_first_or_corresponding_flag = \
            update_reference_data_batch(fw, mod, reference_id_list, reference_id_to_pmid,
                                        pmid_to_reference_id, reference_id_to_authors,
                                        reference_ids_to_reference_relation_type,
                                        reference_id_to_mesh_terms, reference_id_to_doi,
                                        reference_id_to_pmcid, journal_to_resource_id,
                                        resource_id_to_issn, resource_id_to_nlm, count,
                                        authors_with_first_or_corresponding_flag, json_path,
                                        pmids_with_json_updated, bad_date_published,
                                        update_log, offset, start_offset)

    return authors_with_first_or_corresponding_flag


def update_reference_table(db_session, fw, pmid, x, json_data, new_resource_id, journal_title, authors, bad_date_published, update_log, count):   # noqa: C901

    colName_to_json_key = {'issue_name': 'issueName',
                           'page_range': 'pages',
                           'date_published': 'datePublished',
                           'date_published_start': 'datePublishedStart',
                           'date_published_end': 'datePublishedEnd',
                           'pubmed_publication_status': 'publicationStatus',
                           'date_last_modified_in_pubmed': 'dateLastModified',
                           'date_arrived_in_pubmed': 'dateArrivedInPubmed',
                           'plain_language_abstract': 'plainLanguageAbstract',
                           'pubmed_abstract_languages': 'pubmedAbstractLanguages',
                           'pubmed_types': 'pubMedType',
                           'category': 'allianceCategory'}

    has_update = 0
    for colName in refColName_to_update:
        if colName == 'resource_id' and new_resource_id and new_resource_id != x.resource_id:
            fw.write("PMID:" + str(pmid) + ": resource_id is updated from " + str(x.resource_id) + " to " + str(new_resource_id) + "\n")
            x.resource_id = new_resource_id
            has_update = has_update + 1
            update_log['journal'] = update_log['journal'] + 1
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
            if colName == 'date_published':
                if new_value and json_data.get('datePublishedStart') is None:
                    bad_date_published[pmid] = new_value
            elif colName in ['date_published_start', 'date_published_end']:
                if old_value:
                    old_value = str(old_value)[0:10]
                if new_value:
                    new_value = str(new_value)[0:10]
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
        logger.info(str(count) + " PMID:" + str(pmid) + " Reference table has been updated")
        fw.write("PMID:" + str(pmid) + " Reference table has been updated\n")
    else:
        fw.write("PMID:" + str(pmid) + " No Change in Reference table\n")
        logger.info(str(count) + " PMID:" + str(pmid) + " No Change in Reference table")


def get_md5sum(md5sum_path):  # pragma: no cover

    file = md5sum_path + "md5sum"
    pmid_to_md5sum = {}
    if path.exists(file):
        f = open(file)
        for line in f:
            pieces = line.strip().split("\t")
            pmid_to_md5sum["PMID:" + pieces[0]] = pieces[1]
    return pmid_to_md5sum


def set_paths():  # pragma: no cover

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


def generate_pmids_with_info(pmids_all, old_md5sum, new_md5sum, pmid_to_reference_id):

    pmid_to_md5sum = {}
    for pmid in pmids_all:
        pmid_with_prefix = "PMID:" + pmid
        if pmid_with_prefix not in new_md5sum:
            continue
        if old_md5sum.get(pmid_with_prefix) and new_md5sum[pmid_with_prefix] == old_md5sum[pmid_with_prefix]:
            continue
        pmid_to_md5sum[pmid_with_prefix] = new_md5sum[pmid_with_prefix]
    return pmid_to_md5sum


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('-m', '--mod', action='store', type=str, help='MOD to update',
                        choices=['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XB', 'NONE'])
    parser.add_argument('-o', '--offset', action='store', type=int, help="a number for offset")

    args = vars(parser.parse_args())
    if not any(args.values()):
        parser.error('No arguments provided.')
    if not args['mod']:
        parser.error('No mod provided.')
    if not args['offset']:
        args['offset'] = 0
    update_data(args['mod'], args['offset'])
