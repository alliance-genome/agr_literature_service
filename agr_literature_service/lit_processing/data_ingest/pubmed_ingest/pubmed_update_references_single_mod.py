import argparse
import logging
from os import environ, makedirs, path
from dotenv import load_dotenv
from datetime import datetime
import json
import time

from agr_literature_service.api.models import ModModel, ReferenceModel, ModCorpusAssociationModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_resources_nlm import \
    update_resource_pubmed_nlm
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import \
    generate_json
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    load_database_md5data, save_database_md5data
from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_author_data, get_mesh_term_data, get_cross_reference_data, \
    get_cross_reference_data_for_resource, get_reference_relation_data, get_journal_data, \
    get_reference_ids_by_pmids, get_pmid_to_reference_id_for_papers_not_associated_with_mod, \
    get_pmid_to_reference_id
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    update_authors, update_reference_relations, update_mesh_terms, update_cross_reference
from agr_literature_service.lit_processing.utils.report_utils import \
    write_log_and_send_pubmed_update_report, \
    write_log_and_send_pubmed_no_update_report
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

logging.basicConfig(format='%(message)s')
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

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
query_cutoff = 500
sleep_time = 10

init_tmp_dir()


# def update_data(mod, pmids, md5dict=None, newly_added_pmids=None):  # noqa: C901 pragma: no cover
def update_data(mod, pmids, resourceUpdated=None):  # noqa: C901 pragma: no cover

    if resourceUpdated is None:
        update_resource_pubmed_nlm

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
    log.info("Getting data from the database...")

    pmid_to_reference_id = {}
    reference_id_to_pmid = {}
    pmids_all = []
    if mod and pmids is None:
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

    fw.write(str(datetime.now()) + "\n")
    fw.write("Downloading pubmed xml files for " + str(len(pmids_all)) + " PMIDs...\n")
    log.info("Downloading pubmed xml files for " + str(len(pmids_all)) + " PMIDs...")

    if resourceUpdated is None:
        if len(pmids_all) > download_xml_max_size:
            for index in range(0, len(pmids_all), download_xml_max_size):
                pmids_slice = pmids_all[index:index + download_xml_max_size]
                download_pubmed_xml(pmids_slice)
                time.sleep(sleep_time)
        else:
            download_pubmed_xml(pmids_all)

    fw.write(str(datetime.now()) + "\n")

    md5dict = load_database_md5data(['PMID'])
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

    # if newly_added_pmids:
    #    for pmid in newly_added_pmids:
    #        not_found_xml_set.discard(pmid)
    # not_found_xml_list = list(not_found_xml_set)

    new_md5sum = get_md5sum(json_path)

    reference_id_list = []
    pmid_to_md5sum = {}
    if mod:
        (reference_id_list, pmid_to_md5sum) = generate_pmids_with_info(pmids_all, old_md5sum, new_md5sum, pmid_to_reference_id)
    else:
        reference_id_list = list(reference_id_to_pmid.keys())

    if len(reference_id_list) == 0:
        write_log_and_send_pubmed_no_update_report(fw, mod, email_subject)
        return

    fw.write(str(datetime.now()) + "\n")
    fw.write("Updating database...\n")
    log.info("Updating database...")

    pmids_with_json_updated = []
    ## the following two lists are storing the papers that have data updated from PubMed
    pmids_with_pub_status_changed = {}
    pmids_with_no_pub_status_changed = {}
    bad_date_published = {}
    try:
        authors_with_first_or_corresponding_flag = update_database(fw, mod,
                                                               reference_id_list,
                                                               reference_id_to_pmid,
                                                               pmid_to_reference_id,
                                                               update_log, new_md5sum,
                                                               old_md5sum, json_path,
                                                               pmids_with_json_updated,
                                                               pmids_with_pub_status_changed,
                                                               pmids_with_no_pub_status_changed,
                                                               bad_date_published)

    except Exception as e:
        log.info(f"Error updating data for {mod}: {e}")
        return

    # to not report not_found_xml_list for now, but log it
    # log.info("not_found_xml_list count = " + str(len(not_found_xml_list)))
    not_found_xml_list = []
    write_log_and_send_pubmed_update_report(fw, mod, field_names_to_report, update_log,
                                            bad_date_published,
                                            authors_with_first_or_corresponding_flag,
                                            not_found_xml_list, log_url, log_path,
                                            email_subject, pmids_with_pub_status_changed,
                                            pmids_with_no_pub_status_changed)

    md5dict = {'PMID': pmid_to_md5sum}
    save_database_md5data(md5dict)

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


def update_database(fw, mod, reference_id_list, reference_id_to_pmid, pmid_to_reference_id, update_log, new_md5sum, old_md5sum, json_path, pmids_with_json_updated, pmids_with_pub_status_changed, pmids_with_no_pub_status_changed, bad_date_published):   # noqa: C901

    ## 1. do nothing if a field has no value in pubmed xml/json
    ##    so won't delete whatever in the database
    ## 2. update/add the field if a field has a new value in pubmed xml/json
    ##
    ## 2024-02-07 fixed the script to update everything based on the data from PubMed

    ## start a database session
    db_session = create_postgres_session(False)

    ## reference_id => a list of author name in order
    fw.write("Getting author info from database...\n")
    log.info("Getting author info from database...")
    reference_id_to_authors = get_author_data(db_session, mod, reference_id_list, query_cutoff)

    ## (reference_id_from, reference_id_to) => a list of reference_reference_relation_type
    fw.write("Getting reference_relation info from database...\n")
    log.info("Getting reference_relation info from database...")
    reference_ids_to_reference_relation_type = get_reference_relation_data(db_session, mod,
                                                                           reference_id_list)

    ## reference_id => a list of mesh_terms in order
    fw.write("Getting mesh_term info from database...\n")
    log.info("Getting mesh_term info from database...")
    reference_id_to_mesh_terms = get_mesh_term_data(db_session, mod, reference_id_list, query_cutoff)

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

    count = 0
    offset = 0

    ## for some reason, it needs to return from recursive function...
    authors_with_first_or_corresponding_flag = []

    authors_with_first_or_corresponding_flag = update_reference_data_batch(fw, mod, reference_id_list,
                                                                           reference_id_to_pmid,
                                                                           pmid_to_reference_id,
                                                                           reference_id_to_authors,
                                                                           reference_ids_to_reference_relation_type,
                                                                           reference_id_to_mesh_terms,
                                                                           reference_id_to_doi,
                                                                           reference_id_to_pmcid,
                                                                           journal_to_resource_id,
                                                                           resource_id_to_issn,
                                                                           resource_id_to_nlm,
                                                                           old_md5sum,
                                                                           new_md5sum,
                                                                           count,
                                                                           authors_with_first_or_corresponding_flag,
                                                                           json_path,
                                                                           pmids_with_json_updated,
                                                                           pmids_with_pub_status_changed,
                                                                           pmids_with_no_pub_status_changed,
                                                                           bad_date_published,
                                                                           update_log,
                                                                           offset)

    return authors_with_first_or_corresponding_flag


def update_reference_data_batch(fw, mod, reference_id_list, reference_id_to_pmid,
                                pmid_to_reference_id, reference_id_to_authors,
                                reference_ids_to_reference_relation_type,
                                reference_id_to_mesh_terms, reference_id_to_doi,
                                reference_id_to_pmcid, journal_to_resource_id,
                                resource_id_to_issn, resource_id_to_nlm,
                                old_md5sum, new_md5sum, count,
                                authors_with_first_or_corresponding_flag, json_path,
                                pmids_with_json_updated, pmids_with_pub_status_changed,
                                pmids_with_no_pub_status_changed, bad_date_published,
                                update_log, offset):  # noqa: C901 pragma: no cover

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
            db_session.rollback()
            # db_session.commit()
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

        try:
            pub_status_changed = update_reference_table(db_session, fw, pmid, x, json_data,
                                                    new_resource_id, journal_title,
                                                    reference_id_to_authors.get(x.reference_id),
                                                    bad_date_published, pmids_with_pub_status_changed,
                                                    pmids_with_no_pub_status_changed,
                                                    update_log, count)
        except Exception as e:
            log.info(f"PMID:{pmid}: Error occurred when updating reference table: {e}")
            
        ## update cross_reference table for reference
        if json_data.get('doi') or json_data.get('pmc'):
            update_cross_reference(db_session, fw, pmid, x.reference_id,
                                   reference_id_to_doi.get(x.reference_id),
                                   doi_list_in_db,
                                   json_data.get('doi'),
                                   reference_id_to_pmcid.get(x.reference_id),
                                   pmcid_list_in_db,
                                   json_data.get('pmc'),
                                   pub_status_changed,
                                   pmids_with_pub_status_changed,
                                   pmids_with_no_pub_status_changed,
                                   update_log, log)

        ## update author table
        # author_list_with_first_or_corresponding_author =
        # a list of (pmid, name, first_author, corresponding_author)
        authors = update_authors(db_session, x.reference_id,
                                 reference_id_to_authors.get(x.reference_id),
                                 json_data.get('authors'),
                                 None, fw, pmid, update_log)

        authors_with_first_or_corresponding_flag = authors_with_first_or_corresponding_flag + authors

        ## update comments/corrections
        update_reference_relations(db_session, fw, pmid, x.reference_id, pmid_to_reference_id,
                                   reference_ids_to_reference_relation_type,
                                   json_data.get('commentsCorrections'), update_log)

        ## update mesh_detail table
        update_mesh_terms(db_session, fw, pmid, x.reference_id,
                          reference_id_to_mesh_terms.get(x.reference_id),
                          json_data.get('meshTerms'), update_log)

    db_session.rollback()
    # db_session.commit()
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
                                                                               reference_ids_to_reference_relation_type,
                                                                               reference_id_to_mesh_terms,
                                                                               reference_id_to_doi,
                                                                               reference_id_to_pmcid,
                                                                               journal_to_resource_id,
                                                                               resource_id_to_issn,
                                                                               resource_id_to_nlm,
                                                                               old_md5sum,
                                                                               new_md5sum,
                                                                               count,
                                                                               authors_with_first_or_corresponding_flag,
                                                                               json_path,
                                                                               pmids_with_json_updated,
                                                                               pmids_with_pub_status_changed,
                                                                               pmids_with_no_pub_status_changed,
                                                                               bad_date_published,
                                                                               update_log,
                                                                               offset)

    return authors_with_first_or_corresponding_flag


def update_reference_table(db_session, fw, pmid, x, json_data, new_resource_id, journal_title, authors, bad_date_published, pmids_with_pub_status_changed, pmids_with_no_pub_status_changed, update_log, count):  # noqa: C901 pragma: no cover

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

    pub_status_changed = set_pub_status_changed(x.pubmed_publication_status,
                                                colName_to_json_key['pubmed_publication_status'],
                                                json_data)
    has_update = 0
    for colName in refColName_to_update:
        if colName == 'resource_id':
            handle_resource_id_update(fw, pmid, x, new_resource_id, update_log, has_update)
            set_data_changed(pmid, colName, pub_status_changed,
                             pmids_with_pub_status_changed,
                             pmids_with_no_pub_status_changed,
                             x.resource.title, journal_title)
        elif colName in ['date_last_modified_in_pubmed', 'date_arrived_in_pubmed']:
            old_value = str(getattr(x, colName))[0:10]
            j_key = colName_to_json_key[colName]
            new_value = None
            if json_data.get(j_key) and json_data[j_key].get('date_string'):
                new_value = json_data[j_key]['date_string']
            handle_generic_update(fw, pmid, x, colName, old_value, new_value,
                                  update_log, has_update)
        elif colName in ['pubmed_abstract_languages', 'pubmed_types']:
            j_key = colName_to_json_key[colName]
            old_value = getattr(x, colName, [])
            new_value = json_data.get(j_key) if json_data.get(j_key) else []
            handle_generic_update(fw, pmid, x, colName, old_value, new_value,
                                  update_log, has_update)
        elif colName == 'keywords':
            if json_data.get('keywords'):
                # WARNING: never delete keywords - only add new one(s)
                old_keywords = x.keywords
                if old_keywords is None:
                    old_keywords = []
                new_keywords = list(set(old_keywords + json_data['keywords']))
                handle_generic_update(fw, pmid, x, colName, old_keywords,
                                      new_keywords, update_log, has_update)
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
            # if new_value is None:
            #    continue
            ## 2024-02-07: we want to update all data based on the data from PubMed
            if colName == 'category':
                if old_value:
                    old_value = old_value.replace("ReferenceCategory.", "")
                handle_generic_update(fw, pmid, x, colName, old_value,
                                      new_value, update_log, has_update)
                continue
            if colName == 'pubmed_publication_status' and old_value:
                old_value = old_value.replace("PubMedPublicationStatus.", "")
            handle_generic_update(fw, pmid, x, colName, old_value,
                                  new_value, update_log, has_update)
            if str(new_value) != str(old_value) and colName in ['volume', 'issue_name']:
                set_data_changed(pmid, colName, pub_status_changed,
                                 pmids_with_pub_status_changed,
                                 pmids_with_no_pub_status_changed,
                                 old_value, new_value)
    if has_update:
        # x.date_updated = date.today()
        db_session.add(x)
        update_log['pmids_updated'].append(pmid)
        log.info(f"{count} PMID:{pmid} Reference table has been updated")
        fw.write(f"PMID:{pmid} Reference table has been updated\n")
    else:
        fw.write(f"PMID:{pmid} No Change in Reference table\n")
        log.info(f"{count} PMID:{pmid} No Change in Reference table")
    return pub_status_changed


def log_update(fw, pmid, colName, old_value, new_value, update_log, has_update):
    fw.write(f"PMID:{pmid}: {colName} is updated from '{old_value}' to '{new_value}'\n")
    if colName in update_log:
        update_log[colName] += 1
    else:
        update_log[colName] = 1
    has_update += 1


def handle_resource_id_update(fw, pmid, x, new_resource_id, update_log, has_update):
    if new_resource_id and new_resource_id != x.resource_id:
        log_update(fw, pmid, 'resource_id', x.resource_id, new_resource_id,
                   update_log, has_update)
        # setattr(x, 'resource_id', new_resource_id)
        x.resource_id = new_resource_id


def handle_generic_update(fw, pmid, x, colName, old_value, new_value, update_log, has_update):
    if colName in ['keywords', 'pubmed_abstract_languages', 'pubmed_types']:
        if sorted(old_value) != sorted(new_value):
            log_update(fw, pmid, colName, old_value, new_value, update_log, has_update)
            setattr(x, colName, new_value)
    else:
        if str(new_value) != str(old_value):
            log_update(fw, pmid, colName, old_value, new_value, update_log, has_update)
            setattr(x, colName, new_value)


def set_pub_status_changed(pub_status_db, json_key, json_data):

    if pub_status_db:
        pub_status_db = pub_status_db.replace("PubMedPublicationStatus.", "")
    pub_status_new = json_data.get(json_key)
    if pub_status_db == 'aheadofprint' and pub_status_new and pub_status_new in ['ppublish', 'epublish']:
        return True
    return False


def set_data_changed(pmid, colName, pub_status_changed, pmids_with_pub_status_changed,
                     pmids_with_no_pub_status_changed, old_value, new_value):

    message = f"From '{old_value}' to '{new_value}'"
    if pub_status_changed:
        data_changed = pmids_with_pub_status_changed.get(pmid, {})
        data_changed[colName] = message
        pmids_with_pub_status_changed[pmid] = data_changed
    else:
        data_changed = pmids_with_no_pub_status_changed.get(pmid, {})
        data_changed[colName] = message
        pmids_with_no_pub_status_changed[pmid] = data_changed


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

    reference_id_list = []
    pmid_to_md5sum = {}
    for pmid in pmids_all:
        pmid_with_prefix = "PMID:" + pmid
        if pmid_with_prefix not in new_md5sum:
            continue
        if old_md5sum.get(pmid_with_prefix) and new_md5sum[pmid_with_prefix] == old_md5sum[pmid_with_prefix]:
            continue
        pmid_to_md5sum[pmid_with_prefix] = new_md5sum[pmid_with_prefix]
        if pmid in pmid_to_reference_id:
            reference_id_list.append(pmid_to_reference_id[pmid])
    return (reference_id_list, pmid_to_md5sum)


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
