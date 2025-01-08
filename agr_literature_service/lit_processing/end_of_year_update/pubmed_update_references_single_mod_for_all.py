import argparse
import logging
from os import environ, makedirs, path
from datetime import datetime, date
import time
import shutil

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_resources_nlm \
    import update_resource_pubmed_nlm
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import \
    generate_json
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    load_database_md5data, save_database_md5data
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_single_mod \
    import set_paths, update_database, get_md5sum, generate_pmids_with_info
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_pmid_to_reference_id_for_papers_not_associated_with_mod, \
    get_pmid_to_reference_id
from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3
from agr_literature_service.lit_processing.utils.report_utils import \
    write_log_and_send_pubmed_update_report, \
    write_log_and_send_pubmed_no_update_report
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

refColName_to_update = ['title', 'volume', 'issue_name', 'page_range',
                        'abstract', 'pubmed_types', 'pubmed_publication_status',
                        'keywords', 'category', 'plain_language_abstract',
                        'pubmed_abstract_languages', 'language', 'date_published',
                        'date_published_start', 'date_published_end',
                        'date_arrived_in_pubmed', 'date_last_modified_in_pubmed',
                        'publisher', 'resource_id']

field_names_to_report = refColName_to_update + ['doi', 'pmcid', 'author_name',
                                                'comment_erratum', 'mesh_term',
                                                'pmids_updated']

limit = 500
max_rows_per_commit = 250
download_xml_max_size = 5000
query_cutoff = 8000
batch_size_small = 500
sleep_time = 10

large_batch_size = 5000

init_tmp_dir()


def update_data(mod):  # noqa: C901 pragma: no cover

    update_resource_pubmed_nlm()

    db_session = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    (xml_path, json_path, old_xml_path, old_json_path, log_path, log_url,
     email_recipients, sender_email, sender_password, reply_to) = set_paths()

    email_subject = "PubMed Paper Update Report"
    if mod and mod != 'NONE':
        email_subject = mod + " " + email_subject

    # Set new log file with date stamp
    datestamp = str(date.today()).replace("-", "")
    log_file = f"{log_path}update_pubmed_papers_{mod}_{datestamp}.log"

    with open(log_file, "w") as fw:
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

        fw.write(str(datetime.now()) + "\n")
        fw.write("Starting batch processing...\n")
        logger.info("Starting batch processing...")

        not_found_xml_set = set()

        # Split pmids_all into large batches of large_batch_size (e.g., 10,000)
        total_batches = (len(pmids_all) + large_batch_size - 1) // large_batch_size
        for batch_num, i in enumerate(range(0, len(pmids_all), large_batch_size), start=1):
            batch_pmids = pmids_all[i:i + large_batch_size]
            logger.info(f"Processing batch {batch_num}/{total_batches}: {len(batch_pmids)} PMIDs")
            fw.write(str(datetime.now()) + f"\nProcessing batch {batch_num}/{total_batches}: {len(batch_pmids)} PMIDs\n")

            clean_up_files(xml_path, json_path)

            # Download XML files in smaller slices within the batch
            fw.write(str(datetime.now()) + f"\nDownloading PubMed XML files for batch {batch_num}...\n")
            logger.info(f"Downloading PubMed XML files for batch {batch_num}...")
            if len(batch_pmids) > download_xml_max_size:
                for sub_index in range(0, len(batch_pmids), download_xml_max_size):
                    pmids_slice = batch_pmids[sub_index:sub_index + download_xml_max_size]
                    download_pubmed_xml(pmids_slice)
                    fw.write(str(datetime.now()) + f"\nDownloaded XML for PMIDs {pmids_slice[0]} to {pmids_slice[-1]}\n")
                    logger.info(f"Downloaded XML for PMIDs {pmids_slice[0]} to {pmids_slice[-1]}")
                    time.sleep(sleep_time)
            else:
                download_pubmed_xml(batch_pmids)
                fw.write(str(datetime.now()) + f"\nDownloaded XML for batch {batch_num}\n")
                logger.info(f"Downloaded XML for batch {batch_num}")

            # Generate JSON files
            fw.write(str(datetime.now()) + "\nGenerating JSON files...\n")
            logger.info("Generating JSON files...")
            generate_json(batch_pmids, [], not_found_xml_set)

            # Load MD5 sums
            fw.write(str(datetime.now()) + "\nLoading MD5 sums...\n")
            md5dict = load_database_md5data(['PMID'])
            old_md5sum = md5dict['PMID']

            new_md5sum = get_md5sum(json_path)

            # Generate pmids with info
            reference_id_list, pmid_to_md5sum = generate_pmids_with_info(
                batch_pmids,
                old_md5sum,
                new_md5sum,
                pmid_to_reference_id
            )

            if len(reference_id_list) == 0:
                fw.write(str(datetime.now()) + f"\nNo updates required for batch {batch_num}.\n")
                logger.info(f"No updates required for batch {batch_num}.")
                write_log_and_send_pubmed_no_update_report(fw, mod, email_subject)
                # Clean up files even if no updates
                clean_up_files(fw, batch_pmids, xml_path, json_path)
                continue

            # Update the database
            fw.write(str(datetime.now()) + f"\nUpdating database for batch {batch_num}...\n")
            logger.info(f"Updating database for batch {batch_num}...")

            pmids_with_json_updated = []
            pmids_with_pub_status_changed = {}
            bad_date_published = {}
            try:
                authors_with_first_or_corresponding_flag = update_database(
                    fw, mod,
                    reference_id_list,
                    reference_id_to_pmid,
                    pmid_to_reference_id,
                    update_log, new_md5sum,
                    old_md5sum, json_path,
                    pmids_with_json_updated,
                    pmids_with_pub_status_changed,
                    bad_date_published
                )
            except Exception as e:
                logger.error(f"Error updating data for batch {batch_num}: {e}")
                fw.write(f"Error updating data for batch {batch_num}: {e}\n")
                continue  # Proceed to the next batch

            """
            # Send update report
            not_found_xml_list = list(not_found_xml_set)
            try:
                write_log_and_send_pubmed_update_report(
                    fw, mod, field_names_to_report, update_log,
                    bad_date_published,
                    authors_with_first_or_corresponding_flag,
                    not_found_xml_list, log_url, log_path,
                    email_subject, pmids_with_pub_status_changed
                )
            except Exception as e:
                logger.error(f"Error sending update report for batch {batch_num}: {e}")
                fw.write(f"Error sending update report for batch {batch_num}: {e}\n")
            """

            # Save MD5 sums
            md5dict = {'PMID': pmid_to_md5sum}
            save_database_md5data(md5dict)

            # Upload XML files to S3 if in production
            if environ.get('ENV_STATE') and environ['ENV_STATE'] == 'prod':
                fw.write(str(datetime.now()) + f"\nUploading XML files to S3 for batch {batch_num}...\n")
                logger.info(f"Uploading XML files to S3 for batch {batch_num}...")
                for pmid in pmids_with_json_updated:
                    try:
                        logger.info(f"Uploading XML file for PMID:{pmid} to S3")
                        upload_xml_file_to_s3(pmid, 'latest')
                    except Exception as e:
                        logger.error(f"Error uploading PMID:{pmid} to S3: {e}")
                        fw.write(f"Error uploading PMID:{pmid} to S3: {e}\n")

            fw.write(str(datetime.now()) + f"\nCompleted batch {batch_num}.\n")
            logger.info(f"Completed batch {batch_num}.\n")

        # Send update report
        not_found_xml_list = list(not_found_xml_set)
        try:
            write_log_and_send_pubmed_update_report(
                fw, mod, field_names_to_report, update_log,
                bad_date_published,
                authors_with_first_or_corresponding_flag,
                not_found_xml_list, log_url, log_path,
                email_subject, pmids_with_pub_status_changed
            )
        except Exception as e:
            logger.error(f"Error sending update report for batch {batch_num}: {e}")
            fw.write(f"Error sending update report for batch {batch_num}: {e}\n")

        fw.write(str(datetime.now()) + "\nDONE!\n")
        logger.info("DONE!\n")


def clean_up_files(xml_path, json_path):
    try:
        if path.exists(xml_path):
            shutil.rmtree(xml_path)
        if path.exists(json_path):
            shutil.rmtree(json_path)
    except OSError as e:
        logger.info("Error deleting old xml/json: %s" % (e.strerror))

    makedirs(xml_path)
    makedirs(json_path)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()

    group.add_argument('-m', '--mod', action='store', type=str, help='MOD to update',
                       choices=['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XB', 'NONE'])

    args = vars(parser.parse_args())
    if not any(args.values()):
        parser.error('No arguments provided.')
    update_data(args['mod'])
