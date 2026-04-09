import argparse
import logging
import gzip
from typing import Set
from os import environ, path
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
from agr_literature_service.lit_processing.utils.db_read_utils import retrieve_all_pmids, get_mod_papers
from agr_literature_service.api.models import ModCorpusAssociationModel, ModModel
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.report_utils import send_report
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_search_new_references \
    import add_md5sum_to_database
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import \
    sanitize_pubmed_json_list
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils import (
    associate_papers_with_alliance,
    search_pubmed_for_validity,
    clean_up_tmp_directories,
)

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()
init_tmp_dir()

download_url = "https://fms.alliancegenome.org/download/"

has_interactions = {
    "GEN": [
        "HUMAN", "SGD", "WB", "FB", "ZFIN", "MGI", "RGD", "XBXL",
    ],
    "MOL": [
        "HUMAN", "SGD", "WB", "FB", "ZFIN", "MGI", "RGD", "XBXL", "XBXT", "SARS-CoV-2"
    ]
}

base_path = environ.get("XML_PATH", "")
file_path = base_path + "interaction_data/"
json_path = base_path + "pubmed_json/"
xml_path = base_path + "pubmed_xml/"
log_path = environ.get("LOG_PATH", "")
log_url = environ.get("LOG_URL", "")


def load_data(datasetName, dataType, full_obsolete_set, message):

    if not has_interactions.get(dataType) or datasetName not in has_interactions[dataType]:
        logger.error(f"We don't have {dataType} interaction data for {datasetName}")
        return

    db_session = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    clean_up_tmp_directories([file_path, xml_path, json_path])

    file_name, all_pmids, pmid_to_src = extract_pmids(db_session, datasetName, dataType)
    all_pmids_db = retrieve_all_pmids(db_session)
    new_pmids = all_pmids - set(all_pmids_db)

    # Associate HUMAN papers with AGR MOD
    if datasetName == "HUMAN":
        associate_papers_with_alliance(db_session, all_pmids, 'AGR')

    if len(new_pmids) == 0:
        message = check_pmids_and_compose_message(db_session, datasetName, file_name,
                                                  all_pmids, new_pmids, pmid_to_src,
                                                  full_obsolete_set, message)
        return message

    download_pubmed_xml(list(new_pmids))
    generate_json(list(new_pmids), [])

    inject_object = {}
    sanitize_pubmed_json_list(new_pmids, [inject_object])

    json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
    post_references(json_filepath)

    pmids_loaded = set()
    for pmid in new_pmids:
        if path.exists(xml_path + pmid + ".xml"):
            pmids_loaded.add(pmid)
            if environ.get('ENV_STATE') and environ['ENV_STATE'] == 'prod':
                logger.info(f"uploading xml file to s3 for PMID:{pmid}")
                upload_xml_file_to_s3(pmid, 'latest')

    add_md5sum_to_database(db_session, None, pmids_loaded)

    # Associate newly loaded HUMAN papers with AGR MOD
    if datasetName == "HUMAN" and len(pmids_loaded) > 0:
        associate_papers_with_alliance(db_session, pmids_loaded, 'AGR')

    message = check_pmids_and_compose_message(db_session, datasetName, file_name,
                                              all_pmids, pmids_loaded, pmid_to_src,
                                              full_obsolete_set, message)
    return message


def extract_pmids(db_session, datasetName, dataType):

    file_name = f"INTERACTION-{dataType}_{datasetName}.tsv.gz"
    url_to_download = f"{download_url}{file_name}"
    file_with_path = f"{file_path}{file_name}"
    download_file(url_to_download, file_with_path)
    all_pmids = set()
    # all_other_ids = []
    pmid_to_src = {}
    with gzip.open(file_with_path, "rt") as f:
        for line in f:
            if line.startswith("#"):
                continue
            items = line.split("\t")
            pub_ids = items[8].split("|")
            pmid = None
            for id in pub_ids:
                if id.isdigit():
                    pmid = id
                elif id.startswith("pubmed:"):
                    pmid = id.replace("pubmed:", "")
            if pmid:
                all_pmids.add(pmid)
                if len(items) > 12:
                    pmid_to_src[pmid] = items[12].split("(")[1].replace(")", "")
            # else:
            #    all_other_ids.append(items[8])

    return file_name, all_pmids, pmid_to_src


def compose_report_title(file_name):

    # file_name = INTERACTION-GEN_SGD.tsv.gz
    mod = file_name.replace(".tsv.gz", "").split("_")[1]
    fileType = ""
    if mod.startswith("XB"):
        fileType = mod
        mod = "XB"
    title = file_name.split("_")[0]
    return f"{mod}: {title} {fileType}"


def check_pmids_and_compose_message(db_session, datasetName, file_name, all_pmids, pmids_loaded, pmid_to_src, full_obsolete_set, message):  # noqa: C901 pragma: no cover

    logger.info(f"{file_name}:\n")
    logger.info(f"New Reference(s) Added: {len(pmids_loaded)}")

    report_title = compose_report_title(file_name)

    message += f"<b>{report_title}</b><p>"
    message += "<ul>"
    message += f"<li>New Reference(s) Added: {len(pmids_loaded)}"

    all_pmids_db = retrieve_all_pmids(db_session)
    pmids_out_db_set = all_pmids - set(all_pmids_db)

    pmids_in_corpus_set = set()
    pmids_associated_with_mod_but_out_corpus_set = set()
    pmids_in_db_but_not_associated_with_mod_set = set()
    logfile_name = file_name.replace(".tsv.gz", ".log")
    fw = open(log_path + logfile_name, "w")
    has_logfile = False
    if datasetName in ["SGD", "WB", "FB", "ZFIN", "MGI", "RGD", "XBXL", "XBXT"]:
        mod = datasetName
        if mod.startswith("XB"):
            mod = "XB"
        in_corpus_set, out_corpus_set = get_mod_papers(db_session, mod)
        for pmid in set(all_pmids):
            if pmid in in_corpus_set:
                pmids_in_corpus_set.add(pmid)
            elif pmid in out_corpus_set:
                pmids_associated_with_mod_but_out_corpus_set.add(pmid)
            elif pmid in all_pmids_db:
                pmids_in_db_but_not_associated_with_mod_set.add(pmid)
        logger.info(f"In {mod} Corpus: {len(pmids_in_corpus_set)}")
        message += f"<li>In {mod} Corpus: {len(pmids_in_corpus_set)}"
        # For SGD, update papers that are associated but outside corpus to be in corpus
        pmids_still_outside_corpus = pmids_associated_with_mod_but_out_corpus_set
        if datasetName == "SGD" and len(pmids_associated_with_mod_but_out_corpus_set) > 0:
            updated_count, updated_pmids = update_sgd_corpus_flag_to_true(
                db_session, pmids_associated_with_mod_but_out_corpus_set
            )
            if updated_count > 0:
                logger.info(f"Updated {updated_count} paper(s) to SGD corpus (corpus=True)")
                message += f"<li>Updated to SGD Corpus: {updated_count}"
            # Compute remaining PMIDs that were not updated
            pmids_still_outside_corpus = pmids_associated_with_mod_but_out_corpus_set - updated_pmids

        logger.info(f"Associated but Outside Corpus: {len(pmids_still_outside_corpus)}")
        message += f"<li>Associated but Outside Corpus: {len(pmids_still_outside_corpus)}"
        if len(pmids_still_outside_corpus) > 0:
            fw.write("Associated but Outside Corpus:\n\n")
            for pmid in pmids_still_outside_corpus:
                fw.write(f"PMID:{pmid}\n")
            fw.write("\n")
            has_logfile = True
        if len(pmids_in_db_but_not_associated_with_mod_set) > 0:
            pmids_added_to_corpus = set()
            # For SGD, add these papers to the SGD corpus
            if datasetName == "SGD":
                added_count, pmids_added_to_corpus = associate_sgd_interaction_papers_with_corpus(
                    db_session, pmids_in_db_but_not_associated_with_mod_set
                )
                if added_count > 0:
                    logger.info(f"Added {added_count} paper(s) to SGD corpus")
                    message += f"<li>Added to SGD Corpus: {added_count}"
            # Only log PMIDs that were not just added to the corpus
            pmids_still_not_associated = pmids_in_db_but_not_associated_with_mod_set - pmids_added_to_corpus
            # Log the count after SGD papers have been added
            logger.info(f"Not Associated with {mod}: {len(pmids_still_not_associated)}")
            message += f"<li>Not Associated with {mod}: {len(pmids_still_not_associated)}"
            if len(pmids_still_not_associated) > 0:
                fw.write(f"Not Associated with {mod}:\n\n")
                for pmid in pmids_still_not_associated:
                    fw.write(f"PMID:{pmid} ({pmid_to_src.get(pmid)})\n")
                fw.write("\n")
                has_logfile = True
        else:
            logger.info(f"Not Associated with {mod}: 0")
            message += f"<li>Not Associated with {mod}: 0"
    else:
        pmids_in_db_but_not_associated_with_mod_set = set(all_pmids) - pmids_out_db_set
        logger.info(f"In Database: {len(pmids_in_db_but_not_associated_with_mod_set)}")
        message += f"<li>In Database: {len(pmids_in_db_but_not_associated_with_mod_set)}"

    api_key = environ.get('NCBI_API_KEY', '')
    obsolete_pmids, valid_pmids = search_pubmed_for_validity(pmids_out_db_set, api_key)
    if len(obsolete_pmids) > 0:
        logger.info(f"Obsolete PMIDs: {obsolete_pmids}")
        message += f"<li>Obsolete PMIDs: {obsolete_pmids}"
        fw.write("\nObsolete PMIDs:\n\n")
        for o_pmid in obsolete_pmids:
            pmid_with_src = f"PMID:{o_pmid}"
            if pmid_to_src.get(o_pmid):
                pmid_with_src = f"{pmid_with_src} ({pmid_to_src.get(o_pmid)})"
            fw.write(f"{pmid_with_src}\n")
            full_obsolete_set.add(pmid_with_src)
        fw.close()
        has_logfile = True
    if len(valid_pmids) > 0:
        logger.info(f"Valid new PMIDs, but not loaded: {valid_pmids}")
        message += f"<li>Valid new PMIDs, but not loaded: {valid_pmids}<br>"
    logger.info("\n")

    if has_logfile:
        log_file = log_url + logfile_name
        message += f"<li><a href='{log_file}'>log file</a>"
    message += "</ul>"
    return message


def update_sgd_corpus_flag_to_true(db_session, pmids):
    """
    Update SGD interaction papers that are associated but outside corpus
    to be inside the corpus (corpus=True).

    Args:
        db_session: Database session
        pmids: Set of PMIDs to update

    Returns:
        Tuple of (count of papers updated, set of PMIDs that were updated)
    """
    if not pmids:
        return 0, set()

    sgd_mod = db_session.query(ModModel).filter(
        ModModel.abbreviation == 'SGD'
    ).first()
    if not sgd_mod:
        logger.warning("SGD MOD not found in database")
        return 0, set()

    sgd_mod_id = sgd_mod.mod_id

    # Build parameterized query for PMIDs
    pmid_curies = [f"PMID:{pmid}" for pmid in pmids]

    # Get reference_ids for PMIDs using parameterized query
    query = text(
        "SELECT cr.curie, cr.reference_id "
        "FROM cross_reference cr "
        "WHERE cr.curie = ANY(:pmid_curies) "
        "AND cr.is_obsolete = False"
    )
    rows = db_session.execute(query, {"pmid_curies": pmid_curies}).fetchall()

    pmid_to_ref_id = {row[0].replace('PMID:', ''): row[1] for row in rows}
    reference_ids_in_db = set(pmid_to_ref_id.values())

    if not reference_ids_in_db:
        return 0, set()

    ref_ids_list = list(reference_ids_in_db)

    # Build reverse mapping: ref_id -> pmid
    ref_id_to_pmid = {v: k for k, v in pmid_to_ref_id.items()}

    # Update mod_corpus_association records where corpus=False to corpus=True
    # Use RETURNING to get the reference_ids that were actually updated
    update_query = text(
        "UPDATE mod_corpus_association "
        "SET corpus = True "
        "WHERE reference_id = ANY(:ref_ids) "
        "AND mod_id = :sgd_mod_id "
        "AND corpus = False "
        "RETURNING reference_id"
    )
    result = db_session.execute(
        update_query,
        {
            "ref_ids": ref_ids_list,
            "sgd_mod_id": sgd_mod_id
        }
    )
    updated_ref_ids = {row[0] for row in result.fetchall()}
    count = len(updated_ref_ids)

    # Map back to PMIDs
    updated_pmids = {ref_id_to_pmid[ref_id] for ref_id in updated_ref_ids if ref_id in ref_id_to_pmid}

    if count > 0:
        db_session.commit()

    return count, updated_pmids


def associate_sgd_interaction_papers_with_corpus(db_session, pmids):
    """
    Associate SGD interaction papers with the SGD corpus.
    Only associates papers that do NOT already have a mod_corpus_association
    with SGD.

    Args:
        db_session: Database session
        pmids: Set of PMIDs to associate

    Returns:
        Tuple of (count of papers associated, set of PMIDs that were added)
    """
    if not pmids:
        return 0, set()

    sgd_mod = db_session.query(ModModel).filter(
        ModModel.abbreviation == 'SGD'
    ).first()
    if not sgd_mod:
        logger.warning("SGD MOD not found in database")
        return 0, set()

    sgd_mod_id = sgd_mod.mod_id

    # Build parameterized query for PMIDs
    pmid_curies = [f"PMID:{pmid}" for pmid in pmids]

    # Get reference_ids for PMIDs using parameterized query
    query = text(
        "SELECT cr.curie, cr.reference_id "
        "FROM cross_reference cr "
        "WHERE cr.curie = ANY(:pmid_curies) "
        "AND cr.is_obsolete = False"
    )
    rows = db_session.execute(query, {"pmid_curies": pmid_curies}).fetchall()

    pmid_to_ref_id = {row[0].replace('PMID:', ''): row[1] for row in rows}
    reference_ids_in_db = set(pmid_to_ref_id.values())

    if not reference_ids_in_db:
        return 0, set()

    ref_ids_list = list(reference_ids_in_db)

    # Get reference_ids that already have an association with SGD
    refs_already_associated_query = text(
        "SELECT DISTINCT reference_id FROM mod_corpus_association "
        "WHERE reference_id = ANY(:ref_ids) "
        "AND mod_id = :sgd_mod_id"
    )
    refs_already_associated = db_session.execute(
        refs_already_associated_query,
        {"ref_ids": ref_ids_list, "sgd_mod_id": sgd_mod_id}
    ).fetchall()

    already_associated = {row[0] for row in refs_already_associated}

    # Build reverse mapping: ref_id -> pmid
    ref_id_to_pmid = {v: k for k, v in pmid_to_ref_id.items()}

    # Add mod_corpus_association for papers not yet associated with SGD
    count = 0
    pmids_added = set()
    for ref_id in reference_ids_in_db:
        if ref_id not in already_associated:
            mca = ModCorpusAssociationModel(
                reference_id=ref_id,
                mod_id=sgd_mod_id,
                corpus=True,
                mod_corpus_sort_source=ModCorpusSortSourceType.Automated_alliance
            )
            db_session.add(mca)
            count += 1
            if ref_id in ref_id_to_pmid:
                pmids_added.add(ref_id_to_pmid[ref_id])

    if count > 0:
        db_session.commit()
        logger.info(f"Associated {count} SGD interaction paper(s) with SGD corpus")

    return count, pmids_added


def load_all(full_obsolete_set, message):

    combined_dataset_list = has_interactions["GEN"] + has_interactions["MOL"]
    unique_dataset_set = list(set(combined_dataset_list))
    unique_dataset_set.sort()
    for datasetName in unique_dataset_set:
        if datasetName in has_interactions["GEN"]:
            message = load_data(datasetName, "GEN", full_obsolete_set, message)
        if datasetName in has_interactions["MOL"]:
            message = load_data(datasetName, "MOL", full_obsolete_set, message)

    return message


def send_slack_report(message, full_obsolete_set):

    email_subject = "Interaction Reference Loading Report"
    if len(full_obsolete_set) > 0:
        message += "<p><b>Obsolete PMID(s)</b><p>"
        message += "<ul>"
        obsolete_pmid_list = list(full_obsolete_set)
        obsolete_pmid_list.sort()
        for pmid_with_src in obsolete_pmid_list:
            message += f"<li>{pmid_with_src}"
        message += "</ul>"
    send_report(email_subject, message)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--all', action='store', type=str, help="update all")
    parser.add_argument('-d', '--datasetName', action='store', type=str,
                        help='datasetName to update',
                        choices=['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XBXL',
                                 'XBXT', 'HUMAN', 'SARS-CoV-2'])
    parser.add_argument('-t', '--type', action='store', type=str,
                        help='data type to update: MOL or GEN',
                        choices=['MOL', 'GEN'])
    args = parser.parse_args()

    message = ''
    full_obsolete_set: Set[str] = set()
    if args.all:
        message = load_all(full_obsolete_set, message)
    elif args.datasetName and args.type:
        message = load_data(args.datasetName, args.type, full_obsolete_set, message)
    elif args.datasetName:
        types = ['GEN', 'MOL']
        for type in types:
            if args.datasetName in has_interactions[type]:
                message = load_data(args.datasetName, type, full_obsolete_set, message)
    elif args.type:
        for datasetName in has_interactions[args.type]:
            message = load_data(datasetName, args.type, full_obsolete_set, message)
    else:
        message = load_all(full_obsolete_set, message)

    send_slack_report(message, full_obsolete_set)
