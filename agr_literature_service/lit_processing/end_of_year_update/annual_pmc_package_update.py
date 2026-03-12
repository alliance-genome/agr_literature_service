"""
Annual/Bi-annual PMC Package Update Script

This script performs a full update check of all PMC packages currently in the database.
Run this 1-2 times per year to update packages that have been modified at PMC.

Usage:
    python annual_pmc_package_update.py [--batch-size 500] [--dry-run]
"""

import logging
import shutil
import csv
import argparse
from datetime import datetime
from os import path, listdir, makedirs
from typing import Dict, Any, Iterable, Tuple, List
from sqlalchemy import text
from collections import defaultdict
from fastapi import HTTPException

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferencefileModel, ReferencefileModAssociationModel, \
    CrossReferenceModel
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file, gzip_file, classify_pmc_file, \
    get_pmc_oa_s3_client, PMC_OA_S3_BUCKET, get_latest_pmc_version, \
    download_pmc_package_from_s3
from agr_literature_service.lit_processing.utils.db_read_utils import get_pmid_to_reference_id_mapping
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_download_pmc_files import \
    download_packages, unpack_packages, upload_suppl_file_to_s3
from agr_literature_service.api.crud.referencefile_utils import remove_from_s3_and_db
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    get_md5sum
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    insert_referencefile_mod_for_pmc, insert_referencefile
from agr_literature_service.api.crud.referencefile_crud import cleanup_wft_tet_tags_for_deleted_main_pdf

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
DATA_DIR = "data/"
OA_CSV_TIMEFMT = "%Y-%m-%d %H:%M:%S"

# Output files
UPDATE_TSV = "data/pmc_files_to_update.tsv"
REMOVE_TSV = "data/pmc_files_to_remove.tsv"
ADD_TSV = "data/pmc_files_to_add.tsv"
PROGRESS_LOG = "data/update_progress.log"
SUMMARY_LOG = "data/update_summary.log"

file_publication_status = "final"
pmcFileDir = 'pubmed_pmc_download/'


def chunked(iterable: Iterable, size: int):
    lst = list(iterable)
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def log_progress(message: str):
    """Log progress to both console/stdout and progress log file."""
    logger.info(message)
    with open(PROGRESS_LOG, "a") as f:
        f.write(f"{datetime.now().isoformat()} - {message}\n")


def annual_pmc_package_update(mapping_file: str = None, batch_size: int = 500, dry_run: bool = False):
    """
    Main function to perform annual PMC package update.
    Args:
        mapping_file: Deprecated - no longer used. S3 is now the source.
        batch_size: Number of PMIDs to process per batch
        dry_run: If True, only report what would be updated without making changes
    """
    start_time = datetime.now()
    log_progress("=" * 80)
    log_progress(f"Starting Annual PMC Package Update - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log_progress(f"Batch size: {batch_size}, Dry run: {dry_run}")
    log_progress("Using AWS S3 bucket pmc-oa-opendata for package retrieval")
    log_progress("=" * 80)

    # Step 1: Get all PMIDs currently in our database
    log_progress("Step 1: Retrieving PMIDs from database...")
    pmids_in_db, pmid_to_reference_id, pmid_to_last_date_updated, pmids_for_retracted_papers = get_all_pmids_with_files_etc()
    log_progress(f"Found {len(pmids_in_db)} PMIDs with files in database")

    # Step 2: Get PMID to PMCID mapping from database
    log_progress("Step 2: Getting PMID to PMCID mappings from database...")
    pmid_to_pmcid = get_pmid_to_pmcid_from_db()

    # Step 3: Check S3 for packages that need updating
    log_progress("Step 3: Checking S3 for packages with updates...")
    pmid_to_pmcid_to_update = get_pmid_to_pmcid_s3_mapping(pmid_to_pmcid, pmid_to_last_date_updated)

    # Step 4: Intersect - only check PMIDs we have AND that need updates
    log_progress("Step 4: Identifying PMIDs that need checking...")
    pmids_to_check = sorted(
        set(pmid_to_pmcid_to_update.keys()) & set(pmids_in_db),
        key=lambda x: int(x.split(":", 1)[1])
    )

    if not pmids_to_check:
        log_progress("No PMIDs require checking; nothing to do.")
        return

    log_progress(f"Step 4: Will check {len(pmids_to_check)} PMIDs for updates")

    # Ensure directories exist
    if not path.exists(pmcFileDir):
        makedirs(pmcFileDir, exist_ok=True)

    # Initialize tracking files
    init_tsv_files()

    # Statistics tracking

    stats: Dict[str, Any] = {
        'total_pmids': len(pmids_to_check),
        'batches_processed': 0,
        'pmids_with_updates': set(),
        'pmids_with_removals': set(),
        'pmids_with_additions': set(),
        'pmids_unchanged': set(),
        'errors': [],
        'start_time': start_time
    }

    # Process in batches: using floor division // (round down)
    total_batches = (len(pmids_to_check) + batch_size - 1) // batch_size

    for batch_idx, batch_pmids in enumerate(chunked(pmids_to_check, batch_size), start=1):
        try:
            process_batch(
                batch_idx,
                batch_pmids,
                pmid_to_pmcid_to_update,
                stats,
                total_batches,
                pmid_to_reference_id,
                pmids_for_retracted_papers,
                dry_run
            )
        except Exception as e:
            error_msg = f"Batch {batch_idx} failed with error: {e}"
            log_progress(f"ERROR: {error_msg}")
            stats['errors'].append(error_msg)
            break

    # Generate final summary
    generate_summary_report(stats, dry_run)

    log_progress("=" * 80)
    log_progress(f"Annual PMC Package Update Completed - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_progress("=" * 80)


def process_batch(batch_idx, batch_pmids, pmid_to_pmcid, stats, total_batches, pmid_to_reference_id, pmids_for_retracted_papers, dry_run):      # noqa: C901
    """Process a single batch of PMIDs."""
    batch_size = len(batch_pmids)
    batch_start = (batch_idx - 1) * len(batch_pmids) + 1
    batch_end = batch_start + batch_size - 1

    log_progress("")
    log_progress(f"{'='*60}")
    log_progress(f"Processing Batch {batch_idx}/{total_batches}")
    log_progress(f"PMIDs: {batch_start}-{batch_end} of {stats['total_pmids']}")
    log_progress(f"{'='*60}")

    # Get numeric PMIDs for this batch
    batch_pmids_numeric = [p.split(":", 1)[-1] for p in batch_pmids]

    # Load metadata from DB for this batch only
    log_progress(f"[Batch {batch_idx}] Loading metadata from database...")
    try:
        pmid_to_metadata_in_db = get_metadata_for_pmids(batch_pmids_numeric)
        log_progress(f"[Batch {batch_idx}] Loaded metadata for {len(pmid_to_metadata_in_db)} PMIDs")
    except Exception as e:
        log_progress(f"[Batch {batch_idx}] ERROR loading metadata: {e}")
        raise

    # Check which packages need downloading (check for directory instead of tar.gz)
    pmids_to_download = []
    for pmid_with_prefix in batch_pmids:
        pmid_dir = path.join(pmcFileDir, pmid_with_prefix)
        if path.exists(pmid_dir) and listdir(pmid_dir):
            logger.debug(f"[Batch {batch_idx}] Cache hit: {pmid_dir}")
        else:
            pmids_to_download.append(pmid_with_prefix)

    # Download packages from S3
    if pmids_to_download:
        log_progress(f"[Batch {batch_idx}] Downloading {len(pmids_to_download)} PMC packages from S3...")
        if not dry_run:
            try:
                download_packages_from_s3_for_update(pmids_to_download, pmid_to_pmcid)
                log_progress(f"[Batch {batch_idx}] Download complete")
            except Exception as e:
                log_progress(f"[Batch {batch_idx}] ERROR during download: {e}")
                raise
        else:
            log_progress(f"[Batch {batch_idx}] DRY RUN: Would download {len(pmids_to_download)} packages")
    else:
        log_progress(f"[Batch {batch_idx}] All packages cached, skipping download")

    # No unpacking needed - S3 provides individual files

    # Compare files
    log_progress(f"[Batch {batch_idx}] Comparing files on disk vs database...")
    try:
        (
            batch_to_update,
            batch_to_remove,
            batch_to_add,
        ) = get_update_file_list(pmid_to_metadata_in_db, batch_pmids_numeric)

        # Log batch summary
        updates = len(batch_to_update)
        removals = len(batch_to_remove)
        additions = len(batch_to_add)
        unchanged = batch_size - updates - removals - additions

        log_progress(f"[Batch {batch_idx}] Results: {updates} updates, {additions} additions, "
                     f"{removals} removals, {unchanged} unchanged")
    except Exception as e:
        log_progress(f"[Batch {batch_idx}] ERROR during comparison: {e}")
        raise

    if not dry_run:
        try:
            batch_process_changed_files(
                batch_to_update,
                batch_to_remove,
                batch_to_add,
                pmid_to_reference_id,
                pmids_for_retracted_papers
            )
        except Exception as e:
            log_progress(f"[Batch {batch_idx}] ERROR during batch_process_changed_files: {e}")
            raise
    else:
        log_progress(f"[Batch {batch_idx}] DRY RUN: Would update/remove/add files in DB/S3")

    # Write results to TSV
    if not dry_run:
        try:
            append_updates_tsv(batch_to_update, UPDATE_TSV)
            append_removals_tsv(batch_to_remove, REMOVE_TSV)
            append_additions_tsv(batch_to_add, ADD_TSV)
        except Exception as e:
            log_progress(f"[Batch {batch_idx}] ERROR writing TSV files: {e}")
            raise
    else:
        log_progress(f"[Batch {batch_idx}] DRY RUN: Would write results to TSV files")

    # Update statistics
    stats['pmids_with_updates'].update(batch_to_update.keys())
    stats['pmids_with_removals'].update(batch_to_remove.keys())
    stats['pmids_with_additions'].update(batch_to_add.keys())

    # Track unchanged PMIDs
    changed_pmids = (
        set(batch_to_update.keys()) | set(batch_to_remove.keys()) | set(batch_to_add.keys())
    )
    unchanged_in_batch = set(batch_pmids_numeric) - changed_pmids
    stats['pmids_unchanged'].update(unchanged_in_batch)

    stats['batches_processed'] += 1

    # Cleanup
    log_progress(f"[Batch {batch_idx}] Cleaning up batch directory...")
    try:
        if path.exists(pmcFileDir):
            shutil.rmtree(pmcFileDir)
        makedirs(pmcFileDir, exist_ok=True)
    except Exception as e:
        log_progress(f"[Batch {batch_idx}] WARNING: Cleanup failed: {e}")

    # Progress update
    percent_complete = (batch_idx / total_batches) * 100
    elapsed = datetime.now() - stats['start_time']
    log_progress(f"[Batch {batch_idx}] Progress: {percent_complete:.1f}% complete, "
                 f"Elapsed time: {elapsed}")


def init_tsv_files():
    """Create (or truncate) the three TSV files and write headers."""
    try:
        with open(UPDATE_TSV, "w", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(["pmid", "file_name", "old_md5sum", "new_md5sum", "file_class", "pmcid", "path"])

        with open(REMOVE_TSV, "w", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(["pmid", "file_name", "md5sum", "file_class"])

        with open(ADD_TSV, "w", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(["pmid", "file_name", "md5sum", "pmcid", "path"])

        # Initialize progress log
        with open(PROGRESS_LOG, "w") as f:
            f.write(f"PMC Package Update Progress Log - {datetime.now().isoformat()}\n")
            f.write("=" * 80 + "\n")

        logger.info("Initialized output files")
    except Exception as e:
        logger.error(f"Failed to initialize output files: {e}")
        raise


def append_updates_tsv(to_update_file_list, outfile):
    """Append update rows to TSV file."""
    if not to_update_file_list:
        return
    with open(outfile, "a", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        for pmid, files in to_update_file_list.items():
            for meta in files:
                w.writerow([
                    pmid,
                    meta["file_name"],
                    meta["old_md5sum"],
                    meta["new_md5sum"],
                    meta.get("file_class", ""),
                    meta.get("pmcid", ""),
                    meta.get("path", ""),
                ])


def append_removals_tsv(to_remove_file_list, outfile):
    """Append removal rows to TSV file."""
    if not to_remove_file_list:
        return
    with open(outfile, "a", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        for pmid, files in to_remove_file_list.items():
            for meta in files:
                w.writerow([
                    pmid,
                    meta["file_name"],
                    meta["md5sum"],
                    meta.get("file_class", ""),
                ])


def append_additions_tsv(to_add_file_list, outfile):
    """Append addition rows to TSV file."""
    if not to_add_file_list:
        return
    with open(outfile, "a", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        for pmid, files in to_add_file_list.items():
            for meta in files:
                w.writerow([
                    pmid,
                    meta["file_name"],
                    meta["md5sum"],
                    meta.get("pmcid", ""),
                    meta.get("path", ""),
                ])


def get_update_file_list(
    pmid_to_metadata_in_db: Dict[str, Dict[str, Dict[str, Any]]],
    pmids_to_scan: Iterable[str] = None,
) -> Tuple[Dict[str, list], Dict[str, list], Dict[str, list]]:
    """
    Compare files on disk vs DB metadata.

    Returns:
        (to_update_dict, to_remove_dict, to_add_dict)

    Notes:
        - Uses filename-based comparison for normal cases.
        - BUT if a file only differs by name and has the same md5sum
          as an existing file in the DB (for that PMID), it is treated
          as UNCHANGED (no ADD/REMOVE).
    """
    to_update_file_list = defaultdict(list)
    to_remove_file_list = defaultdict(list)
    to_add_file_list = defaultdict(list)

    if not path.exists(pmcFileDir):
        logger.warning(f"{pmcFileDir} does not exist; no local PMC files to compare.")
        return {}, {}, {}

    if pmids_to_scan is not None:
        pmid_list = [str(p).split(":", 1)[-1].strip() for p in pmids_to_scan]
    else:
        pmid_list = [
            d for d in listdir(pmcFileDir)
            if path.isdir(path.join(pmcFileDir, d))
        ]

    for pmid in pmid_list:
        pmid = f"PMID:{pmid}"
        pmid_dir = path.join(pmcFileDir, pmid)
        if not path.isdir(pmid_dir):
            continue

        # DB state for this PMID: {file_name: {md5sum, file_class}}
        db_files = pmid_to_metadata_in_db.get(pmid, {})
        # New state on disk
        new_files: Dict[str, Dict[str, Any]] = {}

        # Scan disk files
        for pmcid in listdir(pmid_dir):
            sub_dir = path.join(pmid_dir, pmcid)
            if not path.isdir(sub_dir):
                continue

            for file_name in listdir(sub_dir):
                file_with_path = path.join(sub_dir, file_name)
                if not path.exists(file_with_path):
                    continue
                md5sum = get_md5sum(file_with_path)
                new_files[file_name] = {
                    "md5sum": md5sum,
                    "path": file_with_path,
                    "pmcid": pmcid,
                }

        # Build md5 sets to detect "same file, different name"
        db_md5s = {meta["md5sum"] for meta in db_files.values()}
        new_md5s = {meta["md5sum"] for meta in new_files.values()}

        # Determine additions & updates
        for file_name, new_meta in new_files.items():
            db_meta = db_files.get(file_name)

            if db_meta is None:
                # No DB entry with this name. Check if this md5sum already
                # exists in DB under a different name; if so, treat as rename-only.
                if new_meta["md5sum"] in db_md5s:
                    logger.info(
                        f"{pmid} file {file_name}: SAME CONTENT as an existing DB file "
                        f"(md5={new_meta['md5sum']}), treating as unchanged (name-only difference)"
                    )
                    continue

                # New file on disk, not in DB and md5 not seen before -> ADD
                logger.info(f"{pmid} file {file_name}: NEW (ADD)")
                to_add_file_list[pmid].append({
                    "file_name": file_name,
                    "md5sum": new_meta["md5sum"],
                    "path": new_meta["path"],
                    "pmcid": new_meta["pmcid"],
                })
            elif db_meta["md5sum"] != new_meta["md5sum"]:
                # Same name but content changed -> UPDATE
                logger.info(f"{pmid} file {file_name}: CHANGED (UPDATE)")
                to_update_file_list[pmid].append({
                    "file_name": file_name,
                    "old_md5sum": db_meta["md5sum"],
                    "new_md5sum": new_meta["md5sum"],
                    "path": new_meta["path"],
                    "pmcid": new_meta["pmcid"],
                    "file_class": db_meta["file_class"],
                })
            else:
                # Unchanged
                logger.debug(
                    f"{pmid} file {file_name}: UNCHANGED "
                    f"({db_meta['md5sum']} == {new_meta['md5sum']})"
                )

        # Determine removals (in DB but not on disk)
        for file_name, db_meta in db_files.items():
            if file_name not in new_files:
                # Check if there is a disk file with the same md5 under a different name.
                if db_meta["md5sum"] in new_md5s:
                    logger.info(
                        f"{pmid} file {file_name}: MISSING by name, but SAME CONTENT exists "
                        f"on disk (md5={db_meta['md5sum']}); treating as unchanged (name-only difference)"
                    )
                    continue

                logger.info(f"{pmid} file {file_name}: MISSING (REMOVE)")
                to_remove_file_list[pmid].append({
                    "file_name": file_name,
                    "md5sum": db_meta["md5sum"],
                    "file_class": db_meta["file_class"],
                })

    return (
        dict(to_update_file_list),
        dict(to_remove_file_list),
        dict(to_add_file_list),
    )


def batch_process_changed_files(to_update_file_list, to_remove_file_list, to_add_file_list, pmid_to_reference_id, pmids_for_retracted_papers):

    referencefile_ids_added = set()
    try:
        db = create_postgres_session(False)

        for pmid, files in to_update_file_list.items():
            for meta in files:
                destroy_file(
                    db,
                    pmid,
                    meta["file_name"],
                    meta["old_md5sum"],
                    meta.get("file_class", ""),
                    pmids_for_retracted_papers
                )
                add_file(
                    db,
                    pmid,
                    meta["file_name"],
                    meta["new_md5sum"],
                    meta.get("file_class", ""),
                    meta.get("pmcid", ""),
                    pmid_to_reference_id.get(pmid.replace("PMID:", "")),
                    referencefile_ids_added
                )

        for pmid, files in to_remove_file_list.items():
            for meta in files:
                destroy_file(
                    db,
                    pmid,
                    meta["file_name"],
                    meta["md5sum"],
                    meta.get("file_class", ""),
                    pmids_for_retracted_papers
                )

        for pmid, files in to_add_file_list.items():
            for meta in files:
                add_file(
                    db,
                    pmid,
                    meta["file_name"],
                    meta["md5sum"],
                    "",
                    meta.get("pmcid", ""),
                    pmid_to_reference_id.get(pmid.replace("PMID:", "")),
                    referencefile_ids_added
                )
    finally:
        db.commit()
        db.close()


def add_file(db, pmid, file_name, md5sum, old_file_class, pmcid, reference_id, referencefile_ids_added):
    """
    Add one PMC file for a given PMID into S3 and DB.
    """
    if reference_id is None:
        logger.info(f"{pmid}: reference_id is None; skipping add_file for {file_name}")
        return

    # Build full path to the file
    file_with_path = path.join(pmcFileDir, pmid, pmcid, file_name)
    if not path.exists(file_with_path):
        logger.info(f"{pmid}: file not found on disk: {file_with_path}")
        return

    # Make sure it is a gzipped file
    if file_with_path.endswith(".gz"):
        gzip_file_with_path = file_with_path
    else:
        gzip_file_with_path = gzip_file(file_with_path)

    if gzip_file_with_path is None:
        logger.warning(f"{pmid}: failed to gzip file {file_with_path}")
        return

    # Upload to s3
    status = upload_suppl_file_to_s3(gzip_file_with_path, md5sum)
    if not status:
        logger.warning(f"{pmid}: upload_suppl_file_to_s3 failed for {file_name}")
        return

    # Derive extension and classify file
    file_class = None
    if old_file_class and old_file_class == 'main' and file_name.endswith('.pdf'):
        file_class = old_file_class
    else:
        if "." in file_name:
            _, file_extension = file_name.rsplit(".", 1)
        else:
            file_extension = ""
        file_class = classify_pmc_file(file_name, file_extension.lower())

    logger.info(f"{pmid}: file_class for {file_name} is {file_class}")

    # Insert metadata into DB
    referencefile_id = None
    refFile = db.query(ReferencefileModel).filter_by(reference_id=reference_id, md5sum=md5sum).one_or_none()
    if refFile:
        logger.info(f"{pmid}: reference_id={reference_id} and md5sum={md5sum} is in the database.")
        referencefile_id = refFile.referencefile_id
    else:
        logger.info(f"{pmid}: adding referencefile row for {file_name}")
        referencefile_id = insert_referencefile(
            db,
            pmid,
            file_class,
            file_publication_status,
            file_name,
            reference_id,
            md5sum,
            logger,
        )

    if not referencefile_id:
        return

    if referencefile_id in referencefile_ids_added:
        logger.info(
            f"{pmid}: referencefile_mod already handled in this batch for "
            f"referencefile_id={referencefile_id}; skipping."
        )

    existing_mod = (
        db.query(ReferencefileModAssociationModel)
        .filter_by(referencefile_id=referencefile_id)
        .first()
    )

    if existing_mod:
        logger.info(
            f"{pmid}: referencefile_mod already exists in DB for "
            f"referencefile_id={referencefile_id}; skipping insert."
        )
    else:
        logger.info(
            f"{pmid}: adding referencefile_mod row for {file_name} - "
            f"referencefile_id = {referencefile_id}"
        )
        insert_referencefile_mod_for_pmc(
            db,
            pmid,
            file_name,
            referencefile_id,
            logger,
        )

    referencefile_ids_added.add(referencefile_id)


def destroy_file(db, pmid, file_name, md5sum, file_class, pmids_for_retracted_papers):

    referencefile = (
        db.query(ReferencefileModel)
        .join(
            CrossReferenceModel,
            ReferencefileModel.reference_id == CrossReferenceModel.reference_id,
        )
        .filter(
            CrossReferenceModel.curie == pmid,
            CrossReferenceModel.curie_prefix == "PMID",
            ReferencefileModel.md5sum == md5sum
        )
        .one_or_none()
    )

    if not referencefile:
        logger.info(f"{pmid}: no referencefile found for md5 {md5sum}, class {file_class}")
        return False

    remove_from_s3_and_db(db, referencefile)

    if file_class != 'main':
        logger.info(f"{pmid}: removing a suppl file ({file_name}).")
    else:
        if pmid in pmids_for_retracted_papers:
            logger.info(f"{pmid}: removing main PDF ({file_name}) for a retracted paper.")
        else:
            logger.info(f"{pmid}: removing main PDF ({file_name}) for a non-retracted paper.")

        # remove tei file if it exists
        tei_display_name = file_name.replace(".pdf", "")
        teiRefFile = (
            db.query(ReferencefileModel)
            .join(
                CrossReferenceModel,
                ReferencefileModel.reference_id == CrossReferenceModel.reference_id,
            )
            .filter(
                CrossReferenceModel.curie == pmid,
                CrossReferenceModel.curie_prefix == "PMID",
                ReferencefileModel.display_name == tei_display_name,
                ReferencefileModel.file_extension == "tei"
            )
            .one_or_none()
        )
        if teiRefFile:
            logger.info(f"{pmid}: removing tei file ({tei_display_name}.tei)")
            remove_from_s3_and_db(db, teiRefFile)

        try:
            cleanup_wft_tet_tags_for_deleted_main_pdf(db, referencefile.reference_id, [], 'all_access')
        except HTTPException as e:
            if e.status_code != 422:
                raise

    return True


def get_pmid_to_pmc_mapping(mapping_file, pmid_to_last_date_updated):
    """
    DEPRECATED: Uses oa_file_list.csv from FTP (retiring August 2026).
    Use get_pmid_to_pmcid_s3_mapping instead.
    """
    logger.warning("get_pmid_to_pmc_mapping using oa_file_list.csv is deprecated. "
                   "Use get_pmid_to_pmcid_s3_mapping for S3-based lookup.")

    logger.info("Reading oa_file_list.csv...")

    pmid_to_oa_url = {}

    with open(mapping_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pmid_raw = row.get("PMID", "") or ""
            if not pmid_raw.strip():
                continue

            pmid = f"PMID:{pmid_raw.strip()}"
            if pmid not in pmid_to_last_date_updated:
                continue

            file_path = (row.get("File", "") or "").strip()
            if not file_path:
                continue

            last_updated_str = (row.get("Last Updated (YYYY-MM-DD HH:MM:SS)", "") or "").strip()

            pmc_update_time = None
            if last_updated_str:
                try:
                    pmc_update_time = datetime.strptime(last_updated_str, OA_CSV_TIMEFMT)
                except ValueError:
                    continue

            lit_update_time = pmid_to_last_date_updated[pmid]
            if pmc_update_time is not None:
                if lit_update_time > pmc_update_time:
                    continue

            pmid_to_oa_url[pmid] = file_path

    logger.info(f"Loaded mapping for {len(pmid_to_oa_url)} PMIDs from oa_file_list.csv")
    return pmid_to_oa_url


def get_pmid_to_pmcid_s3_mapping(pmid_to_pmcid: Dict[str, str],
                                  pmid_to_last_date_updated: Dict[str, datetime]) -> Dict[str, str]:
    """
    Get PMID to PMCID mapping for packages that need updating, using S3 metadata.

    Checks each PMCID's last modified time in S3 and compares with our DB update time.

    Args:
        pmid_to_pmcid: Dict mapping PMID (with prefix) to PMCID (without prefix)
        pmid_to_last_date_updated: Dict mapping PMID to last update datetime in our DB

    Returns:
        Dict mapping PMID (with prefix) to PMCID for packages that need updating
    """
    from botocore.exceptions import ClientError

    logger.info("Checking S3 for PMC package updates...")

    s3_client = get_pmc_oa_s3_client()
    pmid_to_pmcid_to_update = {}
    checked = 0
    needs_update = 0

    for pmid, pmcid in pmid_to_pmcid.items():
        if pmid not in pmid_to_last_date_updated:
            # No update time in DB, include for checking
            pmid_to_pmcid_to_update[pmid] = pmcid
            needs_update += 1
            continue

        checked += 1
        if checked % 1000 == 0:
            logger.info(f"Checked {checked}/{len(pmid_to_pmcid)} PMCIDs...")

        # Get latest version prefix
        latest_version = get_latest_pmc_version(pmcid)
        if not latest_version:
            # Package not found in S3, skip
            continue

        # Get last modified time from S3 object metadata
        try:
            # Check the metadata JSON file for last modified
            json_key = f"{latest_version}/{latest_version}.json"
            response = s3_client.head_object(Bucket=PMC_OA_S3_BUCKET, Key=json_key)
            s3_last_modified = response['LastModified'].replace(tzinfo=None)

            lit_update_time = pmid_to_last_date_updated[pmid]

            # If S3 has newer version, include for update
            if s3_last_modified > lit_update_time:
                pmid_to_pmcid_to_update[pmid] = pmcid
                needs_update += 1
        except ClientError:
            # Try checking the directory itself if JSON doesn't exist
            try:
                response = s3_client.list_objects_v2(
                    Bucket=PMC_OA_S3_BUCKET,
                    Prefix=f"{latest_version}/",
                    MaxKeys=1
                )
                if response.get('Contents'):
                    s3_last_modified = response['Contents'][0]['LastModified'].replace(tzinfo=None)
                    lit_update_time = pmid_to_last_date_updated[pmid]
                    if s3_last_modified > lit_update_time:
                        pmid_to_pmcid_to_update[pmid] = pmcid
                        needs_update += 1
            except ClientError as e:
                logger.debug(f"Could not check S3 for {pmcid}: {e}")
                continue

    logger.info(f"Checked {checked} PMCIDs in S3, {needs_update} need updating")
    return pmid_to_pmcid_to_update


def get_pmid_to_pmcid_from_db() -> Dict[str, str]:
    """
    Get mapping of PMID to PMCID for all papers with PMC packages in our database.

    Returns:
        Dict mapping PMID (with prefix) to PMCID (without prefix)
    """
    db = create_postgres_session(False)
    try:
        rows = db.execute(text("""
            SELECT cr_pmid.curie AS pmid, cr_pmcid.curie AS pmcid
            FROM cross_reference cr_pmid
            JOIN cross_reference cr_pmcid ON cr_pmid.reference_id = cr_pmcid.reference_id
            JOIN referencefile rf ON cr_pmid.reference_id = rf.reference_id
            JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
            WHERE cr_pmid.curie_prefix = 'PMID'
              AND cr_pmid.is_obsolete = False
              AND cr_pmcid.curie_prefix = 'PMCID'
              AND cr_pmcid.is_obsolete = False
              AND rfm.mod_id IS NULL
            GROUP BY cr_pmid.curie, cr_pmcid.curie
        """)).fetchall()

        pmid_to_pmcid = {
            row.pmid: row.pmcid.replace("PMCID:", "")
            for row in rows
        }
        logger.info(f"Found {len(pmid_to_pmcid)} PMID-PMCID mappings in database")
        return pmid_to_pmcid
    finally:
        db.close()


def download_packages_from_s3_for_update(pmids_to_download: list, pmid_to_pmcid: Dict[str, str]):
    """
    Download PMC packages from S3 for the update workflow.

    Note: Older PMCIDs (below ~PMC9000000) may not be in the new S3 bucket yet.
    These will be skipped. The full migration is expected by August 2026.

    Args:
        pmids_to_download: List of PMIDs (with prefix) to download
        pmid_to_pmcid: Dict mapping PMID to PMCID
    """
    s3_success = 0
    s3_not_found = 0

    for pmid in pmids_to_download:
        pmid_numeric = pmid.replace("PMID:", "")
        pmcid = pmid_to_pmcid.get(pmid, "")
        if not pmcid:
            logger.warning(f"No PMCID found for {pmid}, skipping")
            continue

        # Create directory structure matching expected format
        # pmcFileDir/PMID:12345/PMC12345/files
        pmid_dir = path.join(pmcFileDir, pmid)
        if path.exists(pmid_dir) and listdir(pmid_dir):
            continue

        logger.info(f"{pmid} ({pmcid}): Downloading from S3...")
        makedirs(pmid_dir, exist_ok=True)

        success = download_pmc_package_from_s3(pmcid, pmid_dir)
        if success:
            s3_success += 1
        else:
            s3_not_found += 1
            # Clean up empty directory
            try:
                if path.exists(pmid_dir) and not listdir(pmid_dir):
                    import shutil
                    shutil.rmtree(pmid_dir)
            except Exception:
                pass
            logger.debug(f"PMC package for {pmid} ({pmcid}) not in S3 - may be older package not yet migrated")

    if s3_not_found > 0:
        logger.info(f"S3 download summary: {s3_success} successful, {s3_not_found} not found (older PMCIDs not yet in S3)")


def get_all_pmids_with_files_etc():
    """Get all PMIDs that have files in our database."""
    db = create_postgres_session(False)
    try:
        # PMIDs with any files
        rows = db.execute(text("""
            SELECT DISTINCT cr.curie
            FROM cross_reference cr
            JOIN referencefile rf ON cr.reference_id = rf.reference_id
            JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
            WHERE cr.curie_prefix = 'PMID'
              AND rfm.mod_id IS NULL
            ORDER BY cr.curie
        """)).fetchall()

        pmids_in_db = [row.curie for row in rows]

        # PMIDs that are retracted
        rows = db.execute(text("""
            SELECT DISTINCT cr.curie
             FROM cross_reference cr
            JOIN reference r ON cr.reference_id = r.reference_id
            WHERE cr.curie_prefix = 'PMID'
              AND (
                 r.pubmed_types::text LIKE '%Retracted%'
                 OR r.pubmed_types::text LIKE '%Retraction%'
              )
        """)).fetchall()

        pmids_for_retracted_papers = {row.curie for row in rows}

        # get last updated date
        rows = db.execute(text("""
            SELECT cr.curie AS pmid, MAX(rfm.date_updated) AS newest_date_updated
            FROM cross_reference cr
            JOIN referencefile rf  ON cr.reference_id = rf.reference_id
            JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
            WHERE cr.curie_prefix = 'PMID'
             AND rfm.mod_id IS NULL
            GROUP BY cr.curie
        """)).fetchall()

        # NOTE: result values are datetime (or None if DB side returned NULL)
        pmid_to_last_date_updated = {row.pmid: row.newest_date_updated for row in rows if row.newest_date_updated is not None}

        # Main PDFs
        # rows = db.execute(text("""
        #    SELECT cr.curie, rf.display_name, rf.file_extension
        #    FROM cross_reference cr
        #    JOIN referencefile rf ON cr.reference_id = rf.reference_id
        #    JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
        #    WHERE cr.curie_prefix = 'PMID'
        #      AND rf.file_class = 'main'
        #      AND rfm.mod_id IS NULL
        # """)).fetchall()

        # pmid_to_main_pdf_name = {
        #    row.curie: f"{row.display_name}.{row.file_extension}"
        #    for row in rows
        # }

        pmid_to_reference_id = get_pmid_to_reference_id_mapping(db)

        return pmids_in_db, pmid_to_reference_id, pmid_to_last_date_updated, pmids_for_retracted_papers
    finally:
        db.close()


def get_metadata_for_pmids(pmids: List[str]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Load metadata from database for specific PMIDs."""
    pmid_to_metadata_in_db: Dict[str, Dict[str, Dict[str, Any]]] = {}

    if not pmids:
        return pmid_to_metadata_in_db

    db = create_postgres_session(False)
    try:
        curie_list = [f"PMID:{pmid}" for pmid in pmids]

        query = text("""
            SELECT cr.curie, rf.display_name, rf.md5sum, rf.file_extension, rf.file_class
            FROM cross_reference cr
            JOIN referencefile rf ON cr.reference_id = rf.reference_id
            JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
            WHERE cr.curie_prefix = 'PMID'
              AND cr.curie = ANY(:curie_list)
              AND rfm.mod_id IS NULL
            ORDER BY cr.curie
        """)

        rows = db.execute(query, {"curie_list": curie_list}).fetchall()

        for curie, display_name, md5sum, file_extension, file_class in rows:
            file_name = f"{display_name}.{file_extension}"
            pmid_to_metadata_in_db.setdefault(curie, {})[file_name] = {
                "md5sum": md5sum,
                "file_class": file_class,
            }
    finally:
        db.close()

    return pmid_to_metadata_in_db


def generate_summary_report(stats: Dict, dry_run: bool):
    """Generate a comprehensive summary report."""
    end_time = datetime.now()
    duration = end_time - stats['start_time']

    summary = []
    summary.append("=" * 80)
    summary.append("ANNUAL PMC PACKAGE UPDATE - SUMMARY REPORT")
    summary.append("=" * 80)
    summary.append(f"Start Time: {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    summary.append(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    summary.append(f"Duration: {duration}")
    summary.append(f"Dry Run: {dry_run}")
    summary.append("")
    summary.append("PROCESSING STATISTICS:")
    summary.append(f"  Total PMIDs checked: {stats['total_pmids']}")
    summary.append(f"  Batches processed: {stats['batches_processed']}")
    summary.append("")
    summary.append("RESULTS:")
    summary.append(f"  PMIDs with file UPDATES: {len(stats['pmids_with_updates'])}")
    summary.append(f"  PMIDs with file ADDITIONS: {len(stats['pmids_with_additions'])}")
    summary.append(f"  PMIDs with file REMOVALS: {len(stats['pmids_with_removals'])}")
    summary.append(f"  PMIDs UNCHANGED: {len(stats['pmids_unchanged'])}")
    summary.append("")

    # Calculate unique PMIDs with any changes
    pmids_with_changes = (
        stats['pmids_with_updates'] | stats['pmids_with_removals'] | stats['pmids_with_additions']
    )
    change_percentage = (len(pmids_with_changes) / stats['total_pmids'] * 100) if stats['total_pmids'] > 0 else 0

    summary.append(f"  PMIDs with ANY changes: {len(pmids_with_changes)} ({change_percentage:.2f}%)")
    summary.append("")

    if stats['errors']:
        summary.append("ERRORS:")
        for error in stats['errors']:
            summary.append(f"  - {error}")
        summary.append("")

    summary.append("OUTPUT FILES:")
    summary.append(f"  Updates: {UPDATE_TSV}")
    summary.append(f"  Removals: {REMOVE_TSV}")
    summary.append(f"  Additions: {ADD_TSV}")
    summary.append(f"  Progress log: {PROGRESS_LOG}")
    summary.append(f"  Summary: {SUMMARY_LOG}")
    summary.append("=" * 80)

    summary_text = "\n".join(summary)

    # Write to summary file
    with open(SUMMARY_LOG, "w") as f:
        f.write(summary_text)

    # Also log to console
    log_progress("")
    log_progress(summary_text)


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Annual PMC Package Update - Check and update existing PMC packages"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of PMIDs to process per batch (default: 500)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform dry run - report what would be updated without making changes"
    )

    args = parser.parse_args()

    # Prepare data directory
    if path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    makedirs(DATA_DIR, exist_ok=True)

    # PMC FTP service deprecated August 2026. Using AWS S3 bucket pmc-oa-opendata.
    # See: https://ncbiinsights.ncbi.nlm.nih.gov/2026/02/12/pmc-article-dataset-distribution-services/
    log_progress("Using AWS S3 bucket pmc-oa-opendata for PMC package retrieval")

    # Run the update (no mapping_file needed - using S3 directly)
    annual_pmc_package_update(batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
