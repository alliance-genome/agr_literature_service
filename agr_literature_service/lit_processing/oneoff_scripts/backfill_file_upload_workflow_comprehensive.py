"""
Comprehensive backfill script for file upload workflow tags.

This script ensures all papers have the correct file upload workflow status:
- Papers with main PDF uploaded by a specific MOD → "file uploaded" for that MOD only
- Papers with main PDF uploaded from PMC (mod_id is NULL) → "file uploaded" for all associated MODs
- Papers with other files but no main PDF → "file upload in progress"
- Papers with no files → "file needed" (or leave unchanged if already set)

Uses workflow transitions so downstream transitions (like "text conversion needed") are
automatically triggered.
"""
import argparse
import logging
from os import path
from typing import Dict, Optional, Set, Tuple

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.workflow_tag_crud import transition_to_workflow_status
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

BATCH_SIZE = 250

# Workflow tag ATP IDs for file upload process (ATP:0000140)
FILE_UPLOADED_TAG = "ATP:0000134"       # file uploaded
FILE_IN_PROGRESS_TAG = "ATP:0000139"    # file upload in progress
FILE_NEEDED_TAG = "ATP:0000141"         # file needed


def backfill_file_upload_workflow(dry_run: bool = False):  # pragma: no cover
    """
    Main function to backfill file upload workflow tags for all MODs.

    Args:
        dry_run: If True, only log what would be done without making changes
    """
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    logger.info("=" * 60)
    logger.info("Starting comprehensive file upload workflow backfill")
    logger.info(f"Dry run: {dry_run}")
    logger.info("=" * 60)

    # Get all MOD info
    mod_rows = db.execute(text("SELECT mod_id, abbreviation FROM mod")).fetchall()
    mod_id_to_abbr = {row[0]: row[1] for row in mod_rows}
    mod_abbr_to_id = {row[1]: row[0] for row in mod_rows}

    # Get current file upload workflow tags for all references
    logger.info("Loading current file upload workflow tags...")
    current_wft = get_current_file_upload_wfts(db)
    logger.info(f"Found {len(current_wft)} existing file upload workflow tags")

    # Get file upload status for all references
    logger.info("Loading file upload status for all references...")
    (refs_with_main_pdf_by_mod,
     refs_with_main_pdf_from_pmc,
     refs_with_files_no_main_pdf) = get_file_upload_status(db)
    logger.info(f"References with main PDF by MOD: {sum(len(v) for v in refs_with_main_pdf_by_mod.values())}")
    logger.info(f"References with main PDF from PMC: {len(refs_with_main_pdf_from_pmc)}")
    logger.info(f"References with files but no main PDF: {len(refs_with_files_no_main_pdf)}")

    # Get MOD corpus associations
    logger.info("Loading MOD corpus associations...")
    ref_to_mods = get_mod_corpus_associations(db)
    logger.info(f"Found corpus associations for {len(ref_to_mods)} references")

    # Process each category
    process_mod_specific_main_pdfs(db, refs_with_main_pdf_by_mod, mod_id_to_abbr,
                                   ref_to_mods, current_wft, dry_run)

    process_pmc_main_pdfs(db, refs_with_main_pdf_from_pmc, mod_abbr_to_id,
                          ref_to_mods, current_wft, dry_run)

    process_files_without_main_pdf(db, refs_with_files_no_main_pdf, refs_with_main_pdf_from_pmc,
                                   refs_with_main_pdf_by_mod, mod_abbr_to_id, ref_to_mods,
                                   current_wft, dry_run)

    logger.info("\n" + "=" * 60)
    logger.info("Backfill complete!")
    db.close()


def process_mod_specific_main_pdfs(db, refs_with_main_pdf_by_mod: Dict[int, Set[int]],
                                   mod_id_to_abbr: Dict[int, str],
                                   ref_to_mods: Dict[int, Set[str]],
                                   current_wft: Dict[Tuple[int, int], str],
                                   dry_run: bool):  # pragma: no cover
    """Process references with main PDF uploaded by specific MOD."""
    logger.info("\n" + "=" * 60)
    logger.info("Processing references with main PDF uploaded by specific MOD...")
    transition_count = 0
    error_count = 0

    for mod_id, reference_ids in refs_with_main_pdf_by_mod.items():
        mod_abbr = mod_id_to_abbr.get(mod_id)
        if not mod_abbr:
            continue

        for ref_id in reference_ids:
            if ref_id not in ref_to_mods or mod_abbr not in ref_to_mods[ref_id]:
                continue

            current_tag = current_wft.get((ref_id, mod_id))
            if current_tag == FILE_UPLOADED_TAG:
                continue

            if dry_run:
                logger.info(f"[DRY RUN] Would transition ref={ref_id}, mod={mod_abbr} "
                            f"from {current_tag} to {FILE_UPLOADED_TAG}")
            else:
                success = safe_transition(db, ref_id, mod_abbr, FILE_UPLOADED_TAG, current_tag)
                if success:
                    transition_count += 1
                else:
                    error_count += 1

            if transition_count % BATCH_SIZE == 0 and transition_count > 0:
                db.commit()
                logger.info(f"Progress: {transition_count} transitions completed")

    db.commit()
    logger.info(f"MOD-specific main PDF: {transition_count} transitions, {error_count} errors")


def process_pmc_main_pdfs(db, refs_with_main_pdf_from_pmc: Set[int],
                          mod_abbr_to_id: Dict[str, int],
                          ref_to_mods: Dict[int, Set[str]],
                          current_wft: Dict[Tuple[int, int], str],
                          dry_run: bool):  # pragma: no cover
    """Process references with main PDF from PMC (set for all associated MODs)."""
    logger.info("\n" + "=" * 60)
    logger.info("Processing references with main PDF from PMC...")
    transition_count = 0
    error_count = 0

    for ref_id in refs_with_main_pdf_from_pmc:
        if ref_id not in ref_to_mods:
            continue

        for mod_abbr in ref_to_mods[ref_id]:
            mod_id = mod_abbr_to_id.get(mod_abbr)
            if not mod_id:
                continue

            current_tag = current_wft.get((ref_id, mod_id))
            if current_tag == FILE_UPLOADED_TAG:
                continue

            if dry_run:
                logger.info(f"[DRY RUN] Would transition ref={ref_id}, mod={mod_abbr} "
                            f"from {current_tag} to {FILE_UPLOADED_TAG}")
            else:
                success = safe_transition(db, ref_id, mod_abbr, FILE_UPLOADED_TAG, current_tag)
                if success:
                    transition_count += 1
                else:
                    error_count += 1

            if transition_count % BATCH_SIZE == 0 and transition_count > 0:
                db.commit()
                logger.info(f"Progress: {transition_count} transitions completed")

    db.commit()
    logger.info(f"PMC main PDF: {transition_count} transitions, {error_count} errors")


def process_files_without_main_pdf(db, refs_with_files_no_main_pdf: Set[int],
                                   refs_with_main_pdf_from_pmc: Set[int],
                                   refs_with_main_pdf_by_mod: Dict[int, Set[int]],
                                   mod_abbr_to_id: Dict[str, int],
                                   ref_to_mods: Dict[int, Set[str]],
                                   current_wft: Dict[Tuple[int, int], str],
                                   dry_run: bool):  # pragma: no cover
    """Process references with files but no main PDF → file upload in progress."""
    logger.info("\n" + "=" * 60)
    logger.info("Processing references with files but no main PDF...")
    transition_count = 0
    error_count = 0

    # Build set of all references with main PDF
    all_refs_with_main_pdf = refs_with_main_pdf_from_pmc.copy()
    for mod_refs in refs_with_main_pdf_by_mod.values():
        all_refs_with_main_pdf.update(mod_refs)

    for ref_id in refs_with_files_no_main_pdf:
        if ref_id in all_refs_with_main_pdf:
            continue

        if ref_id not in ref_to_mods:
            continue

        for mod_abbr in ref_to_mods[ref_id]:
            mod_id = mod_abbr_to_id.get(mod_abbr)
            if not mod_id:
                continue

            current_tag = current_wft.get((ref_id, mod_id))
            if current_tag in (FILE_UPLOADED_TAG, FILE_IN_PROGRESS_TAG):
                continue

            if dry_run:
                logger.info(f"[DRY RUN] Would transition ref={ref_id}, mod={mod_abbr} "
                            f"from {current_tag} to {FILE_IN_PROGRESS_TAG}")
            else:
                success = safe_transition(db, ref_id, mod_abbr, FILE_IN_PROGRESS_TAG, current_tag)
                if success:
                    transition_count += 1
                else:
                    error_count += 1

            if transition_count % BATCH_SIZE == 0 and transition_count > 0:
                db.commit()
                logger.info(f"Progress: {transition_count} transitions completed")

    db.commit()
    logger.info(f"Files without main PDF: {transition_count} transitions, {error_count} errors")


def safe_transition(db, reference_id: int, mod_abbr: str, target_tag: str,
                    current_tag: Optional[str]) -> bool:  # pragma: no cover
    """
    Safely transition workflow status, handling errors gracefully.

    Returns True if successful, False otherwise.
    """
    try:
        # If no current tag exists and we're not setting to file_needed,
        # we may need to create the initial state first
        if current_tag is None and target_tag != FILE_NEEDED_TAG:
            try:
                transition_to_workflow_status(db, str(reference_id), mod_abbr, FILE_NEEDED_TAG)
            except Exception:
                pass  # May already exist or have other state

        transition_to_workflow_status(db, str(reference_id), mod_abbr, target_tag)
        logger.info(f"Transitioned ref={reference_id}, mod={mod_abbr}: "
                    f"{current_tag} -> {target_tag}")
        return True
    except Exception as e:
        logger.error(f"Failed to transition ref={reference_id}, mod={mod_abbr}: {e}")
        db.rollback()
        return False


def get_current_file_upload_wfts(db) -> Dict[Tuple[int, int], str]:  # pragma: no cover
    """
    Get current file upload workflow tags for all references.

    Returns:
        Dict mapping (reference_id, mod_id) -> workflow_tag_id
    """
    wft_ids = [FILE_UPLOADED_TAG, FILE_IN_PROGRESS_TAG, FILE_NEEDED_TAG]
    wft_ids_str = ",".join(f"'{w}'" for w in wft_ids)

    rows = db.execute(text(f"""
        SELECT reference_id, mod_id, workflow_tag_id
        FROM workflow_tag
        WHERE workflow_tag_id IN ({wft_ids_str})
    """)).fetchall()

    return {(row[0], row[1]): row[2] for row in rows}


def get_file_upload_status(db) -> Tuple[Dict[int, Set[int]], Set[int], Set[int]]:  # pragma: no cover
    """
    Get file upload status for all references.

    Returns:
        Tuple of:
        - refs_with_main_pdf_by_mod: Dict[mod_id, Set[reference_id]] for MOD-specific uploads
        - refs_with_main_pdf_from_pmc: Set[reference_id] for PMC uploads (mod_id is NULL)
        - refs_with_files_no_main_pdf: Set[reference_id] with files but no main PDF
    """
    rows = db.execute(text("""
        SELECT rf.reference_id, rfm.mod_id,
               rf.file_class, rf.file_publication_status, rf.pdf_type, rf.file_extension
        FROM referencefile rf
        JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
    """)).fetchall()

    refs_with_main_pdf_by_mod: Dict[int, Set[int]] = {}
    refs_with_main_pdf_from_pmc: Set[int] = set()
    refs_with_any_file: Set[int] = set()

    for row in rows:
        ref_id = row[0]
        mod_id = row[1]
        file_class = row[2]
        file_status = row[3]
        pdf_type = row[4]
        file_ext = row[5]

        refs_with_any_file.add(ref_id)

        # Check if this is a main PDF (final, pdf extension, main class)
        is_main_pdf = (file_class == 'main'
                       and file_status == 'final'
                       and file_ext == 'pdf'
                       and (pdf_type == 'pdf' or pdf_type is None))

        if is_main_pdf:
            if mod_id is None:
                refs_with_main_pdf_from_pmc.add(ref_id)
            else:
                if mod_id not in refs_with_main_pdf_by_mod:
                    refs_with_main_pdf_by_mod[mod_id] = set()
                refs_with_main_pdf_by_mod[mod_id].add(ref_id)

    # References with files but no main PDF
    all_refs_with_main_pdf = refs_with_main_pdf_from_pmc.copy()
    for mod_refs in refs_with_main_pdf_by_mod.values():
        all_refs_with_main_pdf.update(mod_refs)

    refs_with_files_no_main_pdf = refs_with_any_file - all_refs_with_main_pdf

    return refs_with_main_pdf_by_mod, refs_with_main_pdf_from_pmc, refs_with_files_no_main_pdf


def get_mod_corpus_associations(db) -> Dict[int, Set[str]]:  # pragma: no cover
    """
    Get MOD corpus associations for all references.

    Returns:
        Dict mapping reference_id -> Set of MOD abbreviations in corpus
    """
    rows = db.execute(text("""
        SELECT mca.reference_id, m.abbreviation
        FROM mod_corpus_association mca
        JOIN mod m ON mca.mod_id = m.mod_id
        WHERE mca.corpus = TRUE
    """)).fetchall()

    ref_to_mods: Dict[int, Set[str]] = {}
    for row in rows:
        ref_id = row[0]
        mod_abbr = row[1]
        # Skip AGR - it doesn't have file upload workflow transitions configured
        if mod_abbr == 'AGR':
            continue
        if ref_id not in ref_to_mods:
            ref_to_mods[ref_id] = set()
        ref_to_mods[ref_id].add(mod_abbr)

    return ref_to_mods


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill file upload workflow tags for all references"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Log what would be done without making changes"
    )
    args = parser.parse_args()

    backfill_file_upload_workflow(dry_run=args.dry_run)
