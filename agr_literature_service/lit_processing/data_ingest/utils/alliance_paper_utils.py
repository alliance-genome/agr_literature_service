"""
Shared utility functions for loading papers into the Alliance database.

This module contains common functions used by:
- load_interaction_papers.py
- load_goa_human_papers.py
- load_mod_gaf_papers.py
"""
import gzip
import logging
import shutil
import requests
from os import makedirs, path
from typing import Dict, List, Set, Tuple

from sqlalchemy import text

from agr_literature_service.api.models import ModCorpusAssociationModel, ModModel
from agr_literature_service.api.schemas import ModCorpusSortSourceType

logger = logging.getLogger(__name__)


def associate_papers_with_alliance(db_session, all_pmids: Set[str],
                                   mod_abbr: str = 'AGR',
                                   sort_source: ModCorpusSortSourceType = ModCorpusSortSourceType.Automated_alliance) -> int:
    """
    Associate papers with a MOD (default: AGR).
    Only associate papers that do NOT already have a mod_corpus_association
    with corpus=True for any MOD. This ensures we only add papers to the
    specified MOD that are not already in another MOD's corpus.

    Args:
        db_session: Database session
        all_pmids: Set of PMIDs to associate (without PMID: prefix)
        mod_abbr: MOD abbreviation to associate with (default: 'AGR')
        sort_source: The source type for mod_corpus_sort_source column
                     (default: Automated_alliance)

    Returns:
        Number of papers associated with the MOD
    """
    if not all_pmids:
        return 0

    # Get MOD record
    mod = db_session.query(ModModel).filter(
        ModModel.abbreviation == mod_abbr
    ).first()
    if not mod:
        logger.warning(f"{mod_abbr} MOD not found in database")
        return 0

    mod_id = mod.mod_id

    # Build parameterized query for PMIDs
    pmid_curies = [f"PMID:{pmid}" for pmid in all_pmids]

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
        return 0

    ref_ids_list = list(reference_ids_in_db)

    # Get reference_ids that already have corpus=True for any MOD
    # OR already have an association with the target MOD (to avoid unique constraint violation)
    refs_to_exclude_query = text(
        "SELECT DISTINCT reference_id FROM mod_corpus_association "
        "WHERE reference_id = ANY(:ref_ids) "
        "AND (corpus = True OR mod_id = :mod_id)"
    )
    refs_to_exclude = db_session.execute(
        refs_to_exclude_query,
        {"ref_ids": ref_ids_list, "mod_id": mod_id}
    ).fetchall()

    already_excluded = {row[0] for row in refs_to_exclude}

    # Add mod_corpus_association for papers not yet in any MOD's corpus
    # and not already associated with target MOD
    count = 0
    for ref_id in reference_ids_in_db:
        if ref_id not in already_excluded:
            mca = ModCorpusAssociationModel(
                reference_id=ref_id,
                mod_id=mod_id,
                corpus=True,
                mod_corpus_sort_source=sort_source
            )
            db_session.add(mca)
            count += 1

    if count > 0:
        db_session.commit()
        logger.info(f"Associated {count} paper(s) with {mod_abbr} MOD")

    return count


def search_pubmed_for_validity(pmids: Set[str], api_key: str = '',
                               timeout: int = 30) -> Tuple[Set[str], Set[str]]:
    """
    Check if PMIDs are valid or obsolete in PubMed.

    Args:
        pmids: Set of PMIDs to check (without PMID: prefix)
        api_key: NCBI API key (optional but recommended)
        timeout: Request timeout in seconds

    Returns:
        Tuple of (obsolete_pmids, valid_pmids)
    """
    pubmed_efetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    base_url = f"{pubmed_efetch_url}?db=pubmed&id="
    if api_key:
        base_url = f"{pubmed_efetch_url}?api_key={api_key}&db=pubmed&id="

    obsolete_pmids: Set[str] = set()
    valid_pmids: Set[str] = set()

    for pmid in pmids:
        url = f"{base_url}{pmid}"
        try:
            response = requests.get(url, timeout=timeout)
            content = response.text.replace("\n", "")
            if "<PubmedArticleSet></PubmedArticleSet>" in content:
                obsolete_pmids.add(pmid)
            else:
                valid_pmids.add(pmid)
        except requests.RequestException as e:
            logger.warning(f"Error checking PMID {pmid}: {e}")
            # Assume valid if we can't check
            valid_pmids.add(pmid)

    return obsolete_pmids, valid_pmids


def clean_up_tmp_directories(paths: List[str]) -> None:
    """
    Clean up and recreate temporary directories.

    Args:
        paths: List of directory paths to clean and recreate
    """
    for dir_path in paths:
        try:
            if path.exists(dir_path):
                shutil.rmtree(dir_path)
        except OSError as e:
            logger.info(f"Error deleting directory {dir_path}: {e.strerror}")

    for dir_path in paths:
        makedirs(dir_path, exist_ok=True)


def update_sgd_corpus_flag_to_true(db_session,  # pragma: no cover
                                   pmids: Set[str]) -> Tuple[int, Set[str]]:
    """
    Update SGD papers that are associated but outside corpus to be inside
    the corpus (corpus=True).

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

    # Update mod_corpus_association records where corpus=False or NULL to corpus=True
    # Use RETURNING to get the reference_ids that were actually updated
    update_query = text(
        "UPDATE mod_corpus_association "
        "SET corpus = True "
        "WHERE reference_id = ANY(:ref_ids) "
        "AND mod_id = :sgd_mod_id "
        "AND (corpus = False OR corpus IS NULL) "
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


def associate_sgd_papers_with_corpus(db_session, pmids: Set[str],  # pragma: no cover
                                     sort_source: ModCorpusSortSourceType) -> Tuple[int, Set[str]]:
    """
    Associate SGD papers with the SGD corpus.
    Only associates papers that do NOT already have a mod_corpus_association
    with SGD.

    Args:
        db_session: Database session
        pmids: Set of PMIDs to associate
        sort_source: The source type for mod_corpus_sort_source column

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
    pmids_added: Set[str] = set()
    for ref_id in reference_ids_in_db:
        if ref_id not in already_associated:
            mca = ModCorpusAssociationModel(
                reference_id=ref_id,
                mod_id=sgd_mod_id,
                corpus=True,
                mod_corpus_sort_source=sort_source
            )
            db_session.add(mca)
            count += 1
            if ref_id in ref_id_to_pmid:
                pmids_added.add(ref_id_to_pmid[ref_id])

    if count > 0:
        db_session.commit()
        logger.info(f"Associated {count} SGD paper(s) with SGD corpus")

    return count, pmids_added


def extract_pmids_from_gaf(file_with_path: str) -> Set[str]:
    """
    Extract all unique PMIDs from a GAF file.

    Args:
        file_with_path: Path to the GAF file (gzipped or plain text)

    Returns:
        Set of PMIDs (without PMID: prefix)
    """
    pmids, _ = extract_pmids_with_sources_from_gaf(file_with_path)
    return pmids


def extract_pmids_with_sources_from_gaf(file_with_path: str) -> Tuple[Set[str], Dict[str, Set[str]]]:
    """
    Extract all unique PMIDs and their annotation sources from a GAF file.

    Args:
        file_with_path: Path to the GAF file (gzipped or plain text)

    Returns:
        Tuple of (Set of PMIDs, Dict mapping PMID to set of sources)
    """
    all_pmids: Set[str] = set()
    pmid_sources: Dict[str, Set[str]] = {}

    try:
        # Handle both gzipped and plain text files
        if file_with_path.endswith('.gz'):
            f = gzip.open(file_with_path, "rt")
        else:
            f = open(file_with_path, "r")

        with f:
            for line in f:
                # Skip comment lines
                if line.startswith("!"):
                    continue

                parts = line.strip().split("\t")
                if len(parts) < 15:
                    continue

                # Column 6 (index 5) contains the DB:Reference field
                # Column 15 (index 14) contains the assigned_by field
                ref_col = parts[5]
                source = parts[14] if len(parts) > 14 else "Unknown"
                refs = ref_col.split("|")

                for ref in refs:
                    ref = ref.strip()
                    if ref.startswith("PMID:"):
                        pmid = ref.replace("PMID:", "")
                        if pmid.isdigit():
                            all_pmids.add(pmid)
                            if pmid not in pmid_sources:
                                pmid_sources[pmid] = set()
                            pmid_sources[pmid].add(source)
    except Exception as e:
        logger.error(f"Error reading GAF file {file_with_path}: {e}")

    return all_pmids, pmid_sources
