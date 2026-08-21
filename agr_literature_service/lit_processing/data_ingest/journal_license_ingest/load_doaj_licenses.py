"""
load_doaj_licenses.py
=====================

Monthly script to load open-access license data from DOAJ (Directory of
Open Access Journals, https://doaj.org) into the Resource table.

For each journal in the DOAJ CSV export:
1. Match the journal to a resource via its ISSN/EISSN cross-references
   (cross_reference rows with curie_prefix='ISSN')
2. Set license_list, license_start_year and copyright_license_id
   (the most restricted license from the DOAJ license list)

By default only resources with no existing license data are updated, so
journals newly added to DOAJ get their licenses loaded without clobbering
existing (possibly manually curated) license data. Use --update-existing
to also refresh resources whose stored license data differs from DOAJ.

Usage:
    python load_doaj_licenses.py [--input FILE] [--dry-run] [--update-existing]

Options:
    --input FILE        Read a local DOAJ CSV file instead of downloading
    --dry-run           Show what would be updated without making changes
    --update-existing   Also update resources whose license data differs
"""

import argparse
import csv
import io
import logging
import sys
from os import path
from typing import Dict, List, Optional, Tuple

import requests
from sqlalchemy.orm import Session

from agr_literature_service.api.models import (
    CopyrightLicenseModel,
    CrossReferenceModel,
    ResourceModel,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

DOAJ_CSV_URL = "https://doaj.org/csv"
DOWNLOAD_TIMEOUT_SECONDS = 300

# DOAJ CSV column names
COL_TITLE = "Journal title"
COL_ISSN = "Journal ISSN (print version)"
COL_EISSN = "Journal EISSN (online version)"
COL_LICENSE = "Journal license"
COL_OA_START_YEAR = ("When did the journal start to publish all content "
                     "using an open license?")

# License restriction order: most restricted first, based on
# Creative Commons license restrictiveness
LICENSE_RESTRICTION_ORDER = [
    "CC BY-NC-ND",   # Most restricted: no commercial, no derivatives
    "CC BY-ND",      # No derivatives (commercial OK)
    "CC BY-NC-SA",   # No commercial, share-alike
    "CC BY-NC",      # No commercial
    "CC BY-SA",      # Share-alike (commercial OK)
    "CC BY",         # Attribution only
    "CC0",           # Public domain dedication - least restricted
]

# Map DOAJ license names to copyright_license table names
LICENSE_NAME_ALIASES = {
    "PUBLIC DOMAIN": "CC0",
}

# Commit after this many resource updates to avoid losing work on crash
BATCH_COMMIT_SIZE = 100


def normalize_license_name(license_name: str) -> str:
    """Normalize a license name for comparison."""
    name = license_name.strip().upper().replace("  ", " ")
    return LICENSE_NAME_ALIASES.get(name, name)


def normalize_issn(issn: str) -> str:
    """Normalize an ISSN for matching: strip and remove hyphens."""
    return issn.strip().replace("-", "").upper()


def parse_license_list(license_str: str) -> List[str]:
    """Parse DOAJ's comma-separated license list, e.g. "CC BY, CC BY-NC"."""
    if not license_str or not license_str.strip():
        return []
    return [lic.strip() for lic in license_str.split(",") if lic.strip()]


def get_most_restricted_license(license_list: List[str]) -> Optional[str]:
    """Return the most restricted recognized license from the list."""
    normalized_licenses = [normalize_license_name(lic) for lic in license_list]
    for restricted in LICENSE_RESTRICTION_ORDER:
        if restricted in normalized_licenses:
            return restricted
    return None


def download_doaj_csv(url: str = DOAJ_CSV_URL) -> str:
    """Download the DOAJ journal list CSV export and return its content."""
    logger.info(f"Downloading DOAJ journal list from {url}")
    response = requests.get(url, timeout=DOWNLOAD_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def parse_doaj_csv(csv_content: str) -> Dict[str, dict]:
    """Parse the DOAJ CSV export into a mapping keyed by normalized ISSN.

    Both the print ISSN and the EISSN of a journal map to the same entry:
    {'title': str, 'license_list': List[str], 'oa_start_year': Optional[int]}
    """
    reader = csv.DictReader(io.StringIO(csv_content))
    fieldnames = reader.fieldnames or []
    required_columns = [COL_TITLE, COL_ISSN, COL_EISSN, COL_LICENSE, COL_OA_START_YEAR]
    missing = [col for col in required_columns if col not in fieldnames]
    if missing:
        raise ValueError(f"DOAJ CSV is missing expected column(s): {missing}")

    issn_to_journal: Dict[str, dict] = {}
    total = 0
    no_license = 0
    no_issn = 0
    for row in reader:
        total += 1
        title = (row.get(COL_TITLE) or "").strip()
        license_list = parse_license_list(row.get(COL_LICENSE) or "")
        if not license_list:
            no_license += 1
            continue

        oa_start_year: Optional[int] = None
        year_str = (row.get(COL_OA_START_YEAR) or "").strip()
        if year_str:
            try:
                oa_start_year = int(year_str)
            except ValueError:
                logger.warning(f"Invalid OA start year '{year_str}' for journal '{title}'")

        journal = {
            "title": title,
            "license_list": license_list,
            "oa_start_year": oa_start_year,
        }
        issns = [normalize_issn(row.get(COL_ISSN) or ""),
                 normalize_issn(row.get(COL_EISSN) or "")]
        issns = [issn for issn in issns if issn]
        if not issns:
            no_issn += 1
            continue
        for issn in issns:
            issn_to_journal[issn] = journal

    logger.info(f"Parsed {total} DOAJ journals: {len(issn_to_journal)} ISSN keys, "
                f"{no_license} without license data, {no_issn} without ISSN")
    return issn_to_journal


def load_license_name_to_id(db: Session) -> Dict[str, int]:
    """Map normalized copyright license names to copyright_license_id."""
    license_map: Dict[str, int] = {}
    for lic in db.query(CopyrightLicenseModel).all():
        license_map[normalize_license_name(lic.name)] = lic.copyright_license_id
    return license_map


def find_matching_resources(db: Session, issn_to_journal: Dict[str, dict]) -> List[Tuple[int, dict]]:
    """Find resources whose ISSN cross-references match DOAJ journals.

    Returns:
        List of (resource_id, doaj_journal_dict) tuples, one per resource.
    """
    query = db.query(
        CrossReferenceModel.resource_id,
        CrossReferenceModel.curie
    ).filter(
        CrossReferenceModel.curie_prefix == 'ISSN',
        CrossReferenceModel.is_obsolete.is_(False),
        CrossReferenceModel.resource_id.isnot(None)
    )

    resource_to_journal: Dict[int, dict] = {}
    for resource_id, curie in query.all():
        issn = normalize_issn(curie.replace('ISSN:', ''))
        journal = issn_to_journal.get(issn)
        if journal is None:
            continue
        existing = resource_to_journal.get(resource_id)
        if existing is not None and existing is not journal:
            logger.warning(f"Resource {resource_id}: multiple ISSNs match different "
                           f"DOAJ journals ('{existing['title']}' vs '{journal['title']}'); "
                           f"keeping the first match")
            continue
        resource_to_journal[resource_id] = journal

    logger.info(f"Matched {len(resource_to_journal)} resources to DOAJ journals")
    return sorted(resource_to_journal.items())


def update_resources(db: Session, matches: List[Tuple[int, dict]],
                     license_map: Dict[str, int], update_existing: bool = False,
                     dry_run: bool = False) -> Dict[str, int]:
    """Apply DOAJ license data to matched resources.

    Returns:
        dict with statistics: {'updated', 'skipped_has_license',
        'skipped_unchanged', 'errors'}
    """
    stats = {'updated': 0, 'skipped_has_license': 0, 'skipped_unchanged': 0, 'errors': 0}
    updates_since_commit = 0

    for resource_id, journal in matches:
        license_list = journal["license_list"]
        oa_start_year = journal["oa_start_year"]
        most_restricted = get_most_restricted_license(license_list)
        mapped_license_id = license_map.get(most_restricted) if most_restricted else None
        if mapped_license_id is None:
            logger.warning(f"Resource {resource_id} ('{journal['title'][:50]}'): no license in "
                           f"{license_list} maps to the copyright_license table")

        try:
            resource = db.query(ResourceModel).filter(
                ResourceModel.resource_id == resource_id
            ).one_or_none()
            if resource is None:
                logger.warning(f"Resource {resource_id} not found")
                stats['errors'] += 1
                continue

            # Never clear curated values with None: when DOAJ has no mapped
            # license or no OA start year, keep the existing values
            new_license_id = mapped_license_id if mapped_license_id is not None \
                else resource.copyright_license_id
            new_start_year = oa_start_year if oa_start_year is not None \
                else resource.license_start_year

            has_license_data = (bool(resource.license_list)
                                or resource.copyright_license_id is not None
                                or resource.license_start_year is not None)
            unchanged = (
                (resource.license_list or []) == license_list
                and resource.license_start_year == new_start_year
                and resource.copyright_license_id == new_license_id
            )
            if unchanged:
                stats['skipped_unchanged'] += 1
                continue
            if has_license_data and not update_existing:
                stats['skipped_has_license'] += 1
                logger.debug(f"Skipping {resource.curie}: already has license data")
                continue

            action = "Would update" if dry_run else "Updated"
            logger.info(f"{action}: {resource.curie} ('{journal['title'][:50]}') - "
                        f"licenses={license_list}, year={new_start_year}, "
                        f"most_restricted={most_restricted}")
            if not dry_run:
                resource.license_list = license_list
                resource.license_start_year = new_start_year
                resource.copyright_license_id = new_license_id
                updates_since_commit += 1
            stats['updated'] += 1

            if not dry_run and updates_since_commit >= BATCH_COMMIT_SIZE:
                db.commit()
                logger.info(f"Batch commit ({updates_since_commit} updates)")
                updates_since_commit = 0

        except Exception as e:
            logger.error(f"Error updating resource {resource_id}: {e}")
            db.rollback()
            stats['errors'] += 1
            # the rollback also discarded every update pending since the last
            # commit, so keep the summary honest
            if updates_since_commit:
                logger.error(f"Rollback discarded {updates_since_commit} pending "
                             f"updates since the last commit")
                stats['updated'] -= updates_since_commit
                stats['errors'] += updates_since_commit
                updates_since_commit = 0

    if not dry_run and updates_since_commit > 0:
        try:
            db.commit()
            logger.info(f"Final commit ({updates_since_commit} updates)")
        except Exception as e:
            db.rollback()
            logger.error(f"Final commit failed, discarding {updates_since_commit} updates: {e}")
            stats['updated'] -= updates_since_commit
            stats['errors'] += updates_since_commit

    return stats


def load_doaj_licenses(input_file: Optional[str] = None, dry_run: bool = False,
                       update_existing: bool = False) -> None:
    """Download/read the DOAJ journal list and load license data into resources."""
    if input_file:
        logger.info(f"Reading DOAJ CSV from local file: {input_file}")
        with open(input_file, "r", encoding="utf-8") as f:
            csv_content = f.read()
    else:
        csv_content = download_doaj_csv()

    issn_to_journal = parse_doaj_csv(csv_content)

    db = create_postgres_session(False)
    try:
        script_name = path.basename(__file__).replace(".py", "")
        set_global_user_id(db, script_name)

        license_map = load_license_name_to_id(db)
        logger.info(f"Loaded {len(license_map)} copyright license mappings")

        matches = find_matching_resources(db, issn_to_journal)
        stats = update_resources(db, matches, license_map,
                                 update_existing=update_existing, dry_run=dry_run)

        logger.info("=" * 50)
        logger.info("Summary:")
        logger.info(f"  Matched resources:       {len(matches)}")
        logger.info(f"  Updated:                 {stats['updated']}")
        logger.info(f"  Skipped (has license):   {stats['skipped_has_license']}")
        logger.info(f"  Skipped (unchanged):     {stats['skipped_unchanged']}")
        logger.info(f"  Errors:                  {stats['errors']}")
        logger.info("=" * 50)
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description='Load open-access license data from DOAJ into the Resource table'
    )
    parser.add_argument(
        '-i', '--input',
        default=None,
        help='Local DOAJ CSV file (default: download from https://doaj.org/csv)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be updated without making changes'
    )
    parser.add_argument(
        '--update-existing',
        action='store_true',
        help='Also update resources whose stored license data differs from DOAJ'
    )
    args = parser.parse_args()

    logger.info("Starting load_doaj_licenses")
    if args.dry_run:
        logger.info("DRY-RUN mode: no changes will be made")

    load_doaj_licenses(input_file=args.input, dry_run=args.dry_run,
                       update_existing=args.update_existing)

    logger.info("Finished load_doaj_licenses")


if __name__ == "__main__":
    main()
