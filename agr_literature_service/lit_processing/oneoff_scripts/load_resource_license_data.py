"""
load_resource_license_data.py
=============================
Load copyright license data into Resource table from DOAJ export.

Input file format (TSV):
Journal_Title    ISSN    EISSN    License_list    OA_Start_Year

Usage:
    python load_resource_license_data.py [input_file]

If no input file is provided, it will download from the default URL.
"""
import logging
import sys
from os import path
from typing import Dict, List, Optional, Tuple
import requests

from sqlalchemy import or_
from sqlalchemy.orm import Session

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ResourceModel, CopyrightLicenseModel
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_URL = "https://dev-shuai.alliancegenome.org/ABC/resource_licenses_doaj.txt"
LOCAL_FILE = "data/resource_licenses_doaj.txt"

# License restriction order: most restricted first
# Based on Creative Commons license restrictiveness
LICENSE_RESTRICTION_ORDER = [
    "CC BY-NC-ND",  # Most restricted: no commercial, no derivatives
    "CC BY-ND",      # No derivatives (commercial OK)
    "CC BY-NC-SA",   # No commercial, share-alike
    "CC BY-NC",      # No commercial
    "CC BY-SA",      # Share-alike (commercial OK)
    "CC BY",         # Attribution only
    "CC0",           # Public domain - least restricted
]


def normalize_license_name(license_name: str) -> str:
    """Normalize license name for comparison."""
    return license_name.strip().upper().replace("  ", " ")


def get_most_restricted_license(license_list: List[str]) -> Optional[str]:
    """
    Get the most restricted license from the list.
    Returns the license name as it should appear in the database.
    """
    if not license_list:
        return None

    normalized_licenses = [normalize_license_name(lic) for lic in license_list]

    for restricted_license in LICENSE_RESTRICTION_ORDER:
        normalized_restricted = normalize_license_name(restricted_license)
        for i, norm_lic in enumerate(normalized_licenses):
            if norm_lic == normalized_restricted:
                # Return the original (non-normalized) version from input
                return license_list[i].strip()

    # If none of the standard licenses match, return the first one
    logger.warning(f"Unknown license(s) in list: {license_list}")
    return license_list[0].strip() if license_list else None


def load_license_name_to_id(db: Session) -> Dict[str, int]:
    """
    Load all copyright licenses and create a mapping from name to ID.
    """
    license_map = {}
    licenses = db.query(CopyrightLicenseModel).all()
    for lic in licenses:
        # Store with normalized key for lookup
        license_map[normalize_license_name(lic.name)] = lic.copyright_license_id
        # Also store original name
        license_map[lic.name] = lic.copyright_license_id
    return license_map


def find_resource_by_issn(db: Session, issn: str, eissn: str) -> Optional[ResourceModel]:
    """
    Find a resource by ISSN or EISSN.
    """
    conditions = []
    if issn:
        issn = issn.strip()
        conditions.append(ResourceModel.print_issn == issn)
        conditions.append(ResourceModel.online_issn == issn)
    if eissn:
        eissn = eissn.strip()
        conditions.append(ResourceModel.print_issn == eissn)
        conditions.append(ResourceModel.online_issn == eissn)

    if not conditions:
        return None

    resource = db.query(ResourceModel).filter(or_(*conditions)).first()
    return resource


def parse_license_list(license_str: str) -> List[str]:
    """
    Parse a comma-separated license list string.
    Example: "CC BY, CC BY-SA, CC BY-ND" -> ["CC BY", "CC BY-SA", "CC BY-ND"]
    """
    if not license_str or not license_str.strip():
        return []
    return [lic.strip() for lic in license_str.split(",") if lic.strip()]


def download_file(url: str) -> str:
    """Download file from URL and return content."""
    logger.info(f"Downloading from {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.text


def load_data(input_source: str = None) -> None:
    """
    Load license data from TSV file into Resource table.
    """
    db = create_postgres_session(False)

    # Set script user for audit trail
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    # Load license name to ID mapping
    license_map = load_license_name_to_id(db)
    logger.info(f"Loaded {len(license_map)} license mappings")

    # Statistics
    updated_count = 0
    not_found_count = 0
    skipped_count = 0
    error_count = 0

    # Get input data
    if input_source and path.exists(input_source):
        logger.info(f"Reading from local file: {input_source}")
        with open(input_source, "r", encoding="utf-8") as f:
            lines = f.readlines()
    else:
        url = input_source if input_source and input_source.startswith("http") else DEFAULT_URL
        content = download_file(url)
        lines = content.splitlines()

    for ln, line in enumerate(lines, start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        # Skip header line
        if ln == 1 and "Journal_Title" in line:
            logger.info("Skipping header line")
            continue

        items = line.split("\t")

        if len(items) < 5:
            skipped_count += 1
            logger.warning(f"Line {ln}: skipped (expected 5 columns, got {len(items)})")
            continue

        journal_title = items[0].strip()
        issn = items[1].strip()
        eissn = items[2].strip()
        license_str = items[3].strip()
        oa_start_year_str = items[4].strip()

        # Parse license list
        license_list = parse_license_list(license_str)
        if not license_list:
            skipped_count += 1
            logger.warning(f"Line {ln}: skipped (no license data) - {journal_title}")
            continue

        # Parse start year
        license_start_year = None
        if oa_start_year_str:
            try:
                license_start_year = int(oa_start_year_str)
            except ValueError:
                logger.warning(f"Line {ln}: invalid year '{oa_start_year_str}' for {journal_title}")

        try:
            # Find resource by ISSN/EISSN
            resource = find_resource_by_issn(db, issn, eissn)

            if not resource:
                not_found_count += 1
                logger.info(f"Line {ln}: resource not found - ISSN={issn}, EISSN={eissn}, Title={journal_title}")
                continue

            # Get most restricted license and look up its ID
            most_restricted = get_most_restricted_license(license_list)
            copyright_license_id = None

            if most_restricted:
                # Try normalized lookup
                normalized_key = normalize_license_name(most_restricted)
                copyright_license_id = license_map.get(normalized_key)

                if not copyright_license_id:
                    # Try original name
                    copyright_license_id = license_map.get(most_restricted)

                if not copyright_license_id:
                    logger.warning(
                        f"Line {ln}: license '{most_restricted}' not found in copyright_license table"
                    )

            # Update resource
            resource.license_list = license_list
            resource.license_start_year = license_start_year
            resource.copyright_license_id = copyright_license_id

            db.add(resource)
            updated_count += 1

            logger.info(
                f"Updated: {resource.curie} ({journal_title[:50]}) - "
                f"licenses={license_list}, year={license_start_year}, "
                f"most_restricted={most_restricted}"
            )

        except Exception as e:
            db.rollback()
            error_count += 1
            logger.error(f"Line {ln} ERROR for {journal_title}: {e}")

    # Commit all changes
    db.commit()
    db.close()

    logger.info("=" * 60)
    logger.info(f"Done. Updated: {updated_count}, Not found: {not_found_count}, "
                f"Skipped: {skipped_count}, Errors: {error_count}")


if __name__ == "__main__":
    input_file = sys.argv[1] if len(sys.argv) > 1 else None
    load_data(input_file)
