"""
Update image permissions with journal lists based on 2026 publisher permission grants.

This script updates:
1. image_permission table - permission_text with journal lists, name cleanup
2. resource_image_permission table - links each journal (resource) to its permission
3. Replaces MOD references with 'Alliance' in all permission_text values
   (WormBase, FlyBase, Xenbase, SGD, WB, ZFIN, MGI, RGD, XB, FB -> Alliance)

Matching is done by `name` column (not image_permission_id) so it can run
against dev, stage, and prod databases which have different IDs.

Usage:
    # Dry run (default)
    python update_image_permissions_2026_journals.py

    # Apply changes
    python update_image_permissions_2026_journals.py --apply

    # Use specific env file
    python update_image_permissions_2026_journals.py --env-file .env_prod --apply
"""

import argparse
import logging
import re
from datetime import datetime
from pathlib import Path
from os import environ
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# MOD names and abbreviations to replace with 'Alliance'
# Order matters: longer names first to avoid partial replacements
MOD_REPLACEMENTS: List[Tuple[str, str]] = [
    # Full names (longer patterns first)
    ("WormBase", "Alliance"),
    ("FlyBase", "Alliance"),
    ("Xenbase", "Alliance"),
    # Abbreviations - only replace when they appear as standalone words
    # to avoid replacing parts of other words
]

# MOD abbreviations that should only be replaced as whole words
MOD_ABBREVIATIONS = ["SGD", "WB", "ZFIN", "MGI", "RGD", "XB", "FB"]


def replace_mod_names_with_alliance(text_value: str) -> str:
    """Replace MOD names and abbreviations with 'Alliance' in the given text.

    Args:
        text_value: The text to process

    Returns:
        Text with MOD names replaced by 'Alliance'
    """
    if not text_value:
        return text_value

    result = text_value

    # Replace full MOD names (simple string replacement)
    for old_name, new_name in MOD_REPLACEMENTS:
        result = result.replace(old_name, new_name)

    # Replace MOD abbreviations only as whole words (word boundaries)
    for abbrev in MOD_ABBREVIATIONS:
        # Use word boundary regex to avoid replacing parts of words
        # e.g., don't replace "WB" in "SWIBERG"
        pattern = r'\b' + re.escape(abbrev) + r'\b'
        result = re.sub(pattern, "Alliance", result)

    return result


# Permission updates based on 2026 grants
# Each entry contains:
#   - match_name: pattern to find existing image_permission record
#   - new_name: updated name for the permission
#   - permission_text: detailed text with journal list
#   - can_display_images: boolean flag
#   - journals: list of journal name patterns to link (for resource_image_permission)
PERMISSION_UPDATES: List[Dict[str, Any]] = [
    # Cold Spring Harbor Laboratory Press
    {
        "match_name": "Cold Spring Harbor Laboratory Press%",
        "new_name": "Cold Spring Harbor Laboratory Press - Blanket Permission",
        "permission_text": (
            "Permission granted for past articles and future articles. "
            "Note: if any images were taken from an original source, permission must be "
            "secured from that rightsholder.\n\n"
            "Journals covered:\n"
            "- Genome Research\n"
            "- RNA\n"
            "- Genes & Development\n"
            "- Learning & Memory\n"
            "- Life Science Alliance\n"
            "- Molecular Case Studies\n"
            "- Perspectives in Biology\n"
            "- Perspectives in Medicine\n"
            "- Protocols\n\n"
            "Permission granted by Carol Brown (Permissions Coordinator) on December 16, 2025."
        ),
        "can_display_images": True,
        "journals": [
            "Genome Res",
            "Genome Research",
            "RNA",  # Exact match for the journal "RNA"
            "Genes Dev",
            "Genes & Development",
            "Learn Mem",
            "Learning & Memory",
            "Learning and Memory",
            "Life Sci Alliance",
            "Life Science Alliance",
            "Mol Case Stud",
            "Molecular Case Studies",
            "Cold Spring Harb Perspect Biol",
            "Cold Spring Harb Perspect Med",
            "Cold Spring Harb Protoc",
        ],
    },
    # Cold Spring Harbor Lab Press (alternate name) - same journals
    {
        "match_name": "Cold Spring Harbor Lab Press%",
        "new_name": "Cold Spring Harbor Lab Press - Blanket Permission",
        "permission_text": (
            "Permission granted for past articles and future articles. "
            "Note: if any images were taken from an original source, permission must be "
            "secured from that rightsholder.\n\n"
            "Journals covered:\n"
            "- Genome Research\n"
            "- RNA\n"
            "- Genes & Development\n"
            "- Learning & Memory\n"
            "- Life Science Alliance\n"
            "- Molecular Case Studies\n"
            "- Perspectives in Biology\n"
            "- Perspectives in Medicine\n"
            "- Protocols\n\n"
            "Permission granted by Carol Brown (Permissions Coordinator) on December 16, 2025."
        ),
        "can_display_images": True,
        "journals": [
            "Genome Res",
            "Genome Research",
            "RNA",  # Exact match for the journal "RNA"
            "Genes Dev",
            "Genes & Development",
            "Learn Mem",
            "Learning & Memory",
            "Learning and Memory",
            "Life Sci Alliance",
            "Life Science Alliance",
            "Mol Case Stud",
            "Molecular Case Studies",
            "Cold Spring Harb Perspect Biol",
            "Cold Spring Harb Perspect Med",
            "Cold Spring Harb Protoc",
        ],
    },
    # Charles Univ Prague (Folia Biologica)
    {
        "match_name": "Charles Univ Prague%",
        "new_name": "Charles Univ Prague (Folia Biologica) - CC BY | Open Access",
        "permission_text": (
            "Open Access journal published by the First Faculty of Medicine, "
            "Charles University in Prague. Published under CC-BY license.\n\n"
            "Journals covered:\n"
            "- Folia Biologica\n\n"
            "Permission confirmed by Jan Zivny (Chief-Executive-Editor)."
        ),
        "can_display_images": True,
        "journals": [
            "Folia Biol (Praha)",
            "Folia Biologica (Praha)",
            "Folia Biologica",
        ],
    },
    # Genetic Society of America (GSA) - G3
    {
        "match_name": "Genetic Society of America%",
        "new_name": "Genetic Society of America (GSA) - Blanket Permission",
        "permission_text": (
            "Permission granted specifically for G3, which is open access.\n\n"
            "Journals covered:\n"
            "- G3: Genes|Genomes|Genetics\n\n"
            "Permission granted by Tracey Depellegrin on November 22, 2025."
        ),
        "can_display_images": True,
        "journals": [
            "G3 (Bethesda)",
            "G3 (Bethesda, Md.)",
        ],
    },
    # Portland Press LTD
    {
        "match_name": "Portland Press%",
        "new_name": "Portland Press LTD - Blanket Permission",
        "permission_text": (
            "Standing permission to use any Portland Press articles with full citation "
            "and attribution.\n\n"
            "Journals covered:\n"
            "- Biochemical Journal\n"
            "- Clinical Science\n"
            "- Bioscience Reports\n"
            "- Biochemical Society Transactions\n"
            "- Essays in Biochemistry\n"
            "- Emerging Topics in Life Sciences\n\n"
            "Permission granted by Paul Wearmouth (Sales Manager) on December 10, 2025."
        ),
        "can_display_images": True,
        "journals": [
            "Biochem J",
            "Biochemical Journal",
            "Clin Sci",
            "Clin Sci (Lond)",
            "Clinical Science",
            "Biosci Rep",
            "Bioscience Reports",
            "Biochem Soc Trans",
            "Biochemical Society Transactions",
            "Essays Biochem",
            "Essays in Biochemistry",
            "Emerg Top Life Sci",
            "Emerging Topics in Life Sciences",
        ],
    },
    # The Rockefeller University Press - Contract -> Non-commercial Educational Use
    {
        "match_name": "The Rockefeller University Press - Contract",
        "new_name": "The Rockefeller University Press - Non-commercial Educational Use",
        "permission_text": (
            "All content freely available for non-commercial, educational use with citation.\n\n"
            "Journals covered:\n"
            "- The Journal of Cell Biology\n"
            "- The Journal of Experimental Medicine\n"
            "- Journal of General Physiology\n"
            "- Journal of Human Immunity\n"
            "- Life Science Alliance\n\n"
            "Permission confirmed by Laura (RU Press Permissions) on November 24, 2025."
        ),
        "can_display_images": True,
        "journals": [
            "J Cell Biol",
            "Journal of Cell Biology",
            "J Exp Med",
            "Journal of Experimental Medicine",
            "J Gen Physiol",
            "Journal of General Physiology",
            "Life Sci Alliance",
            "Life Science Alliance",
        ],
    },
    # The Rockefeller University Press - Open Access
    {
        "match_name": "The Rockefeller University Press - Open Access",
        "new_name": "The Rockefeller University Press - Open Access",
        "permission_text": (
            "All content freely available for non-commercial, educational use with citation.\n\n"
            "Journals covered:\n"
            "- The Journal of Cell Biology\n"
            "- The Journal of Experimental Medicine\n"
            "- Journal of General Physiology\n"
            "- Journal of Human Immunity\n"
            "- Life Science Alliance\n\n"
            "Permission confirmed by Laura (RU Press Permissions) on November 24, 2025."
        ),
        "can_display_images": True,
        "journals": [
            "J Cell Biol",
            "Journal of Cell Biology",
            "J Exp Med",
            "Journal of Experimental Medicine",
            "J Gen Physiol",
            "Journal of General Physiology",
            "Life Sci Alliance",
            "Life Science Alliance",
        ],
    },
    # Society for Neuroscience - CC BY 4.0
    {
        "match_name": "Society for Neuroscience - CC BY 4.0%",
        "new_name": "Society for Neuroscience - CC BY 4.0 | Blanket Permission",
        "permission_text": (
            "Blanket permission granted for previously published material. "
            "Permission for material where SfN is not the rightsholder must be obtained "
            "separately.\n\n"
            "Journals covered:\n"
            "- eNeuro\n"
            "- The Journal of Neuroscience\n\n"
            "Licensing by publication date:\n"
            "- Prior to 2010 (Vols. 1-29): Include copyright statement "
            "'Copyright [year] Society for Neuroscience'\n"
            "- 2010-2014 (Vols. 30-34): CC-BY-NC-SA 3.0 "
            "(SfN does not enforce noncommercial clause)\n"
            "- 2014-2025 (Vols. 35-45): CC-BY 4.0\n"
            "- After Dec 31, 2025 (Open Access): CC-BY 4.0\n"
            "- After Dec 31, 2025 (Non-Open Access): Individual requests to "
            "jnpermissions@sfn.org\n\n"
            "Permission granted by Adam Buck (Subscriptions & Licensing Specialist)."
        ),
        "can_display_images": True,
        "journals": [
            "eNeuro",
            "J Neurosci",
            "Journal of Neuroscience",
        ],
    },
    # Society for Neuroscience - Open Access
    {
        "match_name": "Society for Neuroscience - Open Access",
        "new_name": "Society for Neuroscience - Open Access | Blanket Permission",
        "permission_text": (
            "Blanket permission granted for previously published material. "
            "Permission for material where SfN is not the rightsholder must be obtained "
            "separately.\n\n"
            "Journals covered:\n"
            "- eNeuro\n"
            "- The Journal of Neuroscience\n\n"
            "Licensing by publication date:\n"
            "- Prior to 2010 (Vols. 1-29): Include copyright statement "
            "'Copyright [year] Society for Neuroscience'\n"
            "- 2010-2014 (Vols. 30-34): CC-BY-NC-SA 3.0 "
            "(SfN does not enforce noncommercial clause)\n"
            "- 2014-2025 (Vols. 35-45): CC-BY 4.0\n"
            "- After Dec 31, 2025 (Open Access): CC-BY 4.0\n"
            "- After Dec 31, 2025 (Non-Open Access): Individual requests to "
            "jnpermissions@sfn.org\n\n"
            "Permission granted by Adam Buck (Subscriptions & Licensing Specialist)."
        ),
        "can_display_images": True,
        "journals": [
            "eNeuro",
            "J Neurosci",
            "Journal of Neuroscience",
        ],
    },
    # The Company of Biologists - Contract (update to Blanket Permission)
    {
        "match_name": "The Company of Biologists - Contract",
        "new_name": "The Company of Biologists - Blanket Permission",
        "permission_text": (
            "Ongoing permission granted to the Alliance as a whole for images going forward "
            "and for previous images used on MODs.\n\n"
            "Journals covered:\n"
            "- Biology Open\n"
            "- Development (Cambridge, England)\n"
            "- Disease Models & Mechanisms\n"
            "- Journal of Cell Science\n"
            "- The Journal of Experimental Biology\n\n"
            "Permission granted by Alice Baker (Sales & Customer Service Administrator) "
            "on December 18, 2025."
        ),
        "can_display_images": True,
        "journals": [
            "Biol Open",
            "Biology Open",
            "Development",
            "Dis Model Mech",
            "Disease Models & Mechanisms",
            "Disease Models and Mechanisms",
            "J Cell Sci",
            "Journal of Cell Science",
            "J Exp Biol",
            "Journal of Experimental Biology",
        ],
    },
    # The Company of Biologists - Open Access
    {
        "match_name": "The Company of Biologists - Open Access",
        "new_name": "The Company of Biologists - Open Access",
        "permission_text": (
            "Ongoing permission granted to the Alliance as a whole for images going forward "
            "and for previous images used on MODs.\n\n"
            "Journals covered:\n"
            "- Biology Open\n"
            "- Development (Cambridge, England)\n"
            "- Disease Models & Mechanisms\n"
            "- Journal of Cell Science\n"
            "- The Journal of Experimental Biology\n\n"
            "Permission granted by Alice Baker (Sales & Customer Service Administrator) "
            "on December 18, 2025."
        ),
        "can_display_images": True,
        "journals": [
            "Biol Open",
            "Biology Open",
            "Development",
            "Dis Model Mech",
            "Disease Models & Mechanisms",
            "Disease Models and Mechanisms",
            "J Cell Sci",
            "Journal of Cell Science",
            "J Exp Biol",
            "Journal of Experimental Biology",
        ],
    },
    # The Company of Biologists - Permission Granted (subset) - keep as subset
    {
        "match_name": "The Company of Biologists - Permission Granted (subset)",
        "new_name": "The Company of Biologists - Permission Granted (subset)",
        "permission_text": (
            "Permission granted for a subset of articles.\n\n"
            "Journals covered:\n"
            "- Biology Open\n"
            "- Development (Cambridge, England)\n"
            "- Disease Models & Mechanisms\n"
            "- Journal of Cell Science\n"
            "- The Journal of Experimental Biology\n\n"
            "Note: Full blanket permission now available under separate permission entry."
        ),
        "can_display_images": False,  # Keep as subset
        "journals": [],  # Don't update links for subset
    },
    # WormBook
    {
        "match_name": "WormBook%",
        "new_name": "WormBook - Blanket Permission",
        "permission_text": (
            "Permission granted verbally by Paul Sternberg for reproduction of figures.\n\n"
            "Journals covered:\n"
            "- WormBook"
        ),
        "can_display_images": True,
        "journals": [
            "WormBook",
        ],
    },
]


def load_env_file(env_file: Optional[Path]) -> None:
    """Load environment variables from file."""
    if env_file is None:
        return
    if not env_file.exists():
        raise FileNotFoundError(f"Environment file does not exist: {env_file}")

    for raw_line in env_file.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        # Handle 'export VAR=value' format
        if line.startswith("export "):
            line = line[7:]
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        environ.setdefault(key, value)


def find_permission_by_name(session: Session, match_pattern: str) -> Optional[Dict]:
    """Find an image_permission record by name pattern."""
    result = session.execute(
        text("""
            SELECT image_permission_id, name, permission_text, can_display_images
            FROM image_permission
            WHERE name LIKE :pattern
            ORDER BY image_permission_id
            LIMIT 1
        """),
        {"pattern": match_pattern}
    ).fetchone()

    if result:
        return {
            "image_permission_id": result[0],
            "name": result[1],
            "permission_text": result[2],
            "can_display_images": result[3],
        }
    return None


def find_resources_by_journal_pattern(
    session: Session, pattern: str
) -> List[Dict[str, Any]]:
    """Find resource records matching a journal name (exact match)."""
    results = session.execute(
        text("""
            SELECT resource_id, curie, title, title_abbreviation
            FROM resource
            WHERE title_abbreviation = :pattern
               OR title = :pattern
            ORDER BY resource_id
        """),
        {"pattern": pattern}
    ).fetchall()

    return [
        {
            "resource_id": r[0],
            "curie": r[1],
            "title": r[2],
            "title_abbreviation": r[3],
        }
        for r in results
    ]


def find_existing_resource_permission_link(
    session: Session,
    resource_id: int,
    image_permission_id: int
) -> Optional[int]:
    """Check if a resource_image_permission link already exists."""
    result = session.execute(
        text("""
            SELECT resource_image_permission_id
            FROM resource_image_permission
            WHERE resource_id = :resource_id
              AND image_permission_id = :image_permission_id
            LIMIT 1
        """),
        {"resource_id": resource_id, "image_permission_id": image_permission_id}
    ).fetchone()

    return result[0] if result else None


def create_resource_permission_link(
    session: Session,
    resource_id: int,
    image_permission_id: int
) -> int:
    """Create a new resource_image_permission link."""
    result = session.execute(
        text("""
            INSERT INTO resource_image_permission
                (resource_id, image_permission_id, date_created)
            VALUES (:resource_id, :image_permission_id, :date_created)
            RETURNING resource_image_permission_id
        """),
        {
            "resource_id": resource_id,
            "image_permission_id": image_permission_id,
            "date_created": datetime.utcnow(),
        }
    )
    return result.fetchone()[0]


def update_permission(
    session: Session,
    current_name: str,
    new_name: str,
    permission_text: str,
    can_display_images: bool
) -> None:
    """Update an image_permission record."""
    session.execute(
        text("""
            UPDATE image_permission
            SET name = :new_name,
                permission_text = :permission_text,
                can_display_images = :can_display_images,
                date_updated = :date_updated
            WHERE name = :current_name
        """),
        {
            "current_name": current_name,
            "new_name": new_name,
            "permission_text": permission_text,
            "can_display_images": can_display_images,
            "date_updated": datetime.utcnow(),
        }
    )


def update_mod_references_in_permission_text(
    session: Session,
    apply: bool
) -> Dict[str, int]:
    """Update all permission_text values to replace MOD names with 'Alliance'.

    Args:
        session: Database session
        apply: Whether to apply changes

    Returns:
        Dictionary with update statistics
    """
    stats = {
        "mod_refs_found": 0,
        "mod_refs_updated": 0,
        "mod_refs_unchanged": 0,
        "mod_refs_errors": 0,
    }

    # Get all image_permission records with non-null permission_text
    results = session.execute(
        text("""
            SELECT image_permission_id, name, permission_text
            FROM image_permission
            WHERE permission_text IS NOT NULL
            ORDER BY image_permission_id
        """)
    ).fetchall()

    for row in results:
        perm_id = row[0]
        name = row[1]
        original_text = row[2]

        if not original_text:
            continue

        # Replace MOD names with Alliance
        updated_text = replace_mod_names_with_alliance(original_text)

        if updated_text != original_text:
            stats["mod_refs_found"] += 1

            if apply:
                try:
                    session.execute(
                        text("""
                            UPDATE image_permission
                            SET permission_text = :permission_text,
                                date_updated = :date_updated
                            WHERE image_permission_id = :perm_id
                        """),
                        {
                            "permission_text": updated_text,
                            "date_updated": datetime.utcnow(),
                            "perm_id": perm_id,
                        }
                    )
                    logger.info(
                        f"MOD->ALLIANCE: ID {perm_id} '{name}' - "
                        f"replaced MOD references with 'Alliance'"
                    )
                    stats["mod_refs_updated"] += 1
                except Exception as e:
                    logger.error(f"ERROR updating MOD refs in ID {perm_id}: {e}")
                    stats["mod_refs_errors"] += 1
            else:
                logger.info(
                    f"WOULD UPDATE MOD->ALLIANCE: ID {perm_id} '{name}' - "
                    f"would replace MOD references with 'Alliance'"
                )
                stats["mod_refs_updated"] += 1
        else:
            stats["mod_refs_unchanged"] += 1

    return stats


def process_resource_links(
    session: Session,
    image_permission_id: int,
    journal_patterns: List[str],
    apply: bool,
    stats: Dict[str, int]
) -> None:
    """Process resource_image_permission links for a permission."""
    for pattern in journal_patterns:
        resources = find_resources_by_journal_pattern(session, pattern)

        for resource in resources:
            resource_id = resource["resource_id"]
            resource_name = resource["title_abbreviation"] or resource["title"]

            # Check if link already exists
            existing_link = find_existing_resource_permission_link(
                session, resource_id, image_permission_id
            )

            if existing_link:
                logger.debug(
                    f"  LINK EXISTS: {resource_name} -> permission {image_permission_id}"
                )
                stats["links_unchanged"] += 1
                continue

            if apply:
                try:
                    link_id = create_resource_permission_link(
                        session, resource_id, image_permission_id
                    )
                    logger.info(
                        f"  LINK CREATED: {resource_name} (resource {resource_id}) "
                        f"-> permission {image_permission_id} (link {link_id})"
                    )
                    stats["links_created"] += 1
                except Exception as e:
                    logger.error(f"  ERROR creating link for {resource_name}: {e}")
                    stats["link_errors"] += 1
            else:
                logger.info(
                    f"  WOULD LINK: {resource_name} (resource {resource_id}) "
                    f"-> permission {image_permission_id}"
                )
                stats["links_created"] += 1


def process_updates(session: Session, apply: bool) -> Dict[str, int]:
    """Process all permission updates."""
    stats = {
        "found": 0,
        "not_found": 0,
        "updated": 0,
        "unchanged": 0,
        "errors": 0,
        "links_created": 0,
        "links_unchanged": 0,
        "link_errors": 0,
    }

    for update in PERMISSION_UPDATES:
        match_name = update["match_name"]
        new_name = update["new_name"]
        permission_text = update["permission_text"]
        can_display_images = update["can_display_images"]
        journal_patterns = update.get("journals", [])

        # Find the current record
        current = find_permission_by_name(session, match_name)

        if current is None:
            logger.warning(f"NOT FOUND: No record matching '{match_name}'")
            stats["not_found"] += 1
            continue

        stats["found"] += 1
        current_id = current["image_permission_id"]
        current_name = current["name"]

        # Check if update is needed
        name_changed = current_name != new_name
        text_changed = current["permission_text"] != permission_text
        display_changed = current["can_display_images"] != can_display_images

        if not (name_changed or text_changed or display_changed):
            logger.info(f"UNCHANGED: ID {current_id} '{current_name}'")
            stats["unchanged"] += 1
        else:
            # Log what will change
            changes = []
            if name_changed:
                changes.append(f"name: '{current_name}' -> '{new_name}'")
            if text_changed:
                changes.append("permission_text: [updated with journal list]")
            if display_changed:
                changes.append(
                    f"can_display_images: {current['can_display_images']} -> "
                    f"{can_display_images}"
                )

            if apply:
                try:
                    update_permission(
                        session,
                        current_name,
                        new_name,
                        permission_text,
                        can_display_images
                    )
                    logger.info(f"UPDATED: ID {current_id} - {', '.join(changes)}")
                    stats["updated"] += 1
                except Exception as e:
                    logger.error(f"ERROR updating ID {current_id}: {e}")
                    stats["errors"] += 1
                    continue
            else:
                logger.info(f"WOULD UPDATE: ID {current_id} - {', '.join(changes)}")
                stats["updated"] += 1

        # Process resource links (if journals are specified)
        if journal_patterns:
            logger.info(f"  Processing resource links for permission {current_id}...")
            process_resource_links(
                session, current_id, journal_patterns, apply, stats
            )

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update image permissions with journal lists from 2026 grants"
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Environment file to load (e.g., .env_cc, .env_prod)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes. Default is dry-run.",
    )
    args = parser.parse_args()

    env_file = Path(args.env_file) if args.env_file else None
    load_env_file(env_file)

    session = create_postgres_session(False)

    try:
        if not args.apply:
            logger.info("=" * 60)
            logger.info("DRY RUN MODE - No changes will be made")
            logger.info("Pass --apply to commit changes")
            logger.info("=" * 60)

        stats = process_updates(session, args.apply)

        # Update MOD references to Alliance in all permission_text values
        logger.info("\n" + "=" * 60)
        logger.info("UPDATING MOD REFERENCES TO 'Alliance' IN permission_text")
        logger.info("=" * 60)
        mod_stats = update_mod_references_in_permission_text(session, args.apply)

        if args.apply:
            session.commit()
            logger.info("\nChanges committed successfully!")
        else:
            session.rollback()

        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY - IMAGE PERMISSIONS")
        logger.info("=" * 60)
        logger.info(f"Records found: {stats['found']}")
        logger.info(f"Records not found: {stats['not_found']}")
        perm_action = "updated" if args.apply else "would update"
        logger.info(f"Records {perm_action}: {stats['updated']}")
        logger.info(f"Records unchanged: {stats['unchanged']}")
        logger.info(f"Errors: {stats['errors']}")

        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY - RESOURCE LINKS")
        logger.info("=" * 60)
        link_action = "created" if args.apply else "would create"
        logger.info(f"Links {link_action}: {stats['links_created']}")
        logger.info(f"Links already exist: {stats['links_unchanged']}")
        logger.info(f"Link errors: {stats['link_errors']}")

        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY - MOD->ALLIANCE REPLACEMENTS")
        logger.info("=" * 60)
        mod_action = "updated" if args.apply else "would update"
        logger.info(f"Records with MOD references found: {mod_stats['mod_refs_found']}")
        logger.info(f"Records {mod_action}: {mod_stats['mod_refs_updated']}")
        logger.info(f"Records without MOD references: {mod_stats['mod_refs_unchanged']}")
        logger.info(f"Errors: {mod_stats['mod_refs_errors']}")

        # Final summary for easy reading
        logger.info("\n" + "=" * 60)
        mode = "APPLIED" if args.apply else "DRY RUN"
        logger.info(f"FINAL SUMMARY ({mode})")
        logger.info("=" * 60)
        perm_msg = "updated" if args.apply else "would be updated"
        link_msg = "created" if args.apply else "would be created"
        mod_msg = "updated" if args.apply else "would be updated"
        logger.info(f"  - {stats['updated']} image_permission records {perm_msg}")
        logger.info(f"  - {stats['links_created']} resource links {link_msg}")
        logger.info(f"  - {stats['links_unchanged']} resource links already exist")
        logger.info(
            f"  - {mod_stats['mod_refs_updated']} records {mod_msg} "
            f"(MOD->Alliance in permission_text)"
        )

    except Exception as e:
        session.rollback()
        logger.error(f"Error: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
