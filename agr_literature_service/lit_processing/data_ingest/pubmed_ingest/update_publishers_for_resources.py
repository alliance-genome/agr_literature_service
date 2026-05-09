"""
update_publishers_for_resources.py
==================================

Monthly script to update resources that have an NLM ID but are missing
publisher or title_synonyms information.

For each resource with an NLM cross-reference but no publisher:
1. Query the NLM catalog using the NLM ID
2. Extract publisher and title_synonyms from the catalog response
3. Update the resource in the database

Usage:
    python update_publishers_for_resources.py [--dry-run] [--limit N]

Options:
    --dry-run   Show what would be updated without making changes
    --limit N   Process at most N resources (default: no limit)
"""

import argparse
import logging
import sys
import time
from os import path
from typing import List, Optional, Tuple

from sqlalchemy import and_
from sqlalchemy.orm import Session

from agr_literature_service.api.models import CrossReferenceModel, ResourceModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup import (
    fetch_nlm_catalog_xml,
    parse_nlm_catalog_xml,
    search_nlm_catalog,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Rate limiting for NLM API (max 3 requests per second without API key)
# Each resource requires 2 API calls (search + fetch), so 0.8s ensures we stay under limit
REQUEST_DELAY_SECONDS = 0.8

# Commit after this many resources to avoid losing work on crash
BATCH_COMMIT_SIZE = 50


def find_resources_missing_publisher_with_nlm(db: Session, limit: Optional[int] = None) -> List[Tuple[int, str, str]]:
    """Find resources that have an NLM cross-reference but no publisher.

    Returns:
        List of tuples: (resource_id, resource_curie, nlm_id)
    """
    query = (
        db.query(
            ResourceModel.resource_id,
            ResourceModel.curie,
            CrossReferenceModel.curie
        )
        .join(
            CrossReferenceModel,
            and_(
                CrossReferenceModel.resource_id == ResourceModel.resource_id,
                CrossReferenceModel.curie_prefix == 'NLM',
                CrossReferenceModel.is_obsolete.is_(False)
            )
        )
        .filter(
            ResourceModel.publisher.is_(None) | (ResourceModel.publisher == '')
        )
        .order_by(ResourceModel.resource_id)
    )

    if limit:
        query = query.limit(limit)

    results = []
    for resource_id, resource_curie, nlm_curie in query.all():
        # Extract NLM ID from curie (e.g., "NLM:0410462" -> "0410462")
        nlm_id = nlm_curie.replace('NLM:', '') if nlm_curie else None
        if nlm_id:
            results.append((resource_id, resource_curie, nlm_id))

    return results


def fetch_nlm_catalog_data(nlm_id: str) -> dict:
    """Fetch publisher and title_synonyms from NLM catalog.

    Returns:
        dict with 'publisher' and 'titleSynonyms' keys (if available)
    """
    try:
        uid = search_nlm_catalog(nlm_id, 'nlmid')
        if not uid:
            logger.debug(f"No NLM catalog UID found for NLM:{nlm_id}")
            return {}

        xml_text = fetch_nlm_catalog_xml(uid)
        parsed = parse_nlm_catalog_xml(xml_text)
        return parsed

    except Exception as e:
        logger.warning(f"Error fetching NLM catalog data for NLM:{nlm_id}: {e}")
        return {}


def update_resource(
    db: Session,
    resource_id: int,
    publisher: Optional[str],
    title_synonyms: Optional[List[str]],
    dry_run: bool = False
) -> bool:
    """Update a resource with publisher and/or title_synonyms.

    Returns:
        True if resource was updated, False otherwise
    """
    resource = db.query(ResourceModel).filter_by(resource_id=resource_id).one_or_none()
    if not resource:
        logger.warning(f"Resource {resource_id} not found")
        return False

    updated = False

    # Update publisher if currently empty and we have data
    if publisher and (not resource.publisher or resource.publisher == ''):
        if dry_run:
            logger.info(f"  [DRY-RUN] Would set publisher: {publisher}")
        else:
            resource.publisher = publisher
        updated = True

    # Update title_synonyms if we have new synonyms
    if title_synonyms:
        existing_synonyms = set(resource.title_synonyms or [])
        new_synonyms = set(title_synonyms)
        synonyms_to_add = new_synonyms - existing_synonyms

        if synonyms_to_add:
            merged_synonyms = list(existing_synonyms | new_synonyms)
            if dry_run:
                logger.info(f"  [DRY-RUN] Would add title_synonyms: {list(synonyms_to_add)}")
            else:
                resource.title_synonyms = merged_synonyms
            updated = True

    # No need for db.add() - resource is already tracked by the session

    return updated


def process_resources(db: Session, dry_run: bool = False, limit: Optional[int] = None) -> dict:
    """Process all resources missing publisher with NLM IDs.

    Returns:
        dict with statistics: {'processed', 'updated', 'skipped', 'errors'}
    """
    stats = {'processed': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
    updates_since_commit = 0

    resources = find_resources_missing_publisher_with_nlm(db, limit)
    total = len(resources)
    logger.info(f"Found {total} resources with NLM ID but no publisher")

    for i, (resource_id, resource_curie, nlm_id) in enumerate(resources, 1):
        logger.info(f"[{i}/{total}] Processing {resource_curie} (NLM:{nlm_id})")
        stats['processed'] += 1

        # Rate limiting
        time.sleep(REQUEST_DELAY_SECONDS)

        try:
            catalog_data = fetch_nlm_catalog_data(nlm_id)
            if not catalog_data:
                logger.info("  No catalog data found")
                stats['skipped'] += 1
                continue

            publisher = catalog_data.get('publisher')
            title_synonyms = catalog_data.get('titleSynonyms')

            if not publisher and not title_synonyms:
                logger.info("  No publisher or title_synonyms in catalog")
                stats['skipped'] += 1
                continue

            updated = update_resource(db, resource_id, publisher, title_synonyms, dry_run)
            if updated:
                logger.info(f"  Updated: publisher={publisher}, synonyms={len(title_synonyms) if title_synonyms else 0}")
                stats['updated'] += 1
                updates_since_commit += 1

                # Batch commit to avoid losing work on crash
                if not dry_run and updates_since_commit >= BATCH_COMMIT_SIZE:
                    db.commit()
                    logger.info(f"  Batch commit ({updates_since_commit} updates)")
                    updates_since_commit = 0
            else:
                stats['skipped'] += 1

        except Exception as e:
            logger.error(f"  Error processing resource: {e}")
            stats['errors'] += 1
            # Rollback to clear failed transaction state
            db.rollback()
            updates_since_commit = 0

    # Final commit for remaining updates
    if not dry_run and updates_since_commit > 0:
        db.commit()
        logger.info(f"Final commit ({updates_since_commit} updates)")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Update resources from NLM catalog (publisher and title_synonyms)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be updated without making changes'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Process at most N resources (default: no limit)'
    )
    args = parser.parse_args()

    logger.info("Starting update_resources_from_nlm_catalog")
    if args.dry_run:
        logger.info("DRY-RUN mode: no changes will be made")

    db_session = create_postgres_session(False)

    try:
        script_name = path.basename(__file__).replace(".py", "")
        set_global_user_id(db_session, script_name)

        stats = process_resources(db_session, dry_run=args.dry_run, limit=args.limit)

        logger.info("=" * 50)
        logger.info("Summary:")
        logger.info(f"  Processed: {stats['processed']}")
        logger.info(f"  Updated:   {stats['updated']}")
        logger.info(f"  Skipped:   {stats['skipped']}")
        logger.info(f"  Errors:    {stats['errors']}")
        logger.info("=" * 50)

    finally:
        db_session.close()

    logger.info("Finished update_resources_from_nlm_catalog")


if __name__ == "__main__":
    main()
