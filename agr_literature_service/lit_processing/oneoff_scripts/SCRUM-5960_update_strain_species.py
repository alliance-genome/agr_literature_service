#!/usr/bin/env python3
"""
SCRUM-5960: Update species column for strain topic_entity_tag rows.

This script updates the species column for strain extraction results to use
the correct taxon ID from the curation database instead of the generic one.

Filters:
- topic = 'ATP:0000027' (strain)
- entity_type = 'ATP:0000027' (strain)
- topic_entity_tag_source_id = 174 (ML extractor source)

Usage:
    # Dry run (no changes made):
    python SCRUM-5960_update_strain_species.py --dry-run

    # Actually update the database:
    python SCRUM-5960_update_strain_species.py

    # Limit to specific MOD:
    python SCRUM-5960_update_strain_species.py --mod WB
"""

import argparse
import logging
from collections import defaultdict
from typing import Optional

from sqlalchemy.orm import Session

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import TopicEntityTagModel

# Import the A-team API client
from agr_curation_api import AGRCurationAPIClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

STRAIN_TOPIC = 'ATP:0000027'
STRAIN_ENTITY_TYPE = 'ATP:0000027'
SOURCE_ID = 174  # ML extractor source ID

PAGE_LIMIT = 1000


def get_strain_curie_to_taxon_mapping(
    mod_abbreviation: str,
    api_client: AGRCurationAPIClient
) -> dict[str, str]:
    """
    Fetch all strains for a MOD and build a mapping from strain CURIE to taxon ID.

    Args:
        mod_abbreviation: The MOD prefix (e.g., 'WB', 'MGI').
        api_client: Shared AGRCurationAPIClient instance.
    """
    logger.info(f"Fetching strain taxon mappings for {mod_abbreviation}...")

    curie_to_taxon: dict[str, str] = {}
    current_page = 0

    while True:
        entities = api_client.get_agms(
            data_provider=mod_abbreviation,
            subtype='strain',
            limit=PAGE_LIMIT,
            page=current_page
        )

        if not entities:
            break

        for entity in entities:
            curie = getattr(entity, 'primaryExternalId', None)
            if not curie:
                continue

            # taxon is a dict with 'curie' key
            taxon = getattr(entity, 'taxon', None)
            if taxon:
                taxon_curie = (
                    taxon.get('curie') if isinstance(taxon, dict)
                    else getattr(taxon, 'curie', None)
                )
                if taxon_curie:
                    curie_to_taxon[curie] = taxon_curie

        current_page += 1

    logger.info(f"Loaded {len(curie_to_taxon)} strain taxon mappings for {mod_abbreviation}")
    return curie_to_taxon


def update_strain_species(dry_run: bool = True, mod_filter: Optional[str] = None):
    """
    Update species column for strain topic_entity_tag rows.

    Args:
        dry_run: If True, only report what would be changed without making changes.
        mod_filter: If provided, only process strains from this MOD (e.g., 'WB').
    """
    db_session: Session = create_postgres_session(False)
    # Create API client once for reuse across all MODs (singleton pattern)
    api_client = AGRCurationAPIClient()

    try:
        # Query all strain topic_entity_tags from the ML extractor
        query = db_session.query(TopicEntityTagModel).filter(
            TopicEntityTagModel.topic == STRAIN_TOPIC,
            TopicEntityTagModel.entity_type == STRAIN_ENTITY_TYPE,
            TopicEntityTagModel.topic_entity_tag_source_id == SOURCE_ID,
            TopicEntityTagModel.entity.isnot(None)
        )

        tags = query.all()
        logger.info(f"Found {len(tags)} strain topic_entity_tag rows to process")

        if not tags:
            logger.info("No tags to update.")
            return

        # Group tags by MOD prefix to batch fetch taxon mappings
        tags_by_mod: dict[str, list] = defaultdict(list)
        for tag in tags:
            if tag.entity:
                # Extract MOD prefix from entity CURIE (e.g., 'WB:WBStrain00001234' -> 'WB')
                parts = tag.entity.split(':')
                if len(parts) >= 2:
                    mod = parts[0]
                    if mod_filter and mod != mod_filter:
                        continue
                    tags_by_mod[mod].append(tag)

        # Process each MOD
        total_updated = 0
        total_skipped = 0
        total_not_found = 0

        for mod, mod_tags in tags_by_mod.items():
            logger.info(f"Processing {len(mod_tags)} tags for MOD: {mod}")

            # Fetch taxon mappings for this MOD
            try:
                curie_to_taxon = get_strain_curie_to_taxon_mapping(mod, api_client)
            except Exception as e:
                logger.error(f"Failed to fetch taxon mappings for {mod}: {e}")
                continue

            for tag in mod_tags:
                entity_curie = tag.entity
                current_species = tag.species
                correct_taxon = curie_to_taxon.get(entity_curie)

                if not correct_taxon:
                    logger.warning(
                        f"No taxon found for entity {entity_curie} "
                        f"(reference: {tag.reference_id}, tag_id: {tag.topic_entity_tag_id}, "
                        f"current_species: {current_species})"
                    )
                    total_not_found += 1
                    continue

                if current_species == correct_taxon:
                    logger.debug(f"Species already correct for {entity_curie}: {current_species}")
                    total_skipped += 1
                    continue

                # Update needed
                logger.info(f"Updating {entity_curie}: {current_species} -> {correct_taxon}")

                if not dry_run:
                    tag.species = correct_taxon
                    # No need to call db_session.add(tag) - tag is already tracked
                    # by the session since it was loaded via query.all()

                total_updated += 1

        if not dry_run:
            db_session.commit()
            logger.info(f"Committed {total_updated} updates to database")
        else:
            logger.info(f"DRY RUN: Would update {total_updated} rows")

        logger.info("Summary:")
        logger.info(f"  Updated: {total_updated}")
        logger.info(f"  Skipped (already correct): {total_skipped}")
        logger.info(f"  Not found in curation DB: {total_not_found}")

    except Exception as e:
        logger.error(f"Error during update: {e}")
        db_session.rollback()
        raise
    finally:
        db_session.close()


def main():
    parser = argparse.ArgumentParser(
        description='Update species column for strain topic_entity_tag rows'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only report what would be changed without making changes'
    )
    parser.add_argument(
        '--mod',
        type=str,
        default=None,
        help='Only process strains from this MOD (e.g., WB)'
    )

    args = parser.parse_args()

    if args.dry_run:
        logger.info("Running in DRY RUN mode - no changes will be made")
    else:
        logger.info("Running in UPDATE mode - changes will be committed to database")

    update_strain_species(dry_run=args.dry_run, mod_filter=args.mod)


if __name__ == '__main__':
    main()
