"""
SCRUM-5724: Check all species in topic_entity_tag are loaded into A-team NCBITaxon.

A WB (or any MOD) curator wants every species that appears on a topic_entity_tag
to exist in the A-team version of NCBI taxon, so the species name shows up on the
tag in the TET.

This script:
  1. Selects the unique list of NCBITaxon species curies from the topic_entity_tag
     table (same source the report_obsolete_disappeared_species_ids.py check uses).
  2. Determines which of those curies are NOT in the A-team NCBITaxon table
     (via map_curies_to_names("species", ...)).
  3. For each missing curie, calls the A-team API get_or_create_species(taxon_id),
     which auto-imports the taxon from NCBI if it does not already exist.

Run with --dry-run to only report the missing species without importing them.
"""
import argparse
import logging
from sqlalchemy import text

from agr_curation_api import AGRCurationAPIClient, AGRAPIError  # type: ignore

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.ateam_db_helpers import map_curies_to_names

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_distinct_species_curies(db_session):
    """Return the unique list of NCBITaxon: species curies in topic_entity_tag."""
    distinct_values = db_session.execute(
        text("SELECT DISTINCT species FROM topic_entity_tag")
    ).fetchall()
    return [row[0] for row in distinct_values
            if row[0] and row[0].startswith('NCBITaxon:')]


def get_missing_species_curies(all_curies):
    """Return the subset of all_curies that are NOT in the A-team NCBITaxon table."""
    valid_curies = map_curies_to_names("species", all_curies)
    # map_curies_to_names returns {curie: name} only for curies present in A-team;
    # anything not returned (or mapped back to itself with no real name) is missing.
    missing = [c for c in all_curies
               if c not in valid_curies or valid_curies[c] == c]
    return missing


def load_missing_species(missing_curies, dry_run=False):
    """Call get_or_create_species for each missing curie to import it into A-team.

    Returns (loaded, failed) lists of (curie, name_or_error) tuples.
    """
    loaded = []
    failed = []

    if dry_run:
        logger.info("[dry-run] The following species would be imported into A-team:")
        for curie in missing_curies:
            logger.info(f"[dry-run]   {curie}")
        return loaded, failed

    client = AGRCurationAPIClient()
    for curie in missing_curies:
        try:
            term = client.get_or_create_species(curie)
            name = term.name or ""
            loaded.append((curie, name))
            logger.info(f"Loaded {curie} -> {name}")
        except AGRAPIError as e:
            failed.append((curie, str(e)))
            logger.error(f"Failed to load {curie}: {e}")
    return loaded, failed


def check_and_load_missing_species(dry_run=False):

    db_session = create_postgres_session(False)
    try:
        all_curies = get_distinct_species_curies(db_session)
        logger.info(f"Total {len(all_curies)} unique species are in topic_entity_tag table.")

        missing_curies = get_missing_species_curies(all_curies)
        logger.info(f"{len(missing_curies)} out of {len(all_curies)} NCBITaxon ID(s) "
                    f"are missing from the A-team NCBITaxon table.")
    finally:
        db_session.close()

    if not missing_curies:
        logger.info("No missing species to load. A-team NCBITaxon is up to date.")
        return

    loaded, failed = load_missing_species(missing_curies, dry_run=dry_run)

    if not dry_run:
        logger.info(f"Done: {len(loaded)} species loaded, {len(failed)} failed "
                    f"out of {len(missing_curies)} missing.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Import species from topic_entity_tag that are missing from "
                    "the A-team NCBITaxon table."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Only report the missing species; do not import them.")
    args = parser.parse_args()

    check_and_load_missing_species(dry_run=args.dry_run)
