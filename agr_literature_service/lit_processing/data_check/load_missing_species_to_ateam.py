"""
SCRUM-5724: Check all species in topic_entity_tag are loaded into A-team NCBITaxon.

A WB (or any MOD) curator wants every species that appears on a topic_entity_tag
to exist in the A-team version of NCBI taxon, so the species name shows up on the
tag in the TET.

This script:
  1. Selects the unique list of NCBITaxon species curies from the topic_entity_tag
     table. Two sources are unioned:
       - the species column (same source the report_obsolete_disappeared_species_ids.py
         check uses), and
       - the entity column for species tags, i.e. rows where topic = 'ATP:0000123'
         (species), whose entity value is itself an NCBITaxon curie.
  2. Determines which of those curies are NOT in the A-team NCBITaxon table
     (via map_curies_to_names("species", ...)).
  3. For each missing curie, calls the A-team API get_or_create_species(taxon_id),
     which auto-imports the taxon from NCBI if it does not already exist.

Run with --dry-run to only report the missing species without importing them.
"""
import argparse
import logging
import time
from sqlalchemy import text

from agr_curation_api import AGRCurationAPIClient, AGRAPIError  # type: ignore

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.ateam_db_helpers import map_curies_to_names

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ATP term whose tags carry a species in the entity column.
species_atp = 'ATP:0000123'


def get_distinct_species_curies(db_session):
    """Return the unique list of NCBITaxon: species curies in topic_entity_tag.

    Unions two sources:
      - the species column, and
      - the entity column of species tags (topic = species_atp), whose entity
        value is itself an NCBITaxon curie.
    """
    curies = set()

    species_rows = db_session.execute(
        text("SELECT DISTINCT species FROM topic_entity_tag")
    ).fetchall()
    curies.update(row[0] for row in species_rows
                  if row[0] and row[0].startswith('NCBITaxon:'))

    entity_rows = db_session.execute(
        text("SELECT DISTINCT entity FROM topic_entity_tag WHERE topic = :species_atp"),
        {"species_atp": species_atp}
    ).fetchall()
    curies.update(row[0] for row in entity_rows
                  if row[0] and row[0].startswith('NCBITaxon:'))

    return sorted(curies)


def get_missing_species_curies(all_curies):
    """Return the subset of all_curies that are NOT in the A-team NCBITaxon table."""
    valid_curies = map_curies_to_names("species", all_curies)
    # map_curies_to_names returns {curie: name} only for curies present in A-team;
    # anything not returned (or mapped back to itself with no real name) is missing.
    missing = [c for c in all_curies
               if c not in valid_curies or valid_curies[c] == c]
    return missing


def load_missing_species(missing_curies, dry_run=False, max_attempts=4,
                         retry_delay=15, call_delay=0.34):
    """Call get_or_create_species for each missing curie to import it into A-team.

    The A-Team endpoint imports the taxon from NCBI on demand. The first call for a
    not-yet-imported taxon can return before the import is ready ("no entity in
    response"), and rapid calls can be rate-limited on the NCBI side. So curies that
    fail are retried over several passes, with a delay between passes to let the
    triggered imports settle, and a small delay between calls to stay under NCBI's
    request rate. get_or_create_species is idempotent, so retrying is safe.

    Returns (loaded, failed) lists of (curie, name_or_error) tuples.
    """
    if dry_run:
        logger.info("[dry-run] The following species would be imported into A-team:")
        for curie in missing_curies:
            logger.info(f"[dry-run]   {curie}")
        return [], []

    client = AGRCurationAPIClient()
    loaded = []
    pending = list(missing_curies)
    last_error = {}

    for attempt in range(1, max_attempts + 1):
        if not pending:
            break
        if attempt > 1:
            logger.info(f"Retry pass {attempt - 1}: {len(pending)} taxon(s) remaining; "
                        f"waiting {retry_delay}s for pending NCBI imports to settle...")
            time.sleep(retry_delay)

        still_pending = []
        for curie in pending:
            try:
                term = client.get_or_create_species(curie)
                name = term.name or ""
                loaded.append((curie, name))
                logger.info(f"Loaded {curie} -> {name}")
            except AGRAPIError as e:
                still_pending.append(curie)
                last_error[curie] = str(e)
                msg = f"[attempt {attempt}/{max_attempts}] Failed to load {curie}: {e}"
                if attempt < max_attempts:
                    logger.warning(msg)
                else:
                    logger.error(msg)
            if call_delay:
                time.sleep(call_delay)
        pending = still_pending

    failed = [(curie, last_error.get(curie, "")) for curie in pending]
    return loaded, failed


def check_and_load_missing_species(dry_run=False, max_attempts=4, retry_delay=15):

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

    loaded, failed = load_missing_species(missing_curies, dry_run=dry_run,
                                          max_attempts=max_attempts, retry_delay=retry_delay)

    if not dry_run:
        logger.info(f"Done: {len(loaded)} species loaded, {len(failed)} failed "
                    f"out of {len(missing_curies)} missing.")
        if failed:
            logger.info("Still failing after retries (re-running the script later "
                        "often resolves these, as the NCBI imports complete):")
            for curie, err in failed:
                logger.info(f"  {curie}: {err}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Import species from topic_entity_tag that are missing from "
                    "the A-team NCBITaxon table."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Only report the missing species; do not import them.")
    parser.add_argument("--max-attempts", type=int, default=4,
                        help="Total passes over failing taxa (1 initial + retries). Default: 4.")
    parser.add_argument("--retry-delay", type=int, default=15,
                        help="Seconds to wait between retry passes. Default: 15.")
    args = parser.parse_args()

    check_and_load_missing_species(dry_run=args.dry_run,
                                   max_attempts=args.max_attempts,
                                   retry_delay=args.retry_delay)
