"""
backfill_sgd_curies_for_gaf_interaction.py
==========================================

This oneoff script back-populates SGD mod curies (e.g., SGD:S100002728) for
papers in the SGD corpus that were added via GAF or Interaction loading
but are missing SGD curies.

Target papers:
- mod_id = SGD (4)
- corpus = True
- mod_corpus_sort_source IN ('Gaf', 'Interaction')
- No existing SGD cross reference (curie_prefix = 'SGD', is_obsolete = False)

Usage:
    python backfill_sgd_curies_for_gaf_interaction.py [--dry-run]

Options:
    --dry-run    Report what would be done without making changes
"""
import argparse
import logging.config
from os import path

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import CrossReferenceModel, ModModel
from agr_literature_service.api.user import set_global_user_id

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def get_sgd_mod_id(db_session):
    """Get the mod_id for SGD."""
    sgd_mod = db_session.query(ModModel).filter(
        ModModel.abbreviation == 'SGD'
    ).first()
    if not sgd_mod:
        logger.error("SGD MOD not found in database")
        return None
    return sgd_mod.mod_id


def get_references_missing_sgd_curie(db_session, sgd_mod_id):
    """
    Get reference_ids in SGD corpus (Gaf or Interaction source) that are
    missing SGD curies.

    Returns:
        List of tuples: (reference_id, reference_curie, mod_corpus_sort_source)
    """
    query = text("""
        SELECT mca.reference_id, r.curie, mca.mod_corpus_sort_source
        FROM mod_corpus_association mca
        JOIN reference r ON r.reference_id = mca.reference_id
        WHERE mca.mod_id = :sgd_mod_id
          AND mca.corpus = True
          AND mca.mod_corpus_sort_source IN ('Gaf', 'Interaction')
          AND NOT EXISTS (
              SELECT 1 FROM cross_reference cr
              WHERE cr.reference_id = mca.reference_id
                AND cr.curie_prefix = 'SGD'
                AND cr.is_obsolete = False
          )
        ORDER BY mca.reference_id
    """)

    rows = db_session.execute(query, {"sgd_mod_id": sgd_mod_id}).fetchall()
    return [(row[0], row[1], row[2]) for row in rows]


def create_sgd_curie(db_session, reference_id):
    """Create an SGD curie for the given reference_id."""
    # Get next SGD ID from sequence
    row = db_session.execute(text("SELECT nextval('sgd_id_seq')")).fetchone()
    if not row:
        logger.error(f"Failed to get next SGD ID for reference_id={reference_id}")
        return None

    sgdid_number = row[0]
    new_sgdid = f"SGD:S{sgdid_number}"

    # Create new cross reference
    new_xref = CrossReferenceModel(
        curie=new_sgdid,
        curie_prefix='SGD',
        reference_id=reference_id,
        pages=['reference'],
        is_obsolete=False
    )
    db_session.add(new_xref)
    return new_sgdid


def backfill_sgd_curies(dry_run=False):
    """Main function to back-populate SGD curies."""
    db_session = create_postgres_session(False)

    # Set script user for audit tracking
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_name)

    # Get SGD mod_id
    sgd_mod_id = get_sgd_mod_id(db_session)
    if sgd_mod_id is None:
        db_session.close()
        return

    logger.info(f"SGD mod_id: {sgd_mod_id}")

    # Get references missing SGD curies
    refs_missing_curies = get_references_missing_sgd_curie(db_session, sgd_mod_id)
    logger.info(f"Found {len(refs_missing_curies)} reference(s) missing SGD curies")

    if not refs_missing_curies:
        logger.info("No references need SGD curie backfill")
        db_session.close()
        return

    # Group by source for reporting
    gaf_count = sum(1 for r in refs_missing_curies if r[2] == 'Gaf')
    interaction_count = sum(1 for r in refs_missing_curies if r[2] == 'Interaction')
    logger.info(f"  - Gaf source: {gaf_count}")
    logger.info(f"  - Interaction source: {interaction_count}")

    if dry_run:
        logger.info("DRY RUN - No changes will be made")
        logger.info("References that would be updated:")
        for ref_id, ref_curie, source in refs_missing_curies:
            logger.info(f"  reference_id={ref_id}, curie={ref_curie}, source={source}")
        db_session.close()
        return

    # Create SGD curies
    created_count = 0
    for ref_id, ref_curie, source in refs_missing_curies:
        new_curie = create_sgd_curie(db_session, ref_id)
        if new_curie:
            logger.info(f"Created {new_curie} for reference_id={ref_id} ({ref_curie}), source={source}")
            created_count += 1
        else:
            logger.error(f"Failed to create SGD curie for reference_id={ref_id}")

    if created_count > 0:
        db_session.commit()
        logger.info(f"Successfully created {created_count} SGD curie(s)")
    else:
        logger.info("No SGD curies were created")

    db_session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Back-populate SGD curies for GAF and Interaction papers"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Report what would be done without making changes"
    )
    args = parser.parse_args()

    backfill_sgd_curies(dry_run=args.dry_run)
