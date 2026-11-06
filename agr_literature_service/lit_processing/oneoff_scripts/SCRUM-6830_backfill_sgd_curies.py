"""Give every paper in the SGD corpus an active SGD curie (SCRUM-6830).

Papers reached the SGD corpus without an SGD curie (SGD:S1000xxxxx) in two ways:

- The GAF and Interaction loaders moved papers already associated with SGD
  into the corpus with a raw UPDATE, which did not create curies before
  2026-04-27. Those rows kept their earlier sort source (mostly
  Mod_pubmed_search), so backfill_sgd_curies_for_gaf_interaction.py, which
  selected only Gaf and Interaction sort sources, skipped them.
- Papers moved out of the corpus keep their SGD curie as obsolete. Some came
  back in without the curie being made active again.

This script selects every SGD corpus paper with no active SGD curie, whatever
its sort source, and gives it one with ensure_active_sgd_curie: a paper with an
obsolete SGD curie gets that curie back, any other paper a new one from
sgd_id_seq. From SCRUM-6830 on, sgd_curie_listener does the same whenever a
paper is put into the SGD corpus, so no new gaps should appear.

Usage:
    python SCRUM-6830_backfill_sgd_curies.py --dry-run
    python SCRUM-6830_backfill_sgd_curies.py
"""
import argparse
import logging.config
from os import path

from sqlalchemy import text

from agr_literature_service.api.models.sgd_curie_listener import ensure_active_sgd_curie
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

SCRIPT_NAME = "SCRUM-6830_backfill_sgd_curies"
BATCH_COMMIT_SIZE = 200


def get_references_missing_sgd_curie(db_session):
    """(reference_id, reference curie, sort source, latest obsolete SGD curie or None)
    for every SGD corpus paper with no active SGD curie."""
    rows = db_session.execute(text("""
        SELECT mca.reference_id, r.curie, mca.mod_corpus_sort_source,
               (SELECT x.curie FROM cross_reference x
                 WHERE x.reference_id = mca.reference_id AND x.curie_prefix = 'SGD'
                 ORDER BY x.id DESC LIMIT 1) AS old_sgd_curie
        FROM mod_corpus_association mca
        JOIN mod m ON m.id = mca.mod_id AND m.abbreviation = 'SGD'
        JOIN reference r ON r.id = mca.reference_id
        WHERE mca.corpus IS TRUE
          AND NOT EXISTS (
              SELECT 1 FROM cross_reference x
              WHERE x.reference_id = mca.reference_id
                AND x.curie_prefix = 'SGD'
                AND x.obsolete IS FALSE)
        ORDER BY mca.reference_id
    """)).fetchall()
    return [(row[0], row[1], row[2], row[3]) for row in rows]


def backfill_sgd_curies(db_session, dry_run=False):
    """Returns the number of papers given an active SGD curie (would be, on a dry run)."""
    refs = get_references_missing_sgd_curie(db_session)
    reactivate = sum(1 for ref in refs if ref[3])
    logger.info(f"{len(refs)} SGD corpus paper(s) have no active SGD curie: "
                f"{reactivate} with an obsolete SGD curie to reactivate, "
                f"{len(refs) - reactivate} that need a new one")
    if dry_run:
        for reference_id, curie, sort_source, old_sgd_curie in refs:
            action = f"reactivate {old_sgd_curie}" if old_sgd_curie else "mint a new SGD curie"
            logger.info(f"DRY RUN {curie} (reference_id={reference_id}, sort source {sort_source}): {action}")
        return len(refs)

    count = 0
    for reference_id, curie, _sort_source, _old in refs:
        if ensure_active_sgd_curie(db_session, reference_id):
            count += 1
            if count % BATCH_COMMIT_SIZE == 0:
                db_session.commit()
        else:
            logger.warning(f"{curie} (reference_id={reference_id}) got no SGD curie")
    db_session.commit()
    logger.info(f"Gave {count} SGD corpus paper(s) an active SGD curie")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Give every SGD corpus paper an active SGD curie")
    parser.add_argument('--dry-run', action='store_true', help="Report what would change, change nothing")
    args = parser.parse_args()

    session = create_postgres_session(False)
    set_global_user_id(session, SCRIPT_NAME)
    try:
        backfill_sgd_curies(session, dry_run=args.dry_run)
    finally:
        session.close()
