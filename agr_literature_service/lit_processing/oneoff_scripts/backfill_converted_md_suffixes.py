"""Backfill display_name suffixes on existing converted_merged_main md rows.

Historically, nxml2md.py wrote the converted markdown with the source XML's
display_name unchanged. The UI groups converted files with their source by
stripping a known method suffix (_grobid/_docling/_marker/_merged/_nxml/_tei)
from display_name; rows without any suffix are grouped correctly only when
the source display_name happens to equal the converted display_name. Going
forward nxml2md.py writes _nxml/_tei suffixes and the new pdf2md.py writes
_grobid/_docling/_marker/_merged, so existing legacy rows need a one-time
backfill to fit the same convention.

Rules applied by this script:
  - Rows whose display_name already ends in any known suffix are left alone
    (_merged, _nxml, _tei, _grobid, _docling, _marker).
  - For each remaining converted_merged_main row (file_extension='md'),
    append '_nxml' if the reference has any nXML source file_class row,
    else append '_tei'.

This script only updates display_name — it does not touch S3, referencefile_mod
associations, md5sums, or workflow state.

Usage::

    # Preview counts and samples (safe)
    python3 -m agr_literature_service.lit_processing.oneoff_scripts.backfill_converted_md_suffixes --dry-run

    # Apply
    python3 -m agr_literature_service.lit_processing.oneoff_scripts.backfill_converted_md_suffixes --execute

DB connection (host, credentials, database name) is read from environment
variables via the standard agr_literature_service database config; no host
or password is hardcoded here.

SCRUM-5871
"""

import argparse
import logging
import os

from sqlalchemy import create_engine, text

from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


KNOWN_SUFFIXES = ("_merged", "_nxml", "_tei", "_grobid", "_docling", "_marker")

_NOT_LIKE_CLAUSES = " AND ".join(
    f"rf.display_name NOT LIKE '%\\{s}' ESCAPE '\\'" for s in KNOWN_SUFFIXES
)

WHERE_NEEDS_SUFFIX = f"""
  rf.file_class = 'converted_merged_main'
  AND rf.file_extension = 'md'
  AND {_NOT_LIKE_CLAUSES}
"""

HAS_NXML_SUBQUERY = """
  EXISTS (
      SELECT 1 FROM referencefile nxml
      WHERE nxml.reference_id = rf.reference_id
        AND nxml.file_class = 'nXML'
  )
"""


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill _nxml/_tei suffixes on converted_merged_main md rows",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="Report what would change without updating the DB")
    group.add_argument("--execute", action="store_true",
                       help="Apply the rename")
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info(
        "ENV_STATE=%s DB_HOST=%s DB_NAME=%s",
        os.environ.get("ENV_STATE", ""),
        os.environ.get("PSQL_HOST", ""),
        os.environ.get("PSQL_DATABASE", ""),
    )

    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        connect_args={"options": "-c timezone=utc"},
    )

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM referencefile rf WHERE {WHERE_NEEDS_SUFFIX}")
        ).scalar()

        if not total:
            logger.info("No converted_merged_main md rows need a suffix. Nothing to do.")
            return

        would_be_nxml = conn.execute(
            text(
                f"SELECT COUNT(*) FROM referencefile rf "
                f"WHERE {WHERE_NEEDS_SUFFIX} AND {HAS_NXML_SUBQUERY}"
            )
        ).scalar()
        would_be_tei = total - would_be_nxml

        logger.info("=" * 60)
        logger.info("BACKFILL PLAN")
        logger.info("=" * 60)
        logger.info("Rows missing a known suffix: %d", total)
        logger.info("  would receive _nxml      : %d", would_be_nxml)
        logger.info("  would receive _tei       : %d", would_be_tei)
        logger.info("=" * 60)

        for label, clause in (
            ("would_be_nxml", HAS_NXML_SUBQUERY),
            ("would_be_tei", f"NOT {HAS_NXML_SUBQUERY}"),
        ):
            rows = conn.execute(
                text(
                    f"SELECT rf.referencefile_id, rf.display_name, r.curie "
                    f"FROM referencefile rf "
                    f"JOIN reference r ON r.reference_id = rf.reference_id "
                    f"WHERE {WHERE_NEEDS_SUFFIX} AND {clause} LIMIT 5"
                )
            ).fetchall()
            logger.info("Samples (%s):", label)
            for r in rows:
                logger.info(
                    "  rf_id=%d  curie=%s  display=%s",
                    r.referencefile_id, r.curie, r.display_name,
                )

        if args.dry_run:
            return

        # Safety pre-check: flag any target that would collide with an
        # existing (reference_id, display_name, file_extension) row after
        # appending the suffix. This should not happen in practice but a
        # noisy abort is better than a partial UPDATE.
        collisions = conn.execute(
            text(
                f"""
                SELECT rf.referencefile_id, rf.display_name, r.curie,
                       CASE WHEN {HAS_NXML_SUBQUERY} THEN '_nxml' ELSE '_tei' END AS suffix
                FROM referencefile rf
                JOIN reference r ON r.reference_id = rf.reference_id
                WHERE {WHERE_NEEDS_SUFFIX}
                  AND EXISTS (
                      SELECT 1 FROM referencefile other
                      WHERE other.reference_id = rf.reference_id
                        AND other.file_extension = 'md'
                        AND other.referencefile_id <> rf.referencefile_id
                        AND other.display_name = rf.display_name || (
                            CASE WHEN {HAS_NXML_SUBQUERY} THEN '_nxml' ELSE '_tei' END
                        )
                  )
                LIMIT 20
                """
            )
        ).fetchall()
        if collisions:
            logger.error(
                "Aborting: %d row(s) would collide with existing display_name on rename:",
                len(collisions),
            )
            for c in collisions:
                logger.error(
                    "  rf_id=%d  curie=%s  display=%s  suffix=%s",
                    c.referencefile_id, c.curie, c.display_name, c.suffix,
                )
            return

        result = conn.execute(
            text(
                f"""
                UPDATE referencefile rf
                SET display_name = rf.display_name || (
                    CASE WHEN {HAS_NXML_SUBQUERY} THEN '_nxml' ELSE '_tei' END
                )
                WHERE {WHERE_NEEDS_SUFFIX}
                """
            )
        )
        conn.commit()
        logger.info("Updated %d rows", result.rowcount)


if __name__ == "__main__":
    main()
