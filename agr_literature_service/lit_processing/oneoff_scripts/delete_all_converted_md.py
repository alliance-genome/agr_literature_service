"""Cleanup script to remove all converted_merged_main files from DB and S3.

Use this to clean up after a bad batch run. Removes:
  1. S3 objects (only if no other referencefile shares the same md5sum)
  2. referencefile_mod associations (cascaded by FK)
  3. referencefile records with file_class='converted_merged_main'

Usage::

    # Preview what will be deleted (safe)
    ENV_STATE=build python3 cleanup_converted_md.py --dry-run

    # Actually delete
    ENV_STATE=build python3 cleanup_converted_md.py --execute
"""

import argparse
import logging
import os

import boto3
from botocore.config import Config as BotoConfig
from sqlalchemy import create_engine, text

from agr_literature_service.api.crud.referencefile_utils import (
    get_s3_folder_from_md5sum,
)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Remove all converted_merged_main files from DB and S3",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="Show what would be deleted")
    group.add_argument("--execute", action="store_true",
                       help="Actually delete")
    args = parser.parse_args()

    env_state = os.environ.get("ENV_STATE", "")
    logger.info("ENV_STATE=%s", env_state)

    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        connect_args={"options": "-c timezone=utc"},
    )
    conn = engine.connect()

    # Find all converted_merged_main files
    rows = conn.execute(text(
        "SELECT rf.referencefile_id, rf.md5sum, rf.display_name,"
        "       rf.reference_id, r.curie"
        " FROM referencefile rf"
        " JOIN reference r ON r.reference_id = rf.reference_id"
        " WHERE rf.file_class = 'converted_merged_main'"
    )).fetchall()

    logger.info("Found %d converted_merged_main files to remove.", len(rows))

    if not rows:
        conn.close()
        return

    # Find md5sums shared with non-converted_merged_main files (single query)
    md5sums = {row.md5sum for row in rows}
    shared_rows = conn.execute(text(
        "SELECT DISTINCT md5sum FROM referencefile"
        " WHERE file_class != 'converted_merged_main'"
        "   AND md5sum IN :md5s"
    ), {"md5s": tuple(md5sums)}).fetchall()
    shared_md5sums = {r.md5sum for r in shared_rows}

    s3_to_delete = md5sums - shared_md5sums

    logger.info("=" * 60)
    logger.info("CLEANUP SUMMARY")
    logger.info("=" * 60)
    logger.info("DB records to delete:     %d", len(rows))
    logger.info("S3 objects to delete:     %d", len(s3_to_delete))
    logger.info("S3 objects shared (skip): %d", len(shared_md5sums))
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("")
        logger.info("Sample records (first 20):")
        for row in rows[:20]:
            in_s3 = "DELETE" if row.md5sum in s3_to_delete else "KEEP (shared)"
            logger.info(
                "  %s  referencefile_id=%d  md5sum=%s  S3=%s",
                row.curie, row.referencefile_id, row.md5sum, in_s3,
            )
        if len(rows) > 20:
            logger.info("  ... and %d more", len(rows) - 20)
        conn.close()
        return

    # === EXECUTE ===
    logger.info("Deleting S3 objects...")
    s3_client = boto3.client("s3", config=BotoConfig(max_pool_connections=10))
    s3_deleted = 0
    s3_failed = 0
    for md5 in s3_to_delete:
        folder = get_s3_folder_from_md5sum(md5)
        s3_key = f"{folder}/{md5}.gz"
        try:
            s3_client.delete_object(Bucket="agr-literature", Key=s3_key)
            s3_deleted += 1
        except Exception as exc:
            logger.error("Failed to delete S3 object %s: %s", s3_key, exc)
            s3_failed += 1

    logger.info("S3: deleted=%d, failed=%d", s3_deleted, s3_failed)

    # Delete DB records (referencefile_mod cascade-deletes automatically)
    logger.info("Deleting DB records...")
    try:
        result = conn.execute(text(
            "DELETE FROM referencefile"
            " WHERE file_class = 'converted_merged_main'"
        ))
        conn.commit()
        logger.info("DB: deleted %d referencefile rows.", result.rowcount)
    except Exception as exc:
        conn.rollback()
        logger.error("DB delete failed: %s", exc)

    conn.close()
    logger.info("Cleanup complete.")


if __name__ == "__main__":
    main()
