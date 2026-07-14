"""
One-off cleanup for SCRUM-6281 / SCRUM-6095: re-classify mislabeled PMC thumbnails.

Historically every PMC image was stored with ``file_class = 'figure'`` because
the classifier only recognized a thumbnail when the file name contained
"thumb". In reality PMC ships each figure as a large image plus a small,
same-named thumbnail that carries no "thumb" token, so ~half of the ``figure``
rows are actually thumbnails.

This script scans ``figure``-class ``gif``/``jpg``/``jpeg`` rows, reads each
object's size from S3, and re-classifies it as ``thumbnail`` when either:

  * it falls below the absolute size threshold for its type
    (gif < 25 KB, jpg/jpeg < 15 KB), or
  * (SCRUM-6095) it is a gif above that threshold that has a same-named
    jpg/jpeg companion in the same reference which is larger -- i.e. the gif
    is the smaller half of a gif/jpg pair. Some publishers ship "large"
    thumbnails (~70 KB gif alongside a ~340 KB jpg) that the absolute cutoff
    alone misses.

The thresholds live in file_processing_utils.THUMBNAIL_MAX_SIZE_BYTES, the same
values the ingest-time classifier uses, so past and future data agree.

By default the script is a dry run: it reports what it would change (with a
size histogram) but commits nothing. Pass --apply to perform the updates.

Usage:
    python reclassify_pmc_thumbnails.py                 # dry run
    python reclassify_pmc_thumbnails.py --apply         # perform updates
    python reclassify_pmc_thumbnails.py --limit 5000    # cap rows (testing)
"""
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor
from os import environ, path

import boto3
from botocore.exceptions import ClientError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.referencefile_utils import get_s3_folder_from_md5sum
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import (
    is_thumbnail_by_size,
    is_paired_thumbnail,
)
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = environ.get("REFFILE_S3_BUCKET", "agr-literature")
BATCH_SIZE = 500
S3_WORKERS = 24

SELECT_CANDIDATES = text(
    "SELECT rf.referencefile_id, rf.display_name, rf.file_extension, rf.md5sum, "
    "  (SELECT j.md5sum FROM referencefile j "
    "     WHERE j.reference_id = rf.reference_id "
    "       AND j.display_name = rf.display_name "
    "       AND lower(j.file_extension) IN ('jpg', 'jpeg') "
    "     ORDER BY j.file_extension "
    "     LIMIT 1) AS sibling_jpg_md5sum "
    "FROM referencefile rf "
    "WHERE rf.file_class = 'figure' "
    "AND lower(rf.file_extension) IN ('gif', 'jpg', 'jpeg') "
    "AND rf.referencefile_id > :last_id "
    "ORDER BY rf.referencefile_id "
    "LIMIT :limit"
)

UPDATE_CLASS = text(
    "UPDATE referencefile "
    "SET file_class = 'thumbnail' "
    "WHERE referencefile_id = :referencefile_id"
)


def get_s3_size(s3_client, md5sum):
    """Return the size in bytes of the stored (gzipped) object, or None."""
    folder = get_s3_folder_from_md5sum(md5sum)
    key = f"{folder}/{md5sum}.gz"
    try:
        return s3_client.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    except ClientError:
        return None


def classify_reason(file_extension, size, sibling_jpg_md5sum, size_by_md5):
    """Return a short reason string if this row should become a thumbnail, else None."""
    if is_thumbnail_by_size(file_extension, size):
        return "size"
    if file_extension.lower() == 'gif' and sibling_jpg_md5sum:
        sibling_size = size_by_md5.get(sibling_jpg_md5sum)
        if is_paired_thumbnail('gif', size, {'jpg': sibling_size}):
            return f"paired(jpg={sibling_size})"
    return None


def _bucket_label(size):
    if size < 10000:
        return "<10KB"
    if size < 15000:
        return "10-15KB"
    if size < 20000:
        return "15-20KB"
    if size < 25000:
        return "20-25KB"
    return ">=25KB"


def reclassify_thumbnails(apply_changes: bool, row_limit: int) -> None:

    db_session = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_name)

    s3_client = boto3.client("s3")

    mode = "APPLY" if apply_changes else "DRY-RUN"
    logger.info(f"Running in {mode} mode (bucket={BUCKET})")

    last_id = 0
    scanned = 0
    updated = 0
    missing = 0
    histogram: dict = {}

    while True:
        if row_limit and scanned >= row_limit:
            break

        batch = min(BATCH_SIZE, row_limit - scanned) if row_limit else BATCH_SIZE
        try:
            rows = db_session.execute(
                SELECT_CANDIDATES, {"last_id": last_id, "limit": batch}
            ).fetchall()
        except SQLAlchemyError as e:
            logger.error(f"Error executing SELECT query: {e}")
            break

        if not rows:
            break

        # Fetch S3 sizes for the batch concurrently. Include the same-named jpg
        # companion of each gif so the paired-thumbnail rule (SCRUM-6095) can
        # compare their sizes.
        md5_set = set()
        for row in rows:
            md5_set.add(row[3])
            if row[4] and row[2].lower() == 'gif':
                md5_set.add(row[4])
        md5_list = list(md5_set)
        with ThreadPoolExecutor(max_workers=S3_WORKERS) as pool:
            fetched = list(pool.map(lambda m: get_s3_size(s3_client, m), md5_list))
        size_by_md5 = dict(zip(md5_list, fetched))

        to_update = []
        for row in rows:
            referencefile_id, display_name, file_extension, md5sum, sibling_jpg_md5sum = row
            last_id = referencefile_id
            scanned += 1

            size = size_by_md5.get(md5sum)
            if size is None:
                missing += 1
                logger.info(f"MISSING in S3: referencefile_id={referencefile_id} "
                            f"display_name='{display_name}'")
                continue

            reason = classify_reason(file_extension, size, sibling_jpg_md5sum, size_by_md5)
            if reason is None:
                continue

            histogram[_bucket_label(size)] = histogram.get(_bucket_label(size), 0) + 1
            to_update.append(referencefile_id)
            logger.info(f"figure -> thumbnail [{reason}]: referencefile_id={referencefile_id} "
                        f"display_name='{display_name}.{file_extension}' size={size}")

        if to_update:
            updated += len(to_update)
            if apply_changes:
                try:
                    for referencefile_id in to_update:
                        db_session.execute(
                            UPDATE_CLASS, {"referencefile_id": referencefile_id}
                        )
                    db_session.commit()
                except SQLAlchemyError as e:
                    logger.error(f"Error committing batch: {e}")
                    db_session.rollback()
                    updated -= len(to_update)

    logger.info("---- size histogram of reclassified rows ----")
    for label in ["<10KB", "10-15KB", "15-20KB", "20-25KB", ">=25KB"]:
        if label in histogram:
            logger.info(f"  {label}: {histogram[label]}")

    logger.info(
        f"DONE! mode={mode} scanned={scanned} missing_in_s3={missing} "
        f"{'updated' if apply_changes else 'to_update'}={updated}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reclassify size-qualifying PMC 'figure' rows as 'thumbnail'."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit the updates. Without this flag the script only reports.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Cap the number of rows scanned (0 = no cap). Useful for testing.",
    )
    args = parser.parse_args()
    reclassify_thumbnails(args.apply, args.limit)
