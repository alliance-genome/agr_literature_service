"""
SCRUM-6612_reclassify_htp_supplements.py
========================================

Backfill for the htp_supplement file class (SCRUM-6612). classify_pmc_file
marks large tabular/text supplements at download time, but files loaded
before the rule existed stayed plain ``supplement`` — the referencefile
table stores no size, so they can only be reclassified by measuring the
stored files.

For every ``file_class='supplement'`` row whose extension has an entry in
HTP_SUPPLEMENT_MIN_BYTES (txt/text/tsv/csv at 0.5 MB, xlsx at 1 MB — xls is
pending a curator decision and picks itself up here automatically if it is
ever added to that map), the script streams the S3 object
(``agr-literature/{env}/reference/documents/{m/d/5/s}/{md5sum}.gz``) through
gunzip and counts the UNCOMPRESSED bytes, because the stored object is
gzipped and its Content-Length is the compressed size while the download-time
rule measures the real file. Decompression stops as soon as the count passes
the extension's threshold, so multi-GB datasets cost one partial download.

Safe by default: without ``--update`` the script only reports what it would
reclassify. Pass ``--update`` to write, ``--limit N`` for a trial slice.
ENV_STATE picks the S3 folder (prod/develop/test), exactly as the API does.
"""
import argparse
import logging
import zlib
from os import path

import boto3
from sqlalchemy import text

from agr_literature_service.api.crud.referencefile_utils import get_s3_folder_from_md5sum
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import (
    HTP_SUPPLEMENT_MIN_BYTES,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = "agr-literature"
BATCH_COMMIT_SIZE = 100
READ_CHUNK_BYTES = 64 * 1024


def uncompressed_size_exceeds(body, threshold_bytes):
    """Stream-gunzip an S3 body and decide ``size > threshold_bytes``.

    Returns (exceeds, measured_bytes). Stops reading as soon as the
    decompressed count passes the threshold, so measured_bytes is only the
    full size when the file is at or under the threshold — which is all the
    classification rule needs (strictly-greater, like is_htp_supplement_by_size).
    """
    # 16 + MAX_WBITS = expect a gzip header
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    total = 0
    for chunk in iter(lambda: body.read(READ_CHUNK_BYTES), b""):
        total += len(decompressor.decompress(chunk))
        if total > threshold_bytes:
            return True, total
    total += len(decompressor.flush())
    return total > threshold_bytes, total


def candidate_rows(db_session, limit=None):
    """supplement rows whose extension is governed by the HTP size rule."""
    extensions = sorted(HTP_SUPPLEMENT_MIN_BYTES)
    placeholders = ", ".join(f"'{ext}'" for ext in extensions)
    sql = ("SELECT referencefile_id, md5sum, display_name, file_extension "
           "FROM referencefile "
           "WHERE file_class = 'supplement' "
           f"AND lower(file_extension) IN ({placeholders}) "
           "AND md5sum IS NOT NULL "
           "ORDER BY referencefile_id")
    if limit:
        sql += f" LIMIT {int(limit)}"
    # SQLAlchemy 2.0 rejects plain SQL strings in Session.execute — raw SQL
    # must be declared with text() (review finding).
    return db_session.execute(text(sql)).fetchall()


def reclassify(update=False, limit=None):
    db_session = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_name)
    s3_client = boto3.client('s3')

    rows = candidate_rows(db_session, limit)
    logger.info("%s candidate supplement rows with HTP-governed extensions "
                "(%s)", len(rows), ", ".join(sorted(HTP_SUPPLEMENT_MIN_BYTES)))

    reclassified = under_threshold = errors = 0
    pending_commit = 0
    for referencefile_id, md5sum, display_name, file_extension in rows:
        threshold = HTP_SUPPLEMENT_MIN_BYTES[file_extension.lower()]
        key = f"{get_s3_folder_from_md5sum(md5sum)}/{md5sum}.gz"
        try:
            s3_object = s3_client.get_object(Bucket=BUCKET, Key=key)
            exceeds, measured = uncompressed_size_exceeds(s3_object['Body'], threshold)
        except Exception as e:  # noqa: BLE001 - one bad object must not stop the run
            errors += 1
            logger.info("ERROR referencefile_id=%s %s.%s s3=%s: %s",
                        referencefile_id, display_name, file_extension, key, e)
            continue

        if not exceeds:
            under_threshold += 1
            continue

        reclassified += 1
        logger.info("%s referencefile_id=%s %s.%s uncompressed>%s bytes "
                    "(measured %s) -> htp_supplement",
                    "UPDATE" if update else "WOULD UPDATE",
                    referencefile_id, display_name, file_extension,
                    threshold, measured)
        if update:
            db_session.execute(
                text("UPDATE referencefile SET file_class = 'htp_supplement' "
                     "WHERE referencefile_id = :rid AND file_class = 'supplement'"),
                {"rid": referencefile_id})
            pending_commit += 1
            if pending_commit >= BATCH_COMMIT_SIZE:
                db_session.commit()
                pending_commit = 0

    if update and pending_commit:
        db_session.commit()

    logger.info("done: %s reclassified%s, %s under threshold, %s errors",
                reclassified, "" if update else " (dry run)",
                under_threshold, errors)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Reclassify already-loaded large tabular/text supplements "
                    "as htp_supplement (SCRUM-6612 backfill). Dry run unless "
                    "--update is passed.")
    parser.add_argument("--update", action="store_true",
                        help="write the reclassifications (default: report only)")
    parser.add_argument("--limit", type=int, default=None,
                        help="only examine the first N candidate rows")
    args = parser.parse_args()
    reclassify(update=args.update, limit=args.limit)
