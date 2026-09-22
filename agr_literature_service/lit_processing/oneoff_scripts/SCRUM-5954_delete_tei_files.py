"""SCRUM-5954: delete raw TEI referencefiles — DB rows AND their S3 objects.

The text-conversion pipeline no longer reads or writes TEI files (PDFX/nXML
produce Markdown directly), so the legacy GROBID TEI artifacts
(file_class='tei', file_extension='tei'; ~297k rows) are dead weight.
TEI-derived Markdown rows (display_name ending in '_tei') are NOT touched —
they are still the current full text for their references.

!!! STOP DEBEZIUM BEFORE RUNNING WITH --execute !!!
Deleting ~297k rows floods the CDC stream. Stop the debezium connector
containers (agr.literature.build.dbz.*) first and restart them afterwards.

Default is a dry run (prints counts, deletes nothing). Pass --execute to
delete, optionally --limit N for a trial slice.

Safety rails:
- An S3 object (keyed by md5sum) is deleted only when NO surviving row —
  non-TEI, or a TEI row excluded below — still references that md5sum.
- TEI rows that are the source of embeddings (embedding_file.
  source_referencefile_id) are skipped and reported instead of deleted.

Deletion is batched for throughput (a per-row ORM pass measured ~1.6s/row):
S3 objects go through delete_objects (1000 keys per request, idempotent on
already-missing keys) and rows through bulk DELETE ... = ANY(:ids). Every FK
referencing referencefile (referencefile_mod, embedding_file) is ON DELETE
CASCADE at the DB level, so children go with their rows. Version-table
records are not written for these deletes — acceptable for a mass cleanup
of dead artifacts.
"""
import argparse
import logging

import boto3
from sqlalchemy import text

from agr_literature_service.api.crud.referencefile_utils import get_s3_folder_from_md5sum
from agr_literature_service.api.models import ReferencefileModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

S3_BATCH_SIZE = 1000   # delete_objects hard limit
DB_BATCH_SIZE = 5000


def fetch_embedding_source_ids(db):
    """TEI rows some embedding was generated from — never delete these blindly."""
    rows = db.execute(text("""
        SELECT DISTINCT rf.referencefile_id
        FROM referencefile rf
        JOIN embedding_file ef ON ef.source_referencefile_id = rf.referencefile_id
        WHERE rf.file_class = 'tei' AND rf.file_extension = 'tei'
    """)).fetchall()
    return {row[0] for row in rows}


def fetch_md5sums_still_referenced(db):
    """md5sums shared with any non-TEI row: their S3 objects must survive."""
    rows = db.execute(text("""
        SELECT DISTINCT md5sum FROM referencefile
        WHERE NOT (file_class = 'tei' AND file_extension = 'tei')
    """)).fetchall()
    return {row[0] for row in rows}


def s3_key_for_md5sum(md5sum: str) -> str:
    return f"{get_s3_folder_from_md5sum(md5sum)}/{md5sum}.gz"


def delete_tei_files(execute: bool, limit: int = 0):
    db = create_postgres_session(False)

    query = db.query(
        ReferencefileModel.referencefile_id,
        ReferencefileModel.md5sum,
    ).filter(
        ReferencefileModel.file_class == "tei",
        ReferencefileModel.file_extension == "tei",
    ).order_by(ReferencefileModel.referencefile_id)
    if limit:
        query = query.limit(limit)
    tei_rows = query.all()

    embedding_sources = fetch_embedding_source_ids(db)
    keep_md5sums = fetch_md5sums_still_referenced(db)
    # md5sums of skipped TEI rows must survive too
    keep_md5sums.update(
        md5sum for rf_id, md5sum in tei_rows if rf_id in embedding_sources
    )

    delete_ids = [rf_id for rf_id, _ in tei_rows if rf_id not in embedding_sources]
    delete_keys = list({
        s3_key_for_md5sum(str(md5sum))
        for rf_id, md5sum in tei_rows
        if rf_id not in embedding_sources and md5sum not in keep_md5sums
    })

    logger.info(f"TEI rows selected: {len(tei_rows)}")
    logger.info(f"  skipped (embedding source): {len(tei_rows) - len(delete_ids)}")
    logger.info(f"  rows to delete: {len(delete_ids)}, S3 objects to delete: {len(delete_keys)}")
    if not execute:
        logger.info("  DRY RUN: nothing deleted")
        return

    # S3 first: a re-run after a crash re-selects the surviving rows and
    # re-issues the (idempotent) object deletes.
    s3_client = boto3.client("s3")
    deleted_objects = 0
    for i in range(0, len(delete_keys), S3_BATCH_SIZE):
        chunk = delete_keys[i:i + S3_BATCH_SIZE]
        resp = s3_client.delete_objects(
            Bucket="agr-literature",
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )
        errors = resp.get("Errors", [])
        deleted_objects += len(chunk) - len(errors)
        for err in errors:
            logger.warning(f"S3 delete failed: {err.get('Key')} — "
                           f"{err.get('Code')}: {err.get('Message')}")
        logger.info(f"  S3 progress: {min(i + S3_BATCH_SIZE, len(delete_keys))}"
                    f"/{len(delete_keys)} objects")

    deleted_rows = 0
    for i in range(0, len(delete_ids), DB_BATCH_SIZE):
        chunk = delete_ids[i:i + DB_BATCH_SIZE]
        result = db.execute(
            text("DELETE FROM referencefile WHERE referencefile_id = ANY(:ids)"),
            {"ids": chunk},
        )
        db.commit()
        deleted_rows += result.rowcount
        logger.info(f"  DB progress: {min(i + DB_BATCH_SIZE, len(delete_ids))}"
                    f"/{len(delete_ids)} rows")

    logger.info(f"Done. Deleted {deleted_rows} referencefile rows and "
                f"{deleted_objects} S3 objects.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Delete raw TEI referencefiles from the DB and S3 (SCRUM-5954). "
                    "STOP DEBEZIUM before running with --execute.")
    parser.add_argument("--execute", action="store_true",
                        help="Actually delete (default is a dry run)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Only process the first N TEI rows (trial run)")
    args = parser.parse_args()
    delete_tei_files(execute=args.execute, limit=args.limit)
