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
- Rows are deleted through the ORM in batches so referencefile_mod children
  and version records are handled normally.
"""
import argparse
import logging

from fastapi import HTTPException
from sqlalchemy import text

from agr_literature_service.api.crud.referencefile_utils import remove_file_from_s3
from agr_literature_service.api.models import ReferencefileModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BATCH_SIZE = 500


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


def delete_tei_files(execute: bool, limit: int = 0):
    db = create_postgres_session(False)

    tei_rows = db.query(ReferencefileModel).filter(
        ReferencefileModel.file_class == "tei",
        ReferencefileModel.file_extension == "tei",
    ).order_by(ReferencefileModel.referencefile_id)
    if limit:
        tei_rows = tei_rows.limit(limit)
    tei_rows = tei_rows.all()

    embedding_sources = fetch_embedding_source_ids(db)
    keep_md5sums = fetch_md5sums_still_referenced(db)
    # md5sums of skipped TEI rows must survive too
    keep_md5sums.update(
        row.md5sum for row in tei_rows if row.referencefile_id in embedding_sources
    )

    logger.info(f"TEI rows selected: {len(tei_rows)}")
    logger.info(f"  skipped (embedding source): {len(embedding_sources)}")
    if not execute:
        s3_deletable = {
            row.md5sum for row in tei_rows
            if row.referencefile_id not in embedding_sources
            and row.md5sum not in keep_md5sums
        }
        logger.info(f"  DRY RUN: would delete {len(tei_rows) - len(embedding_sources)} "
                    f"rows and {len(s3_deletable)} S3 objects")
        return

    deleted_rows = 0
    deleted_objects = 0
    removed_md5sums = set()
    for i, row in enumerate(tei_rows, start=1):
        if row.referencefile_id in embedding_sources:
            logger.info(f"SKIP embedding source: referencefile_id={row.referencefile_id} "
                        f"({row.display_name}.tei)")
            continue
        if row.md5sum not in keep_md5sums and row.md5sum not in removed_md5sums:
            try:
                remove_file_from_s3(str(row.md5sum))
                deleted_objects += 1
            except HTTPException:
                # Object already gone from S3 — still drop the DB row.
                logger.warning(f"S3 object missing for md5sum={row.md5sum} "
                               f"(referencefile_id={row.referencefile_id})")
            removed_md5sums.add(row.md5sum)
        db.delete(row)
        deleted_rows += 1
        if i % BATCH_SIZE == 0:
            db.commit()
            logger.info(f"  progress: {i}/{len(tei_rows)} rows processed")
    db.commit()
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
