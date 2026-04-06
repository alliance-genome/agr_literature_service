"""Oneoff script to batch-convert NXML or TEI files to ABC Markdown.

Finds all references that:
  - have a source file (NXML or TEI depending on --source)
  - have been converted to text (workflow_tag ATP:0000163)
  - do NOT already have a converted_merged_main file

For NXML (default): converts publisher-provided JATS XML — highest quality.
For TEI (--source tei): converts GROBID TEI from PDF extraction — only for
references that have no NXML file available.

Usage::

    # Convert NXML files (default, highest quality)
    python3 nxml2md.py --workers 100

    # Convert TEI files (only refs without NXML)
    python3 nxml2md.py --source tei --workers 100

    # Dry run for either source
    python3 nxml2md.py --source tei --dry-run

Requires AWS credentials and database access configured via environment
variables or .env file.

SCRUM-5870, SCRUM-5872
"""

import argparse
import gzip
import hashlib
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

import boto3
from botocore.config import Config as BotoConfig
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from agr_abc_document_parsers import convert_xml_to_markdown
from agr_literature_service.api.crud.referencefile_crud import create_metadata
from agr_literature_service.api.crud.referencefile_utils import (
    get_s3_folder_from_md5sum,
)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.models import ReferencefileModel, ReferenceModel
from agr_literature_service.api.s3.upload import upload_file_to_bucket
from agr_literature_service.api.schemas.referencefile_schemas import (
    ReferencefileSchemaPost,
)

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ATP:0000163 = "file converted to text"
FILE_CONVERTED_TO_TEXT_ATP = "ATP:0000163"

# Source configurations: file_class, source_format for the parser
SOURCE_CONFIG = {
    "nxml": {"file_class": "nXML", "parser_format": "jats"},
    "tei": {"file_class": "tei", "parser_format": "tei"},
}

NXML_QUERY = """
    SELECT DISTINCT ON (rf.reference_id)
           rf.referencefile_id,
           rf.md5sum,
           rf.reference_id,
           rf.display_name,
           rf.file_class,
           r.curie AS reference_curie
    FROM referencefile rf
    JOIN reference r ON r.reference_id = rf.reference_id
    WHERE rf.file_class = 'nXML'
      AND rf.file_publication_status = 'final'
      AND EXISTS (
          SELECT 1 FROM workflow_tag wt
          WHERE wt.reference_id = rf.reference_id
            AND wt.workflow_tag_id = :wft_atp
      )
      AND NOT EXISTS (
          SELECT 1 FROM referencefile cmm
          WHERE cmm.reference_id = rf.reference_id
            AND cmm.file_class = 'converted_merged_main'
      )
    ORDER BY rf.reference_id, rf.referencefile_id DESC
"""

TEI_QUERY = """
    SELECT DISTINCT ON (rf.reference_id)
           rf.referencefile_id,
           rf.md5sum,
           rf.reference_id,
           rf.display_name,
           rf.file_class,
           r.curie AS reference_curie
    FROM referencefile rf
    JOIN reference r ON r.reference_id = rf.reference_id
    WHERE rf.file_class = 'tei'
      AND rf.file_publication_status = 'final'
      AND EXISTS (
          SELECT 1 FROM workflow_tag wt
          WHERE wt.reference_id = rf.reference_id
            AND wt.workflow_tag_id = :wft_atp
      )
      AND NOT EXISTS (
          SELECT 1 FROM referencefile cmm
          WHERE cmm.reference_id = rf.reference_id
            AND cmm.file_class = 'converted_merged_main'
      )
      AND NOT EXISTS (
          SELECT 1 FROM referencefile nxml
          WHERE nxml.reference_id = rf.reference_id
            AND nxml.file_class = 'nXML'
            AND nxml.file_publication_status = 'final'
      )
    ORDER BY rf.reference_id, rf.referencefile_id DESC
"""

BOTH_QUERY = """
    SELECT DISTINCT ON (rf.reference_id)
           rf.referencefile_id,
           rf.md5sum,
           rf.reference_id,
           rf.display_name,
           rf.file_class,
           r.curie AS reference_curie
    FROM referencefile rf
    JOIN reference r ON r.reference_id = rf.reference_id
    WHERE rf.file_class IN ('nXML', 'tei')
      AND rf.file_publication_status = 'final'
      AND EXISTS (
          SELECT 1 FROM workflow_tag wt
          WHERE wt.reference_id = rf.reference_id
            AND wt.workflow_tag_id = :wft_atp
      )
      AND NOT EXISTS (
          SELECT 1 FROM referencefile cmm
          WHERE cmm.reference_id = rf.reference_id
            AND cmm.file_class = 'converted_merged_main'
      )
    ORDER BY rf.reference_id,
             CASE rf.file_class WHEN 'nXML' THEN 0 ELSE 1 END,
             rf.referencefile_id DESC
"""

QUERIES = {"nxml": NXML_QUERY, "tei": TEI_QUERY, "both": BOTH_QUERY}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Batch convert NXML or TEI files to Markdown",
    )
    parser.add_argument(
        "--source",
        choices=["nxml", "tei", "both"],
        default="nxml",
        help="Source format: nxml, tei, or both (nxml preferred) (default: nxml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Query and report counts without converting",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of files to convert",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Per-file timeout in seconds (default: 120)",
    )
    return parser.parse_args()


def download_from_s3(s3_client, md5sum):
    """Download and decompress a file from S3 entirely in memory."""
    folder = get_s3_folder_from_md5sum(md5sum)
    s3_key = f"{folder}/{md5sum}.gz"
    response = s3_client.get_object(
        Bucket="agr-literature",
        Key=s3_key,
    )
    compressed = response["Body"].read()
    try:
        return gzip.decompress(compressed)
    except (gzip.BadGzipFile, OSError):
        return compressed


def upload_md_to_s3(s3_client, md_bytes, md5sum):
    """Gzip and upload markdown bytes to S3 entirely in memory."""
    folder = get_s3_folder_from_md5sum(md5sum)
    compressed = gzip.compress(md_bytes)
    env_state = os.environ.get("ENV_STATE", "")
    extra_args = (
        {"StorageClass": "GLACIER_IR"}
        if env_state == "prod"
        else {"StorageClass": "STANDARD"}
    )
    upload_file_to_bucket(
        s3_client=s3_client,
        file_obj=BytesIO(compressed),
        bucket="agr-literature",
        folder=folder,
        object_name=f"{md5sum}.gz",
        ExtraArgs=extra_args,
    )


FORMAT_FROM_CLASS = {"nXML": "jats", "tei": "tei"}


def convert_one(session_factory, s3_client, row, parser_format=None):
    """Convert a single XML file to Markdown. Runs in a worker thread."""
    ref_curie = row.reference_curie
    src_md5sum = row.md5sum
    display_name = row.display_name

    # In "both" mode, derive format from the row's file_class
    if parser_format is None:
        parser_format = FORMAT_FROM_CLASS[row.file_class]

    db = session_factory()
    try:
        # Download source file from S3 in memory
        xml_content = download_from_s3(s3_client, src_md5sum)
        if not xml_content:
            raise ValueError("Empty content returned from S3")

        # Convert to markdown
        markdown = convert_xml_to_markdown(xml_content, parser_format)
        md_bytes = markdown.encode("utf-8")

        # Compute md5sum of markdown
        md5sum = hashlib.md5(md_bytes).hexdigest()

        # Check if this exact file already exists for this reference
        existing = db.query(ReferencefileModel).filter(
            ReferencefileModel.md5sum == md5sum,
            ReferencefileModel.reference.has(
                ReferenceModel.curie == ref_curie
            ),
        ).one_or_none()

        if existing:
            return ref_curie, row.referencefile_id, None

        # Upload to S3 in memory
        # Only upload if no other referencefile has this md5sum
        md5sum_count = db.query(ReferencefileModel).filter(
            ReferencefileModel.md5sum == md5sum
        ).count()
        if md5sum_count == 0:
            upload_md_to_s3(s3_client, md_bytes, md5sum)

        # Create DB record
        metadata = {
            "reference_curie": ref_curie,
            "display_name": display_name,
            "file_class": "converted_merged_main",
            "file_publication_status": "final",
            "file_extension": "md",
            "pdf_type": None,
            "is_annotation": False,
            "mod_abbreviation": None,
        }
        create_request = ReferencefileSchemaPost(md5sum=md5sum, **metadata)
        create_metadata(db, create_request)

        return ref_curie, row.referencefile_id, None
    except Exception as exc:
        db.rollback()
        return ref_curie, row.referencefile_id, str(exc)
    finally:
        db.close()


# Thread-safe counter for progress logging
_lock = threading.Lock()
_progress = {"done": 0}


def main():
    args = parse_args()
    source = args.source
    config = SOURCE_CONFIG.get(source)

    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        connect_args={"options": "-c timezone=utc"},
        pool_size=max(5, args.workers + 2),
        max_overflow=args.workers,
    )
    Session = sessionmaker(bind=engine, autoflush=True)
    db = Session()

    if config:
        logger.info(
            "Source: %s (file_class=%s, format=%s)",
            source, config["file_class"], config["parser_format"],
        )
    else:
        logger.info("Source: both (nXML preferred, TEI fallback)")
    logger.info("Querying for files to convert...")
    query = QUERIES[source]
    if args.limit:
        query += f" LIMIT {args.limit}"

    rows = db.execute(
        text(query),
        {"wft_atp": FILE_CONVERTED_TO_TEXT_ATP},
    ).fetchall()

    logger.info("Found %d files to convert.", len(rows))

    if args.dry_run:
        if source == "both":
            from collections import Counter
            source_counts = Counter(row.file_class for row in rows)
            nxml_count = source_counts.get("nXML", 0)
            tei_count = source_counts.get("tei", 0)
        else:
            nxml_count = len(rows) if source == "nxml" else 0
            tei_count = len(rows) if source == "tei" else 0

        already_converted = db.execute(text(
            "SELECT COUNT(DISTINCT reference_id) FROM referencefile"
            " WHERE file_class = 'converted_merged_main'"
        )).scalar()

        logger.info("=" * 60)
        logger.info("DRY RUN SUMMARY (%s)", source.upper())
        logger.info("=" * 60)
        if source == "both":
            logger.info("  From NXML:                        %d", nxml_count)
            logger.info("  From TEI (no NXML available):     %d", tei_count)
        logger.info("Already have converted_merged_main: %d", already_converted)
        logger.info("Eligible for conversion:            %d", len(rows))
        logger.info("=" * 60)

        if rows:
            logger.info("")
            logger.info("Sample files to convert (first 20):")
            for row in rows[:20]:
                fc = getattr(row, "file_class", source)
                logger.info(
                    "  %s  referencefile_id=%d  source=%s  display_name=%s",
                    row.reference_curie, row.referencefile_id,
                    fc, row.display_name,
                )
            if len(rows) > 20:
                logger.info("  ... and %d more", len(rows) - 20)
        db.close()
        return

    db.close()

    converted = 0
    failed = 0
    errors = []
    total = len(rows)
    _progress["done"] = 0

    s3_client = boto3.client(
        "s3",
        config=BotoConfig(max_pool_connections=max(10, args.workers)),
    )

    logger.info("Starting %s conversion with %d worker(s)...",
                source.upper(), args.workers)
    t_start = time.monotonic()

    # In "both" mode, parser_format is derived per-row from file_class
    fmt = config["parser_format"] if config else None

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(convert_one, Session, s3_client, row, fmt): row
            for row in rows
        }
        for future in as_completed(futures):
            row = futures[future]
            try:
                ref_curie, referencefile_id, error = future.result(
                    timeout=args.timeout,
                )
            except TimeoutError:
                ref_curie = row.reference_curie
                referencefile_id = row.referencefile_id
                error = f"Timed out after {args.timeout}s"
            with _lock:
                _progress["done"] += 1
                done = _progress["done"]
            if error:
                failed += 1
                errors.append((ref_curie, referencefile_id, error))
                logger.error(
                    "[%d/%d] FAILED %s (referencefile_id=%d): %s",
                    done, total, ref_curie, referencefile_id, error,
                )
            else:
                converted += 1
                if done % 100 == 0 or done == total:
                    logger.info(
                        "[%d/%d] converted=%d failed=%d",
                        done, total, converted, failed,
                    )

    elapsed = time.monotonic() - t_start
    mins, secs = divmod(elapsed, 60)
    hours, mins = divmod(mins, 60)
    rate = converted / elapsed if elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info("CONVERSION COMPLETE (%s)", source.upper())
    logger.info("=" * 60)
    logger.info("Converted:    %d", converted)
    logger.info("Failed:       %d", failed)
    logger.info("Total:        %d", total)
    logger.info("Workers:      %d", args.workers)
    logger.info("Elapsed:      %dh %dm %ds", hours, mins, secs)
    logger.info("Rate:         %.1f files/min", rate * 60)
    logger.info("=" * 60)

    if errors:
        logger.info("Errors:")
        for ref_curie, reffile_id, err in errors:
            logger.info("  %s (referencefile_id=%d): %s", ref_curie, reffile_id, err)


if __name__ == "__main__":
    main()
