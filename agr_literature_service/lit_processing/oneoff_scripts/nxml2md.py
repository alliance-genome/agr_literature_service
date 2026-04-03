"""Oneoff script to batch-convert NXML files to ABC Markdown.

Finds all references that:
  - have an NXML file (file_class='nXML', file_publication_status='final')
  - have been converted to text (workflow_tag ATP:0000163)
  - do NOT already have a converted_merged_main file

For each, downloads the NXML from S3, converts to Markdown via
agr_abc_document_parsers, and uploads the result as a new referencefile
with file_class='converted_merged_main'.

Usage::

    ENV_STATE=prod python3 nxml2md.py [--dry-run] [--limit N] [--workers 8]

Requires AWS credentials and database access configured via environment
variables or .env file.

SCRUM-5870
"""

import argparse
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

from fastapi import UploadFile
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from agr_abc_document_parsers import convert_xml_to_markdown
from agr_cognito_py import ModAccess
from agr_literature_service.api.crud.referencefile_crud import (
    download_file,
    file_upload_single,
)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ATP:0000163 = "file converted to text"
FILE_CONVERTED_TO_TEXT_ATP = "ATP:0000163"

QUERY = """
    SELECT DISTINCT ON (rf.reference_id)
           rf.referencefile_id,
           rf.md5sum,
           rf.reference_id,
           rf.display_name,
           r.curie AS reference_curie
    FROM referencefile rf
    JOIN reference r ON r.reference_id = rf.reference_id
    JOIN workflow_tag wt ON wt.reference_id = rf.reference_id
    WHERE rf.file_class = 'nXML'
      AND rf.file_publication_status = 'final'
      AND wt.workflow_tag_id = :wft_atp
      AND rf.reference_id NOT IN (
          SELECT reference_id FROM referencefile
          WHERE file_class = 'converted_merged_main'
      )
    ORDER BY rf.reference_id, rf.referencefile_id DESC
"""


def parse_args():
    parser = argparse.ArgumentParser(
        description="Batch convert NXML files to Markdown",
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
    return parser.parse_args()


def convert_one(session_factory, row):
    """Convert a single NXML file to Markdown. Runs in a worker thread."""
    ref_curie = row.reference_curie
    referencefile_id = row.referencefile_id
    display_name = row.display_name

    db = session_factory()
    try:
        xml_content = download_file(
            db=db,
            referencefile_id=referencefile_id,
            mod_access=ModAccess.ALL_ACCESS,
            use_in_api=False,
        )
        if not xml_content:
            raise ValueError("Empty content returned from S3")

        markdown = convert_xml_to_markdown(xml_content, "jats")

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

        md_bytes = markdown.encode("utf-8")
        file_obj = UploadFile(
            file=BytesIO(md_bytes),
            filename=f"{display_name}.md",
        )

        file_upload_single(db=db, metadata=metadata, file=file_obj)
        return ref_curie, referencefile_id, None
    except Exception as exc:
        db.rollback()
        return ref_curie, referencefile_id, str(exc)
    finally:
        db.close()


# Thread-safe counter for progress logging
_lock = threading.Lock()
_progress = {"done": 0}


def main():
    args = parse_args()

    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        connect_args={"options": "-c timezone=utc"},
        pool_size=max(5, args.workers + 2),
        max_overflow=args.workers,
    )
    Session = sessionmaker(bind=engine, autoflush=True)
    db = Session()

    logger.info("Querying for NXML files to convert...")
    query = QUERY
    if args.limit:
        query += f" LIMIT {args.limit}"

    rows = db.execute(
        text(query),
        {"wft_atp": FILE_CONVERTED_TO_TEXT_ATP},
    ).fetchall()

    logger.info("Found %d NXML files to convert.", len(rows))

    if args.dry_run:
        # Gather stats
        total_nxml = db.execute(text(
            "SELECT COUNT(*) FROM referencefile"
            " WHERE file_class = 'nXML' AND file_publication_status = 'final'"
        )).scalar()
        already_converted = db.execute(text(
            "SELECT COUNT(DISTINCT rf.reference_id) FROM referencefile rf"
            " WHERE rf.file_class = 'nXML' AND rf.file_publication_status = 'final'"
            " AND rf.reference_id IN ("
            "   SELECT reference_id FROM referencefile"
            "   WHERE file_class = 'converted_merged_main')"
        )).scalar()
        no_text_wft = total_nxml - len(rows) - already_converted

        logger.info("=" * 60)
        logger.info("DRY RUN SUMMARY")
        logger.info("=" * 60)
        logger.info("Total NXML files in database:       %d", total_nxml)
        logger.info("Already have converted_merged_main: %d", already_converted)
        logger.info("Missing text conversion WFT:        %d", no_text_wft)
        logger.info("Eligible for conversion:            %d", len(rows))
        logger.info("=" * 60)

        # Investigate references with multiple NXML files
        from collections import defaultdict
        refs_by_id = defaultdict(list)
        for row in rows:
            refs_by_id[row.reference_id].append(row)
        multi_nxml = {rid: rlist for rid, rlist in refs_by_id.items()
                      if len(rlist) > 1}

        if multi_nxml:
            logger.info("")
            logger.info("References with multiple NXML files: %d", len(multi_nxml))
            # Show details for all of them (or cap at 50)
            shown = 0
            for rid, rlist in sorted(multi_nxml.items()):
                if shown >= 50:
                    logger.info("  ... and %d more references with duplicates",
                                len(multi_nxml) - shown)
                    break
                logger.info("  %s (reference_id=%d) — %d NXML files:",
                            rlist[0].reference_curie, rid, len(rlist))
                for row in rlist:
                    logger.info(
                        "    referencefile_id=%d  display_name=%-30s  md5sum=%s",
                        row.referencefile_id, row.display_name, row.md5sum,
                    )
                shown += 1

        if rows:
            logger.info("")
            logger.info("Sample files to convert (first 20):")
            for row in rows[:20]:
                logger.info(
                    "  %s  referencefile_id=%d  md5sum=%s  display_name=%s",
                    row.reference_curie, row.referencefile_id,
                    row.md5sum, row.display_name,
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

    logger.info("Starting conversion with %d worker(s)...", args.workers)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(convert_one, Session, row): row
            for row in rows
        }
        for future in as_completed(futures):
            ref_curie, referencefile_id, error = future.result()
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

    logger.info(
        "Done. converted=%d, failed=%d, total=%d",
        converted, failed, total,
    )

    if errors:
        logger.info("Errors:")
        for ref_curie, reffile_id, err in errors:
            logger.info("  %s (referencefile_id=%d): %s", ref_curie, reffile_id, err)


if __name__ == "__main__":
    main()
