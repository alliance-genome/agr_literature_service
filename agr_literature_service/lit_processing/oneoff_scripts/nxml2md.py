"""Oneoff script to batch-convert NXML files to ABC Markdown.

Finds all references that:
  - have an NXML file (file_class='nXML', file_publication_status='final')
  - have been converted to text (workflow_tag ATP:0000163)
  - do NOT already have a converted_merged_main file

For each, downloads the NXML from S3, converts to Markdown via
agr_abc_document_parsers, and uploads the result as a new referencefile
with file_class='converted_merged_main'.

Usage::

    ENV_STATE=prod python3 nxml2md.py [--dry-run] [--limit N]

Requires AWS credentials and database access configured via environment
variables or .env file.

SCRUM-5870
"""

import argparse
import logging
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
    SELECT rf.referencefile_id,
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
    GROUP BY rf.referencefile_id, r.curie
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
    return parser.parse_args()


def main():
    args = parse_args()

    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        connect_args={"options": "-c timezone=utc"},
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
        for row in rows:
            logger.info(
                "Would convert: %s (referencefile_id=%d, md5sum=%s)",
                row.reference_curie, row.referencefile_id, row.md5sum,
            )
        return

    converted = 0
    failed = 0
    errors = []

    for i, row in enumerate(rows, 1):
        ref_curie = row.reference_curie
        referencefile_id = row.referencefile_id
        display_name = row.display_name

        logger.info(
            "[%d/%d] Converting %s (referencefile_id=%d)...",
            i, len(rows), ref_curie, referencefile_id,
        )

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
            converted += 1
            logger.info("  OK — converted %s", ref_curie)

        except Exception as exc:
            failed += 1
            errors.append((ref_curie, referencefile_id, str(exc)))
            logger.error(
                "  FAILED %s (referencefile_id=%d): %s",
                ref_curie, referencefile_id, exc,
            )
            db.rollback()

    logger.info(
        "Done. converted=%d, failed=%d, total=%d",
        converted, failed, len(rows),
    )

    if errors:
        logger.info("Errors:")
        for ref_curie, reffile_id, err in errors:
            logger.info("  %s (referencefile_id=%d): %s", ref_curie, reffile_id, err)


if __name__ == "__main__":
    main()
