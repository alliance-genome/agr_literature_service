#!/usr/bin/env python3
"""
Test script for PDF to Markdown conversion.

This script tests the process_pdf_for_reference() function by converting
a PDF for a given paper curie.

Usage:
    # Test with a PMID
    python test_pdf2md.py PMID:12345

    # Test with an AGRKB curie
    python test_pdf2md.py AGRKB:101000000000001

    # Test with a MOD paper curie
    python test_pdf2md.py WB:WBPaper00000001

    # Test with specific pdf_type
    python test_pdf2md.py PMID:12345 --pdf-type supplement

    # Test with specific extraction methods
    python test_pdf2md.py PMID:12345 --methods grobid,docling

    # Dry run (just resolve curie and find PDFs, don't process)
    python test_pdf2md.py PMID:12345 --dry-run

Environment variables required:
    PDFX_CLIENT_ID - Cognito client ID for PDFX API
    PDFX_CLIENT_SECRET - Cognito client secret for PDFX API
"""
import argparse
import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
    process_pdf_for_reference,
    resolve_curie_to_reference,
    get_pdf_files_for_reference,
    EXTRACTION_METHODS,
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Test PDF to Markdown conversion for a paper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "curie",
        help="Paper curie (PMID:12345, AGRKB:..., WB:WBPaper..., etc.)"
    )
    parser.add_argument(
        "--pdf-type",
        choices=["main", "supplement", "both"],
        default="main",
        help="Type of PDFs to process (default: main)"
    )
    parser.add_argument(
        "--methods",
        type=str,
        default=None,
        help="Comma-separated extraction methods (default: all - grobid,docling,marker,merged)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only resolve curie and find PDFs, don't process"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose/debug logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse methods if provided
    methods_to_extract = None
    if args.methods:
        methods_to_extract = [m.strip() for m in args.methods.split(",")]
        invalid_methods = [m for m in methods_to_extract if m not in EXTRACTION_METHODS]
        if invalid_methods:
            print(f"Error: Invalid extraction methods: {invalid_methods}")
            print(f"Valid methods: {list(EXTRACTION_METHODS.keys())}")
            sys.exit(1)

    # Create database session
    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        connect_args={"options": "-c timezone=utc"}
    )
    Session = sessionmaker(bind=engine, autoflush=True)
    db = Session()

    try:
        print("\n" + "=" * 60)
        print("Testing PDF to Markdown conversion")
        print("=" * 60)
        print(f"Input curie: {args.curie}")
        print(f"PDF type: {args.pdf_type}")
        print(f"Methods: {methods_to_extract or 'all'}")
        print(f"Dry run: {args.dry_run}")
        print("=" * 60 + "\n")

        # Step 1: Resolve curie to reference
        print("Step 1: Resolving curie to reference...")
        reference = resolve_curie_to_reference(db, args.curie)

        if not reference:
            print(f"ERROR: Could not resolve curie '{args.curie}' to a reference")
            sys.exit(1)

        print(f"  Found reference: {reference.curie}")
        print(f"  Reference ID: {reference.reference_id}")
        if hasattr(reference, 'title') and reference.title:
            title = reference.title[:80] + "..." if len(reference.title) > 80 else reference.title
            print(f"  Title: {title}")

        # Step 2: Find PDF files
        print(f"\nStep 2: Finding PDF files (type={args.pdf_type})...")
        pdf_files = get_pdf_files_for_reference(db, reference.reference_id, args.pdf_type)

        if not pdf_files:
            print(f"  No PDF files found for pdf_type='{args.pdf_type}'")
            sys.exit(1)

        print(f"  Found {len(pdf_files)} PDF file(s):")
        for pdf in pdf_files:
            mod_abbr = None
            for rfm in pdf.referencefile_mods:
                if rfm.mod:
                    mod_abbr = rfm.mod.abbreviation
                    break
            print(f"    - {pdf.display_name}.{pdf.file_extension}")
            print(f"      file_class: {pdf.file_class}, mod: {mod_abbr or 'None (PMC)'}")
            print(f"      referencefile_id: {pdf.referencefile_id}")

        if args.dry_run:
            print("\n[DRY RUN] Skipping actual PDF processing.")
            print("To process, run without --dry-run flag.")
            sys.exit(0)

        # Step 3: Process PDFs
        print("\nStep 3: Processing PDFs through PDFX service...")
        print("  This may take several minutes depending on PDF size and queue...")

        result = process_pdf_for_reference(
            db=db,
            curie=args.curie,
            pdf_type=args.pdf_type,
            methods_to_extract=methods_to_extract
        )

        # Print results
        print("\n" + "=" * 60)
        print("Results")
        print("=" * 60)
        print(f"Success: {result['success']}")
        print(f"Reference curie: {result['reference_curie']}")
        print(f"PDFs processed: {result['pdfs_processed']}")
        print(f"PDFs succeeded: {result['pdfs_succeeded']}")
        print(f"PDFs failed: {result['pdfs_failed']}")

        if result.get('error'):
            print(f"Error: {result['error']}")

        if result.get('details'):
            print("\nDetails:")
            for detail in result['details']:
                status = "SUCCESS" if detail['success'] else "FAILED"
                print(f"  [{status}] {detail['display_name']} ({detail['file_class']})")
                if detail['methods_uploaded']:
                    print(f"    Methods uploaded: {detail['methods_uploaded']}")
                if detail['error']:
                    print(f"    Error: {detail['error']}")

        print("=" * 60 + "\n")

        # Return appropriate exit code
        sys.exit(0 if result['success'] else 1)

    except Exception as e:
        logger.exception(f"Error during test: {e}")
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
