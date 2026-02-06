#!/usr/bin/env python3
"""
Script to download PDFs from Europe PMC for references that:
1. Have a PMCID in cross_reference table
2. Do NOT already have a main PDF from PMC (referencefile with mod_id=NULL, file_class='main', file_extension='pdf')
3. Have corpus=True in mod_corpus_association table

Usage:
    source .env_cc
    python download_pdfs_from_europepmc.py [--dry-run] [--limit N] [--output-dir DIR] [--workers N]
"""

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple

import requests
from sqlalchemy import create_engine, text


def get_db_connection():
    """Create database connection using environment variables."""
    host = os.environ.get('PSQL_HOST', 'localhost')
    port = os.environ.get('PSQL_PORT', '5432')
    database = os.environ.get('PSQL_DATABASE', 'literature')
    username = os.environ.get('PSQL_USERNAME', 'postgres')
    password = os.environ.get('PSQL_PASSWORD', 'postgres')

    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    return engine


def get_pmcids_without_main_pdf(engine, limit: Optional[int] = None) -> list:
    """
    Get PMCIDs that:
    1. Exist in cross_reference table with curie_prefix = 'PMCID'
    2. Do NOT have a main PDF with mod_id=NULL in referencefile/referencefile_mod
    3. Have corpus=True in mod_corpus_association

    Returns list of tuples: (reference_id, pmcid)
    """
    query = """
    SELECT DISTINCT
        cr.reference_id,
        cr.curie as pmcid
    FROM cross_reference cr
    INNER JOIN mod_corpus_association mca ON cr.reference_id = mca.reference_id
    WHERE cr.curie_prefix = 'PMCID'
      AND cr.is_obsolete = false
      AND mca.corpus = true
      AND NOT EXISTS (
          SELECT 1
          FROM referencefile rf
          INNER JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
          WHERE rf.reference_id = cr.reference_id
            AND rfm.mod_id IS NULL
            AND rf.file_class = 'main'
            AND rf.file_extension = 'pdf'
      )
    ORDER BY cr.reference_id
    """

    if limit:
        query += f" LIMIT {limit}"

    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [(row[0], row[1]) for row in result]


def check_pdf_available(pmcid: str, timeout: int = 30) -> bool:
    """
    Check if a PDF is available at Europe PMC for the given PMCID.

    Args:
        pmcid: The PMCID (e.g., 'PMCID:PMC12818324' or 'PMC12818324')
        timeout: Request timeout in seconds

    Returns:
        True if PDF is available, False otherwise
    """
    pmc_id = pmcid.replace('PMCID:', '')
    url = f"https://europepmc.org/backend/ptpmcrender.fcgi?accid={pmc_id}&blobtype=pdf"

    try:
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        content_type = response.headers.get('Content-Type', '')
        return response.status_code == 200 and 'application/pdf' in content_type
    except requests.RequestException:
        return False


def download_pdf_by_pmcid(pmcid: str, output_path: str = None, timeout: int = 60) -> Optional[str]:
    """
    Download a PDF from Europe PMC for the given PMCID.

    Args:
        pmcid: The PMCID (e.g., 'PMCID:PMC12818324' or 'PMC12818324')
        output_path: Optional output file path
        timeout: Request timeout in seconds

    Returns:
        The output path if successful, None otherwise
    """
    pmc_id = pmcid.replace('PMCID:', '')
    url = f"https://europepmc.org/backend/ptpmcrender.fcgi?accid={pmc_id}&blobtype=pdf"

    try:
        response = requests.get(url, timeout=timeout)
        if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', ''):
            output_path = output_path or f"{pmc_id}.pdf"
            with open(output_path, 'wb') as f:
                f.write(response.content)
            return output_path
    except requests.RequestException:
        pass

    return None


def check_single_pmcid(args: Tuple[int, int, str, bool, str]) -> dict:
    """
    Check a single PMCID and optionally download it.

    Args:
        args: Tuple of (index, reference_id, pmcid, dry_run, output_dir)

    Returns:
        dict with results
    """
    index, reference_id, pmcid, dry_run, output_dir = args
    pmc_id = pmcid.replace('PMCID:', '')

    result = {
        'index': index,
        'reference_id': reference_id,
        'pmcid': pmcid,
        'available': False,
        'downloaded': False,
        'output_path': None
    }

    if check_pdf_available(pmcid):
        result['available'] = True

        if not dry_run:
            output_path = os.path.join(output_dir, f"{pmc_id}.pdf")
            if download_pdf_by_pmcid(pmcid, output_path):
                result['downloaded'] = True
                result['output_path'] = output_path

    return result


def main():
    parser = argparse.ArgumentParser(
        description='Download PDFs from Europe PMC for references without main PDFs'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only check availability, do not download'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit the number of PMCIDs to process'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='pmc_pdfs',
        help='Directory to save downloaded PDFs (default: pmc_pdfs)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=20,
        help='Number of parallel workers (default: 20)'
    )

    args = parser.parse_args()

    # Create output directory if not dry run
    if not args.dry_run:
        os.makedirs(args.output_dir, exist_ok=True)

    print("Connecting to database...")
    try:
        engine = get_db_connection()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("Make sure you have sourced .env_cc first: source .env_cc")
        sys.exit(1)

    print("Querying for PMCIDs without main PDFs...")
    pmcids = get_pmcids_without_main_pdf(engine, args.limit)
    total = len(pmcids)
    print(f"Found {total} PMCIDs without main PDFs")

    if not pmcids:
        print("No PMCIDs to process.")
        return

    available_pmcids = []
    unavailable_pmcids = []
    downloaded_count = 0

    # Prepare work items
    work_items = [
        (i, ref_id, pmcid, args.dry_run, args.output_dir)
        for i, (ref_id, pmcid) in enumerate(pmcids, 1)
    ]

    print(f"\nChecking PDF availability at Europe PMC with {args.workers} workers...")
    print("=" * 60)

    completed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(check_single_pmcid, item): item for item in work_items}

        for future in as_completed(futures):
            result = future.result()
            completed += 1

            status = "AVAILABLE" if result['available'] else "NOT AVAILABLE"
            if result['downloaded']:
                status += f" -> Downloaded"

            # Progress update every 100 items or at completion
            if completed % 100 == 0 or completed == total:
                print(f"Progress: {completed}/{total} ({100*completed/total:.1f}%)")

            if result['available']:
                available_pmcids.append((result['reference_id'], result['pmcid']))
                if result['downloaded']:
                    downloaded_count += 1
            else:
                unavailable_pmcids.append((result['reference_id'], result['pmcid']))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total PMCIDs checked: {total}")
    print(f"Available at Europe PMC: {len(available_pmcids)}")
    print(f"Not available: {len(unavailable_pmcids)}")

    if not args.dry_run:
        print(f"Successfully downloaded: {downloaded_count}")

    # Save lists to files
    if available_pmcids:
        with open('pmcids_available.txt', 'w') as f:
            f.write("reference_id\tpmcid\n")
            for ref_id, pmcid in sorted(available_pmcids):
                f.write(f"{ref_id}\t{pmcid}\n")
        print(f"\nAvailable PMCIDs saved to: pmcids_available.txt")

    if unavailable_pmcids:
        with open('pmcids_unavailable.txt', 'w') as f:
            f.write("reference_id\tpmcid\n")
            for ref_id, pmcid in sorted(unavailable_pmcids):
                f.write(f"{ref_id}\t{pmcid}\n")
        print(f"Unavailable PMCIDs saved to: pmcids_unavailable.txt")


if __name__ == '__main__':
    main()
