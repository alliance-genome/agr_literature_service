"""
parse_springer_oa_pdf.py
========================
Regenerate data/springer_oa_journals.json from the Springer Nature fully
open access journals PDF.

The PDF is published by Springer Nature at:
    https://cms-resources.apps.public.k8s.springernature.io/springer-cms/rest/v1/content/27820860/data/v3
(the trailing "v3" is the 2026 revision; bump it when Springer publishes a
new version and pass --download to refresh the local copy).

Each table row holds: Journal title, Imprint, eISSN, APCs, Website. Only
title and eISSN are kept. pypdf sometimes runs columns together without
spaces, so the ISSN regex must not require word boundaries, and titles for
ISSNs already present in the existing JSON are preserved (they were
hand-checked) — only new ISSNs get their title extracted from the PDF.

Journals listed without an eISSN (usually brand-new launches) cannot be
matched to resources and are logged, not written to the JSON.

Requires pypdf (not part of the service requirements):
    pip install pypdf

Usage:
    python parse_springer_oa_pdf.py [--download] [--pdf FILE] [--output FILE]
"""
import argparse
import json
import logging
import re
from os import path

import requests
from pypdf import PdfReader

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SCRIPT_DIR = path.dirname(path.abspath(__file__))
DEFAULT_PDF = path.join(SCRIPT_DIR, "data", "springer_nature_fully_open_access_journals.pdf")
DEFAULT_JSON = path.join(SCRIPT_DIR, "data", "springer_oa_journals.json")
PDF_URL = "https://cms-resources.apps.public.k8s.springernature.io/springer-cms/rest/v1/content/27820860/data/v3"

# No \b before the ISSN: pypdf can run the imprint straight into the ISSN
# (e.g. "co-published with Springer1993-0607")
ISSN_RE = re.compile(r"(\d{4}-\d{3}[\dXx])\b")

# Imprint column values that pypdf appends to the title text
IMPRINT_SUFFIXES = [
    "BioMed Central",
    "Springer Nature",
    "SpringerOpen",
    "Nature Portfolio",
    "Palgrave Macmillan",
    "BSL Media & Learning",
    "Discover",
    "Adis",
    "Springer",
]
# pypdf runs the title straight into the publisher (e.g. "Journal of
# Forestry ResearchNortheast Forestry University, co-published with
# Springer"), so cut at the lowercase-to-uppercase transition
CO_PUBLISHED_RE = re.compile(r"(?<=[a-z)])[A-Z][^,]*,\s*co-published with .*$")


def clean_title(raw_title: str) -> str:
    """Strip the imprint column text that pypdf appends to the title."""
    title = CO_PUBLISHED_RE.sub("", raw_title).strip()
    for imprint in IMPRINT_SUFFIXES:
        if title.endswith(imprint) and len(title) > len(imprint):
            title = title[: -len(imprint)].strip()
            break
    return title


def extract_journals(pdf_file: str) -> dict:
    """Extract eISSN -> raw title text from the PDF table rows."""
    reader = PdfReader(pdf_file)
    journals = {}
    no_issn_lines = []
    for page in reader.pages:
        for line in page.extract_text().split("\n"):
            line = line.strip()
            if not line:
                continue
            match = ISSN_RE.search(line)
            if match:
                title = clean_title(line[: match.start()])
                if title:
                    journals[match.group(1)] = title
            elif re.search(r"\d,\d{3}|see website", line):
                # table row with prices but no eISSN (new journal launches)
                no_issn_lines.append(line)

    logger.info(f"Extracted {len(journals)} journals with eISSN from {len(reader.pages)} pages")
    if no_issn_lines:
        logger.info(f"{len(no_issn_lines)} table rows have no eISSN (not written to JSON):")
        for line in no_issn_lines:
            logger.info(f"  {line[:100]}")
    return journals


def main():
    parser = argparse.ArgumentParser(
        description="Regenerate springer_oa_journals.json from the Springer OA journals PDF"
    )
    parser.add_argument("--pdf", default=DEFAULT_PDF, help="Path of the local PDF copy")
    parser.add_argument("--output", default=DEFAULT_JSON, help="Output JSON file")
    parser.add_argument("--download", action="store_true",
                        help=f"Download the current PDF from {PDF_URL} to --pdf first")
    args = parser.parse_args()

    if args.download:
        logger.info(f"Downloading {PDF_URL}")
        response = requests.get(PDF_URL, timeout=120)
        response.raise_for_status()
        with open(args.pdf, "wb") as f:
            f.write(response.content)

    journals = extract_journals(args.pdf)

    # Preserve hand-checked titles for ISSNs already in the existing JSON
    existing = {}
    if path.exists(args.output):
        with open(args.output, "r", encoding="utf-8") as f:
            existing = json.load(f).get("journals", {})
    added = sorted(set(journals) - set(existing))
    removed = sorted(set(existing) - set(journals))
    merged = {issn: existing.get(issn, title) for issn, title in journals.items()}

    for issn in added:
        logger.info(f"ADDED:   {issn}  {merged[issn]}")
    for issn in removed:
        logger.info(f"REMOVED: {issn}  {existing[issn]}")

    output = {
        "source": PDF_URL,
        "description": "Springer Nature fully open access journals extracted from the PDF above",
        "journals": dict(sorted(merged.items(), key=lambda item: item[1].lower())),
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
        f.write("\n")
    logger.info(f"Wrote {len(merged)} journals to {args.output} "
                f"({len(added)} added, {len(removed)} removed)")


if __name__ == "__main__":
    main()
