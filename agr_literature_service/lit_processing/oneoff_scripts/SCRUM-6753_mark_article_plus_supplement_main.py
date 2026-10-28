#!/usr/bin/env python3
"""
Mark the PMC "Article plus Supplemental Information" PDF as the main PDF
(SCRUM-6753).

Cell Press and other Elsevier author manuscripts have no PDF of the article
in PMC: the article XML is not matched by any PDF, and the whole article
ships as a supplement captioned "Document S2. Article plus Supplemental
Information". load_pmc_metadata only took a PDF named like the XML as main,
so these papers were left with every PMC PDF a 'supplement' and stuck in
"file upload in progress". The loader now reads the caption; this script
fixes the papers loaded before that.

For each reference with a PMC supplement PDF and no PMC main file, it reads
the supplement captions from the Europe PMC article XML and turns the
captioned PDF from 'supplement' into 'main'. A row last edited by someone
other than load_pmc_metadata is reported and skipped, so a curator's
decision stands. It then moves the reference's file workflow tag to "file
uploaded" for its corpus MODs.

    python SCRUM-6753_mark_article_plus_supplement_main.py          # dry run
    python SCRUM-6753_mark_article_plus_supplement_main.py --apply
"""

import argparse
import logging
import time
from os import path
from typing import Optional

import requests
from sqlalchemy import and_, exists

from agr_literature_service.api.models import CrossReferenceModel, ReferencefileModel, \
    ReferencefileModAssociationModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.load_pmc_metadata import \
    find_article_pdf_root, transition_workflow_for_uploaded_pdfs
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SCRIPT_USER = path.basename(__file__).replace(".py", "")
LOADER_USER = "load_pmc_metadata"
EUROPEPMC_XML_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
BATCH_COMMIT_SIZE = 100


def pmc_file():
    """referencefile rows that came from PMC (shared by all MODs, mod_id NULL)."""
    return exists().where(and_(
        ReferencefileModAssociationModel.referencefile_id == ReferencefileModel.referencefile_id,
        ReferencefileModAssociationModel.mod_id.is_(None)))


def get_candidates(db):
    """{reference_id: (pmcid, [PMC supplement PDF rows])} for references
    without a PMC main file."""
    PmcMain = ReferencefileModel.__table__.alias("pmc_main")
    has_pmc_main = exists().where(and_(
        PmcMain.c.reference_id == ReferencefileModel.reference_id,
        PmcMain.c.file_class == "main",
        exists().where(and_(
            ReferencefileModAssociationModel.referencefile_id == PmcMain.c.id,
            ReferencefileModAssociationModel.mod_id.is_(None)))))
    rows = (db.query(ReferencefileModel, CrossReferenceModel.curie)
            .join(CrossReferenceModel, and_(
                CrossReferenceModel.reference_id == ReferencefileModel.reference_id,
                CrossReferenceModel.curie_prefix == "PMCID",
                CrossReferenceModel.is_obsolete.is_(False)))
            .filter(ReferencefileModel.file_class == "supplement",
                    ReferencefileModel.file_extension == "pdf",
                    pmc_file(), ~has_pmc_main)
            .order_by(ReferencefileModel.reference_id).all())
    candidates: dict = {}
    for row, pmcid in rows:
        candidates.setdefault(row.reference_id, (pmcid.replace("PMCID:", ""), []))[1].append(row)
    return candidates


def fetch_article_pdf_root(session, pmcid) -> Optional[str]:
    for attempt in range(3):
        try:
            response = session.get(EUROPEPMC_XML_URL.format(pmcid=pmcid), timeout=60)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return find_article_pdf_root(response.text)
        except requests.RequestException as e:
            logger.warning(f"{pmcid}: Europe PMC request failed ({e}), attempt {attempt + 1}")
            time.sleep(5 * (attempt + 1))
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="commit the changes (default is dry run)")
    args = parser.parse_args()

    db = create_postgres_session(False)
    set_global_user_id(db, SCRIPT_USER)
    http = requests.Session()
    marked = set()
    skipped_edited = 0
    no_article_pdf = 0
    since_commit = 0
    try:
        candidates = get_candidates(db)
        logger.info(f"{len(candidates)} reference(s) with a PMC supplement PDF and no PMC main file")
        for count, (reference_id, (pmcid, pdfs)) in enumerate(candidates.items(), start=1):
            if count % 500 == 0:
                logger.info(f"progress: {count}/{len(candidates)}, {len(marked)} marked")
            article_pdf_root = fetch_article_pdf_root(http, pmcid)
            time.sleep(0.1)
            row = next((pdf for pdf in pdfs if pdf.display_name.lower() == article_pdf_root), None)
            if row is None:
                no_article_pdf += 1
                continue
            if row.updated_by and row.updated_by != LOADER_USER:
                skipped_edited += 1
                logger.info(f"skip {pmcid} referencefile {row.referencefile_id} "
                            f"'{row.display_name}.pdf': last edited by {row.updated_by}")
                continue
            logger.info(f"{pmcid} reference_id {reference_id}: "
                        f"'{row.display_name}.pdf' supplement -> main")
            marked.add(reference_id)
            if args.apply:
                row.file_class = "main"
                since_commit += 1
                if since_commit >= BATCH_COMMIT_SIZE:
                    db.commit()
                    since_commit = 0

        logger.info(f"summary: {len(marked)} PDF(s) marked main; {skipped_edited} skipped as "
                    f"manually edited; {no_article_pdf} reference(s) without an "
                    f"'Article plus Supplemental Information' PDF")
        if args.apply:
            db.commit()
            transition_workflow_for_uploaded_pdfs(db, marked)
            logger.info("[APPLY] committed")
        else:
            db.rollback()
            logger.info("[DRY RUN] nothing committed")
    finally:
        db.close()


if __name__ == "__main__":
    main()
