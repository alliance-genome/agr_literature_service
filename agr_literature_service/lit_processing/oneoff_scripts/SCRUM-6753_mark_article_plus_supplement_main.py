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
captioned PDF from 'supplement' into 'main'. Like the loader, it also turns
the article XML (the PMC .xml named after the PMCID, e.g. PMC5823974.1.xml)
from 'supplement' into 'nXML', so these papers convert from the nXML just
as newly loaded ones do. A row last edited by someone other than
load_pmc_metadata is reported and skipped, so a curator's decision stands.
Right after each paper is committed it moves the paper's file workflow tag
to "file uploaded" for its corpus MODs, so a run that stops partway leaves
no committed paper without its workflow change.

No transaction is open while Europe PMC is called: each paper is read,
updated and committed in its own short transaction. A transaction held
across the HTTP calls kept row locks on referencefile for minutes, and on
stage it deadlocked with the API's startup, which reinstalls the reference
and referencefile triggers (DROP/CREATE TRIGGER takes an exclusive lock).

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


def pmc_file():
    """referencefile rows that came from PMC (shared by all MODs, mod_id NULL)."""
    return exists().where(and_(
        ReferencefileModAssociationModel.referencefile_id == ReferencefileModel.referencefile_id,
        ReferencefileModAssociationModel.mod_id.is_(None)))


def get_candidates(db):
    """{reference_id: (pmcid, [(referencefile_id, display_name)])} of the PMC
    supplement PDFs of references without a PMC main file. Plain values, so
    the caller can end the read transaction before the Europe PMC calls."""
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
        candidates.setdefault(row.reference_id, (pmcid.replace("PMCID:", ""), []))[1].append(
            (row.referencefile_id, row.display_name))
    return candidates


def get_article_xml(db, reference_id, pmcid):
    """The reference's PMC article XML still classed 'supplement': the .xml named
    after the PMCID (PMC packages name it <PMCID>.<version>.xml)."""
    name = pmcid.lower()
    return [row for row in db.query(ReferencefileModel).filter(
        ReferencefileModel.reference_id == reference_id,
        ReferencefileModel.file_class == "supplement",
        ReferencefileModel.file_extension == "xml",
        pmc_file()).all()
        if row.display_name.lower() == name or row.display_name.lower().startswith(name + ".")]


def edited_by_someone_else(row):
    return bool(row.updated_by) and row.updated_by != LOADER_USER


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
    xml_marked = 0
    skipped_edited = 0
    no_article_pdf = 0
    try:
        candidates = get_candidates(db)
        db.rollback()  # end the read transaction: none may be open during the HTTP calls
        logger.info(f"{len(candidates)} reference(s) with a PMC supplement PDF and no PMC main file")
        for count, (reference_id, (pmcid, pdfs)) in enumerate(candidates.items(), start=1):
            if count % 500 == 0:
                logger.info(f"progress: {count}/{len(candidates)}, {len(marked)} marked")
            article_pdf_root = fetch_article_pdf_root(http, pmcid)
            time.sleep(0.1)
            referencefile_id = next((rf_id for rf_id, name in pdfs if name.lower() == article_pdf_root), None)
            if referencefile_id is None:
                no_article_pdf += 1
                continue
            row = db.get(ReferencefileModel, referencefile_id)
            if row is None or row.file_class != "supplement":
                db.rollback()
                continue
            if edited_by_someone_else(row):
                skipped_edited += 1
                logger.info(f"skip {pmcid} referencefile {row.referencefile_id} "
                            f"'{row.display_name}.pdf': last edited by {row.updated_by}")
                db.rollback()
                continue
            logger.info(f"{pmcid} reference_id {reference_id}: "
                        f"'{row.display_name}.pdf' supplement -> main")
            marked.add(reference_id)
            for xml in get_article_xml(db, reference_id, pmcid):
                if edited_by_someone_else(xml):
                    logger.info(f"skip {pmcid} referencefile {xml.referencefile_id} "
                                f"'{xml.display_name}.xml': last edited by {xml.updated_by}")
                    continue
                logger.info(f"{pmcid} reference_id {reference_id}: "
                            f"'{xml.display_name}.xml' supplement -> nXML")
                xml_marked += 1
                if args.apply:
                    xml.file_class = "nXML"
            if args.apply:
                row.file_class = "main"
                db.commit()
                transition_workflow_for_uploaded_pdfs(db, {reference_id})
            else:
                db.rollback()

        logger.info(f"summary: {len(marked)} PDF(s) marked main and {xml_marked} XML(s) marked "
                    f"nXML; {skipped_edited} PDF(s) skipped as manually edited; {no_article_pdf} "
                    f"reference(s) without an 'Article plus Supplemental Information' PDF")
        if args.apply:
            logger.info("[APPLY] committed")
        else:
            db.rollback()
            logger.info("[DRY RUN] nothing committed")
    finally:
        db.close()


if __name__ == "__main__":
    main()
