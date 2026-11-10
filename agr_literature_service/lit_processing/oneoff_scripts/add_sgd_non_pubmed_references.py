"""Add SGD's non-PubMed references (GO_REFs etc.) to ABC with SGD's ID (SCRUM-6833).

SGD makes some references that have no PMID or DOI, for example the GO_REF
methodology references that GO annotations cite. load_sgd_created_references.py
can only match papers by PMID or DOI, so it reports these as "needs a
curator". This script copies them from SGD into ABC the way the older SGD
GO_REFs got there (from SGD's DQM file): an SGD-only reference with SGD's ID
as its one cross reference, SGD's reference type, and in the SGD corpus with
sort source dqm_files.

The reference is created with SGD's ID first and put in the SGD corpus after,
by reference_id, so sgd_curie_listener sees the ID and does not mint another
one. A reference whose SGD ID is already active in ABC is only put in the SGD
corpus (this also finishes a run that stopped in between). A reference is
skipped if its SGD ID is obsolete in ABC, or if SGD gives it a PMID or DOI
(those belong to load_sgd_created_references.py).

Usage:
    python add_sgd_non_pubmed_references.py --sgdid S000381662 --dry-run
    python add_sgd_non_pubmed_references.py --sgdid S000381662
"""
import argparse
import logging
import re
from os import environ
from typing import Dict, List, Optional

import requests

from agr_literature_service.api.crud import reference_crud
from agr_literature_service.api.models import CrossReferenceModel, ModCorpusAssociationModel, ModModel, \
    ReferenceModel
from agr_literature_service.api.schemas import ModCorpusSortSourceType, ReferenceSchemaPost
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SCRIPT_NAME = "add_sgd_non_pubmed_references"
SGD_REFERENCE_URL = environ.get("SGD_REFERENCE_URL", "https://www.yeastgenome.org/backend/reference/{}")
CATEGORY = "personal_communication"


def fetch_sgd_reference(sgdid: str) -> Dict:
    response = requests.get(SGD_REFERENCE_URL.format(sgdid), timeout=120)
    response.raise_for_status()
    return response.json()


def strip_html(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", text)).strip() or None


def title_from_citation(citation: Optional[str], display_name: Optional[str]) -> str:
    """SGD's reference JSON has no title; its citation is
    "<display_name> <title>", e.g. "SGD (2025) YeastPathways to GO-CAM conversion"."""
    citation = (citation or "").strip()
    if display_name and citation.startswith(display_name):
        return citation[len(display_name):].strip()
    return citation


def build_payload(sgd: Dict) -> Dict:
    """ABC reference for one SGD non-PubMed reference."""
    sgdid = sgd["sgdid"]
    year = str(sgd["year"]) if sgd.get("year") else None
    abstract = sgd.get("abstract")
    payload = {
        "title": title_from_citation(sgd.get("citation"), sgd.get("display_name")),
        "category": CATEGORY,
        "date_published": year,
        "date_published_start": f"{year}-01-01" if year else None,
        "abstract": strip_html(abstract.get("text") if isinstance(abstract, dict) else abstract),
        "authors": [{"name": a["display_name"], "author_order": i}
                    for i, a in enumerate(sgd.get("authors") or [], start=1)],
        "cross_references": [{"curie": f"SGD:{sgdid}", "pages": ["reference"]}],
        "mod_reference_types": [{"reference_type": t["display_name"], "mod_abbreviation": "SGD"}
                                for t in sgd.get("reftypes") or []],
    }
    return payload


def skip_reason(sgd: Dict, xref: Optional[CrossReferenceModel]) -> Optional[str]:
    if sgd.get("pubmed_id") or sgd.get("doi"):
        return f"has PMID/DOI ({sgd.get('pubmed_id')}/{sgd.get('doi')}); use load_sgd_created_references.py"
    if xref is not None and (xref.is_obsolete or xref.reference_id is None):
        return f"SGD:{sgd['sgdid']} is obsolete in ABC (reference_id={xref.reference_id})"
    return None


def put_in_sgd_corpus(db, sgd_mod_id: int, reference_id: int, dry_run: bool) -> str:  # pragma: no cover
    """Put the paper in the SGD corpus. By reference_id, not the reference
    object: sgd_curie_listener only finds the paper's saved SGD ID by
    reference_id, and would otherwise mint a second one."""
    mca = db.query(ModCorpusAssociationModel).filter(
        ModCorpusAssociationModel.reference_id == reference_id,
        ModCorpusAssociationModel.mod_id == sgd_mod_id).one_or_none()
    if mca is not None and mca.corpus is True:
        return "already in the SGD corpus"
    if not dry_run:
        if mca is None:
            db.add(ModCorpusAssociationModel(reference_id=reference_id, mod_id=sgd_mod_id, corpus=True,
                                             mod_corpus_sort_source=ModCorpusSortSourceType.Dqm_files))
        else:
            mca.corpus = True
        db.commit()
    return "put in the SGD corpus" if mca is None else "moved into the SGD corpus"


def add_references(sgdids: List[str], dry_run: bool) -> None:  # pragma: no cover
    db = create_postgres_session(False)
    set_global_user_id(db, SCRIPT_NAME)
    logger.info(f"ABC database: {environ.get('PSQL_HOST')}/{environ.get('PSQL_DATABASE')}"
                f"{' (dry run)' if dry_run else ''}")
    sgd_mod_id = db.query(ModModel.mod_id).filter(ModModel.abbreviation == "SGD").scalar()
    for sgdid in sgdids:
        sgd = fetch_sgd_reference(sgdid)
        xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == f"SGD:{sgdid}").first()
        reason = skip_reason(sgd, xref)
        if reason:
            logger.info(f"{sgdid}: skipped, {reason}")
            continue
        if sgd.get("go_ref_id"):
            logger.info(f"{sgdid}: {sgd['go_ref_id']}")
        if xref is not None:
            logger.info(f"{sgdid}: already in ABC (reference_id={xref.reference_id})")
            reference_id = xref.reference_id
        else:
            payload = build_payload(sgd)
            logger.info(f"{sgdid}: {payload}")
            if dry_run:
                logger.info(f"{sgdid}: would be added and put in the SGD corpus")
                continue
            curie = reference_crud.create(db, ReferenceSchemaPost(**payload))
            reference_id = db.query(ReferenceModel.reference_id).filter(ReferenceModel.curie == curie).scalar()
            logger.info(f"{sgdid}: added as {curie} (reference_id={reference_id})")
        outcome = put_in_sgd_corpus(db, sgd_mod_id, reference_id, dry_run)
        logger.info(f"{sgdid}: {'would be ' if dry_run and not outcome.startswith('already') else ''}{outcome}")
    db.close()


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Add SGD's non-PubMed references to ABC with SGD's ID")
    parser.add_argument("--sgdid", action="append", required=True,
                        help="SGD reference ID, e.g. S000381662 (repeatable)")
    parser.add_argument("--dry-run", action="store_true", help="show what would be added, change nothing")
    args = parser.parse_args()
    add_references([s.replace("SGD:", "") for s in args.sgdid], args.dry_run)
