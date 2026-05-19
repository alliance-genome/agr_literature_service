"""One-off backfill that attaches GEO Series (GSE) cross-references to existing
References. For every Reference that has a PMID cross-reference, queries NCBI
elink/esummary for linked GDS records and inserts any missing GEO xrefs.

Safe to re-run: a Reference already carrying a given `GEO:GSE...` row is
skipped, and the new partial index (post-migration a3f7c1d8b4e2) allows
multiple GEO rows per Reference.
"""
import argparse
import logging
import sys
from os import path
from typing import Dict, List, Tuple

from sqlalchemy import and_
from sqlalchemy.orm import Session

from agr_literature_service.api.crud import cross_reference_crud
from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel
from agr_literature_service.api.schemas.cross_reference_schemas import CrossReferenceSchemaPost
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_geo_links import (
    get_geo_accessions_for_pmids,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logger = logging.getLogger(__name__)


def _configure_logging(log_file: str = "") -> None:
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    root.setLevel(logging.INFO)


def _references_with_pmid(db: Session, mod_abbreviation: str = "", limit: int = 0) -> List[Tuple[int, str, str]]:
    """Return [(reference_id, reference_curie, pmid), ...] for refs with a PMID xref."""
    q = (db.query(ReferenceModel.reference_id, ReferenceModel.curie, CrossReferenceModel.curie)
         .join(CrossReferenceModel, CrossReferenceModel.reference_id == ReferenceModel.reference_id)
         .filter(and_(CrossReferenceModel.curie_prefix == "PMID",
                      CrossReferenceModel.is_obsolete.is_(False)))
         .order_by(ReferenceModel.reference_id))
    if mod_abbreviation:
        mod_xref = (db.query(CrossReferenceModel.reference_id)
                    .filter(and_(CrossReferenceModel.curie_prefix == mod_abbreviation,
                                 CrossReferenceModel.is_obsolete.is_(False))))
        q = q.filter(ReferenceModel.reference_id.in_(mod_xref))
    if limit > 0:
        q = q.limit(limit)
    out = []
    for ref_id, ref_curie, pmid_curie in q.all():
        pmid = pmid_curie.split(":", 1)[1] if ":" in pmid_curie else pmid_curie
        out.append((ref_id, ref_curie, pmid))
    return out


def _existing_geo_curies(db: Session, reference_ids: List[int]) -> Dict[int, set]:
    if not reference_ids:
        return {}
    rows = (db.query(CrossReferenceModel.reference_id, CrossReferenceModel.curie)
            .filter(and_(CrossReferenceModel.reference_id.in_(reference_ids),
                         CrossReferenceModel.curie_prefix == "GEO",
                         CrossReferenceModel.is_obsolete.is_(False)))
            .all())
    out: Dict[int, set] = {rid: set() for rid in reference_ids}
    for ref_id, curie in rows:
        out.setdefault(ref_id, set()).add(curie)
    return out


def _insert_geo_xrefs(db: Session, ref_curie: str, missing: List[str], dry_run: bool) -> int:
    inserted = 0
    for gse in missing:
        curie = f"GEO:{gse}"
        if dry_run:
            logger.info("DRY-RUN would insert %s for %s", curie, ref_curie)
            inserted += 1
            continue
        try:
            payload = CrossReferenceSchemaPost(curie=curie, reference_curie=ref_curie)
            cross_reference_crud.create(db, payload)
            inserted += 1
            logger.info("Inserted %s for %s", curie, ref_curie)
        except Exception as exc:
            # cross_reference_crud.create only rolls back on IntegrityError; any
            # other failure (OperationalError, deadlock, connection blip) leaves
            # the transaction aborted and breaks every subsequent insert in the
            # run. Rollback unconditionally — it's a no-op on a clean session.
            db.rollback()
            logger.warning("Failed to insert %s for %s: %s", curie, ref_curie, exc)
    return inserted


def backfill(mod_abbreviation: str = "",
             limit: int = 0,
             batch_size: int = 200,
             dry_run: bool = False) -> Dict[str, int]:
    db = create_postgres_session(False)
    set_global_user_id(db, path.basename(__file__).replace(".py", ""))

    refs = _references_with_pmid(db, mod_abbreviation=mod_abbreviation, limit=limit)
    logger.info("Found %d references with a PMID xref%s",
                len(refs), f" for {mod_abbreviation}" if mod_abbreviation else "")

    stats = {"refs_scanned": len(refs), "refs_with_new_geo": 0, "xrefs_added": 0}

    for start in range(0, len(refs), batch_size):
        batch = refs[start:start + batch_size]
        pmids = [pmid for _, _, pmid in batch]
        try:
            pmid_to_gse = get_geo_accessions_for_pmids(pmids)
        except Exception as exc:
            logger.error("elink batch failed (%d PMIDs starting at %d): %s", len(pmids), start, exc)
            continue
        existing = _existing_geo_curies(db, [r[0] for r in batch])
        for ref_id, ref_curie, pmid in batch:
            gse_list = pmid_to_gse.get(pmid, [])
            if not gse_list:
                continue
            already = {c.split(":", 1)[1] for c in existing.get(ref_id, set())}
            missing = [g for g in gse_list if g not in already]
            if not missing:
                continue
            added = _insert_geo_xrefs(db, ref_curie, missing, dry_run)
            if added:
                stats["refs_with_new_geo"] += 1
                stats["xrefs_added"] += added

    db.close()
    logger.info("Done. refs_scanned=%d refs_with_new_geo=%d xrefs_added=%d",
                stats["refs_scanned"], stats["refs_with_new_geo"], stats["xrefs_added"])
    return stats


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(description="Backfill GEO:GSE cross-references for References with a PMID.")
    parser.add_argument("--mod", default="", help="Only process References belonging to this MOD (e.g. ZFIN)")
    parser.add_argument("--limit", type=int, default=0, help="Cap the number of References processed (0 = no cap)")
    parser.add_argument("--batch-size", type=int, default=200,
                        help="PMIDs per elink call (default 200)")
    parser.add_argument("--dry-run", action="store_true", help="Log intended inserts but write nothing")
    parser.add_argument("--log-file", default="", help="Optional path to also write logs to a file")
    args = parser.parse_args()
    _configure_logging(args.log_file)
    backfill(mod_abbreviation=args.mod, limit=args.limit,
             batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":  # pragma: no cover
    main()
