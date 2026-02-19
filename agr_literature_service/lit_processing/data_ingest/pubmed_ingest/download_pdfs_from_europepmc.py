#!/usr/bin/env python3
"""
Download OA PDFs from Europe PMC, upload to S3, and load metadata into DB.

Pipeline:
  DB query -> OA core filter (isOpenAccess==Y and optionally hasPDF==Y) ->
  ensure local PDF (download if missing; otherwise use existing on disk) ->
  md5(pdf) -> gzip -> upload to S3 -> DB repair/insert:
    - If (reference_id, md5sum) not present: insert referencefile as main + referencefile_mod(mod_id NULL)
    - If (reference_id, md5sum) present: DO NOT skip blindly:
        - If misclassified (e.g. file_class='supplement'), promote it to main/final/pdf
        - Ensure mod_id NULL association exists

IMPORTANT CHANGE:
- We do NOT pre-filter out "already on disk" PDFs. If a PDF exists locally, we still compute md5 and load/repair DB.

ADDED (DB-wide repair step):
- Scan the DB for references where a supplemental PDF has display_name == its corresponding NXML display_name
  for the same reference_id.
    - If a main PDF exists for that reference (main/final/pdf + mod_id NULL), DELETE the misnamed supplement PDF.
    - Otherwise, PROMOTE ONE such supplement PDF to main/final/pdf and DELETE any other matches, and ensure mod_id NULL assoc.

Safety/robustness fixes vs prior version:
- Per-item DB SAVEPOINT so one failure does NOT rollback prior successful items in the current commit batch.
- Avoid full-table preload into memory; use per-item lookups with small in-run caches.
- OA metadata fetch loop catches HTTP errors; cache is saved incrementally and again in finally.
- Repair step reduces N+1 queries by prefetching in batch.
- needs_promotion also checks pdf_type correctness.
- Removes unused regex and dead commented code.
- Avoids extra HEAD request in non-dry-run mode (download attempt is the availability check).

Notes:
- Upload step is skipped if ENV_STATE is missing or ENV_STATE == 'test'.
- DB inserts are also skipped in those environments by default (to keep behavior consistent).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import gzip
import requests
from sqlalchemy import text

from agr_literature_service.api.models import ReferencefileModel, ReferencefileModAssociationModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import get_md5sum
from agr_literature_service.lit_processing.utils.s3_utils import upload_file_to_s3
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# -----------------------------
# Constants
# -----------------------------
S3_BUCKET = "agr-literature"

FILE_CLASS = "main"
FILE_PUBLICATION_STATUS = "final"
FILE_EXTENSION = "pdf"
PDF_TYPE = "pdf"


# -----------------------------
# DB query
# -----------------------------
def get_pmcids_without_main_pdf(limit: Optional[int] = None) -> List[Tuple[int, str]]:  # pragma: no cover
    """
    Returns:
        list of tuples: (reference_id, pmcid) where pmcid is like "PMCID:PMC1234567"
    """
    query = """
    SELECT DISTINCT
        cr.reference_id,
        cr.curie as pmcid
    FROM cross_reference cr
    INNER JOIN mod_corpus_association mca ON cr.reference_id = mca.reference_id
    INNER JOIN reference r ON r.reference_id = cr.reference_id
    WHERE cr.curie_prefix = 'PMCID'
      AND cr.is_obsolete = false
      AND mca.corpus = true
      AND (
        lower(r.language) = 'eng'
        OR lower(r.language) LIKE 'english%'
      )
      AND NOT EXISTS (
          SELECT 1
          FROM referencefile rf
          INNER JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
          WHERE rf.reference_id = cr.reference_id
            AND rfm.mod_id IS NULL
            AND rf.file_class = 'main'
            AND rf.file_extension = 'pdf'
            AND rf.file_publication_status = 'final'
      )
    ORDER BY cr.reference_id
    """
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    db = create_postgres_session(False)
    try:
        rs = db.execute(text(query))
        return [(int(r[0]), str(r[1])) for r in rs]
    finally:
        db.close()


# -----------------------------
# Helpers
# -----------------------------
def normalize_pmcid(pmcid: str) -> str:
    """Normalize 'PMCID:PMC123' or 'PMC123' to 'PMC123' (uppercase)."""
    s = pmcid.strip()
    if s.upper().startswith("PMCID:"):
        s = s.split(":", 1)[1]
    return s.upper()


def europepmc_pdf_url(pmc_id: str) -> str:
    return f"https://europepmc.org/backend/ptpmcrender.fcgi?accid={pmc_id}&blobtype=pdf"


def check_pdf_available(pmcid: str, timeout: int = 30) -> bool:
    """HEAD-check for dry-run reporting."""
    pmc_id = normalize_pmcid(pmcid)
    url = europepmc_pdf_url(pmc_id)
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        ct = r.headers.get("Content-Type", "")
        return r.status_code == 200 and "application/pdf" in ct.lower()
    except requests.RequestException:
        return False


def download_pdf_by_pmcid(pmcid: str, output_path: str, timeout: int = 60) -> bool:
    """
    Never overwrites output_path. Downloads to .tmp then atomic rename.
    Availability is determined by the GET response.
    """
    if os.path.exists(output_path):
        return False

    pmc_id = normalize_pmcid(pmcid)
    url = europepmc_pdf_url(pmc_id)
    tmp_path = output_path + ".tmp"

    try:
        with requests.get(url, timeout=timeout, stream=True) as r:
            ct = r.headers.get("Content-Type", "")
            if r.status_code != 200 or "application/pdf" not in ct.lower():
                return False

            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        os.replace(tmp_path, output_path)
        return True

    except (requests.RequestException, OSError):
        return False
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def gzip_file(pdf_path: str) -> Optional[str]:
    """Create pdf_path + '.gz'. Overwrites the .gz if present."""
    gz_path = pdf_path + ".gz"
    try:
        with open(pdf_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        return gz_path
    except Exception as e:
        logger.error(f"gzip failed for {pdf_path}: {e}")
        try:
            if os.path.exists(gz_path):
                os.remove(gz_path)
        except OSError:
            pass
        return None


def upload_pdf_file_to_s3(gz_path: str, md5sum: str) -> Optional[bool]:
    """
    Returns True/False if attempted; returns None if skipped due to ENV_STATE.
    """
    env_state = os.environ.get("ENV_STATE")
    if env_state is None or env_state == "test":
        return None

    s3_prefix = "/reference/documents/"
    if env_state == "prod":
        s3_prefix = "prod" + s3_prefix
        storage = "GLACIER_IR"
    else:
        s3_prefix = "develop" + s3_prefix
        storage = "STANDARD"

    s3_prefix = (
        s3_prefix
        + md5sum[0] + "/"
        + md5sum[1] + "/"
        + md5sum[2] + "/"
        + md5sum[3] + "/"
    )
    s3_key = s3_prefix + md5sum + ".gz"

    logger.info(f"Uploading {Path(gz_path).name} to s3://{S3_BUCKET}/{s3_key}")
    return upload_file_to_s3(gz_path, S3_BUCKET, s3_key, storage)


# -----------------------------
# OA metadata
# -----------------------------
@dataclass(frozen=True)
class PmcMeta:
    pmcid: str
    hit: bool
    is_open_access: Optional[bool]
    license: Optional[str]
    has_pdf: Optional[bool]
    in_pmc: Optional[bool]


def parse_yn(v: Optional[str]) -> Optional[bool]:
    if v == "Y":
        return True
    if v == "N":
        return False
    return None


def chunked(items: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(items), n):
        yield items[i:i + n]


def load_cache(cache_path: Path) -> Dict[str, dict]:
    if not cache_path.exists():
        return {}
    try:
        return json.loads(cache_path.read_text())
    except Exception:
        return {}


def save_cache(cache_path: Path, cache: Dict[str, dict]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, indent=2, sort_keys=True))


def fetch_batch_core(
    pmcids: List[str],
    session: requests.Session,
    timeout: int = 60,
    page_size: int = 1000,
) -> Dict[str, PmcMeta]:
    or_query = " OR ".join([f"PMCID:{p}" for p in pmcids])
    query = f"({or_query})"

    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    params = {"query": query, "resultType": "core", "format": "json", "pageSize": page_size}

    r = session.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    results = (data.get("resultList", {}) or {}).get("result", []) or []
    out: Dict[str, PmcMeta] = {}

    for res in results:
        pmcid = (res.get("pmcid") or "").upper()
        if not pmcid:
            continue
        out[pmcid] = PmcMeta(
            pmcid=pmcid,
            hit=True,
            is_open_access=parse_yn(res.get("isOpenAccess")),
            license=res.get("license"),
            has_pdf=parse_yn(res.get("hasPDF")),
            in_pmc=parse_yn(res.get("inPMC")),
        )
    return out


def is_oa_from_cache_entry(c: dict, require_has_pdf: bool = True) -> bool:
    if not c or not bool(c.get("hit", False)):
        return False
    if c.get("is_open_access", None) is not True:
        return False
    if require_has_pdf:
        return c.get("has_pdf", None) is True
    return True


# -----------------------------
# DB load helpers
# -----------------------------
@dataclass
class RfInfo:
    referencefile_id: int
    file_class: Optional[str]
    file_extension: Optional[str]
    file_publication_status: Optional[str]
    pdf_type: Optional[str]
    display_name: Optional[str]


def fetch_rf_info_by_key(db, reference_id: int, md5sum: str) -> Optional[RfInfo]:  # pragma: no cover
    row = db.execute(
        text(
            """
            SELECT referencefile_id, file_class, file_extension, file_publication_status, pdf_type, display_name
            FROM referencefile
            WHERE reference_id = :rid AND md5sum = :md5
            LIMIT 1
            """
        ),
        {"rid": reference_id, "md5": md5sum},
    ).fetchone()
    if not row:
        return None
    rf_id, fc, fe, fps, pt, dn = row
    return RfInfo(
        referencefile_id=int(rf_id),
        file_class=str(fc) if fc is not None else None,
        file_extension=str(fe) if fe is not None else None,
        file_publication_status=str(fps) if fps is not None else None,
        pdf_type=str(pt) if pt is not None else None,
        display_name=str(dn) if dn is not None else None,
    )


def has_null_mod_assoc(db, referencefile_id: int) -> bool:  # pragma: no cover
    row = db.execute(
        text(
            """
            SELECT 1
            FROM referencefile_mod
            WHERE referencefile_id = :rf_id AND mod_id IS NULL
            LIMIT 1
            """
        ),
        {"rf_id": referencefile_id},
    ).fetchone()
    return row is not None


def insert_referencefile(db, reference_id: int, display_name: str, md5sum: str) -> int:  # pragma: no cover
    x = ReferencefileModel(
        display_name=display_name,
        reference_id=reference_id,
        md5sum=md5sum,
        file_class=FILE_CLASS,
        file_publication_status=FILE_PUBLICATION_STATUS,
        file_extension=FILE_EXTENSION,
        pdf_type=PDF_TYPE,
        is_annotation=False,
    )
    db.add(x)
    db.flush()
    db.refresh(x)
    return int(x.referencefile_id)


def insert_referencefile_mod(db, referencefile_id: int) -> None:  # pragma: no cover
    x = ReferencefileModAssociationModel(referencefile_id=referencefile_id)
    db.add(x)


def promote_referencefile_to_main(db, rf_id: int) -> bool:  # pragma: no cover
    """
    Promote an existing referencefile row to main/final/pdf using ORM so hooks run.
    Returns True if anything changed, else False.
    """
    rf = db.get(ReferencefileModel, rf_id)
    if rf is None:
        return False

    changed = False

    if (rf.file_class or "").lower() != FILE_CLASS:
        rf.file_class = FILE_CLASS
        changed = True

    if (rf.file_extension or "").lower() != FILE_EXTENSION:
        rf.file_extension = FILE_EXTENSION
        changed = True

    if (rf.file_publication_status or "").lower() != FILE_PUBLICATION_STATUS:
        rf.file_publication_status = FILE_PUBLICATION_STATUS
        changed = True

    if (rf.pdf_type or "").lower() != PDF_TYPE:
        rf.pdf_type = PDF_TYPE
        changed = True

    if changed:
        db.flush()
    return changed


def needs_promotion(info: Optional[RfInfo]) -> bool:
    """True if record should be normalized to main/final/pdf + pdf_type."""
    if info is None:
        return False

    fc_ok = (info.file_class or "").lower() == FILE_CLASS
    fe_ok = (info.file_extension or "").lower() == FILE_EXTENSION
    fps_ok = (info.file_publication_status or "").lower() == FILE_PUBLICATION_STATUS
    pt_ok = (info.pdf_type or "").lower() == PDF_TYPE

    # If everything is correct, no promotion needed.
    if fc_ok and fe_ok and fps_ok and pt_ok:
        return False

    # Only promote rows that are plausibly PDFs (blank extension or pdf)
    fe = (info.file_extension or "").lower()
    return fe in ("", FILE_EXTENSION)


# -----------------------------
# DB-wide repair step (batch-prefetch, low query count)
# -----------------------------
def find_refs_with_nxml_named_supplement_pdfs(db, limit: Optional[int] = None) -> List[int]:  # pragma: no cover
    q = """
    SELECT DISTINCT sp.reference_id
    FROM referencefile sp
    INNER JOIN referencefile nx
      ON nx.reference_id = sp.reference_id
     AND nx.display_name = sp.display_name
    WHERE lower(sp.file_class) = 'supplement'
      AND lower(sp.file_extension) = 'pdf'
      AND sp.display_name IS NOT NULL
      AND lower(nx.file_extension) = 'nxml'
    ORDER BY sp.reference_id
    """
    if limit is not None:
        q += f" LIMIT {int(limit)}"
    rs = db.execute(text(q)).fetchall()
    return [int(r[0]) for r in rs]


def repair_nxml_named_supplement_pdfs(  # pragma: no cover
    db,
    reference_ids: Set[int],
    null_mod_cache: Set[int],
) -> Tuple[int, int]:
    """
    Batch-prefetch repair:
      - If ref has a main/final/pdf with mod_id NULL: delete ALL matching NXML-named supplement PDFs.
      - Else: promote ONE matching supplement PDF to main/final/pdf, delete the rest, ensure mod_id NULL assoc.
    Returns: (removed_count, promoted_count)
    """
    if not reference_ids:
        return (0, 0)

    removed = 0
    promoted = 0

    # 1) Which reference_ids already have a proper main?
    main_rows = db.execute(
        text(
            """
            SELECT DISTINCT rf.reference_id
            FROM referencefile rf
            INNER JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
            WHERE rf.reference_id = ANY(:ref_ids)
              AND rfm.mod_id IS NULL
              AND lower(rf.file_class) = 'main'
              AND lower(rf.file_extension) = 'pdf'
              AND lower(rf.file_publication_status) = 'final'
            """
        ),
        {"ref_ids": list(reference_ids)},
    ).fetchall()
    refs_with_main = {int(r[0]) for r in main_rows}

    # 2) Fetch ALL NXML-named supplement PDFs in one query:
    #    These are the supplement PDF rows whose display_name matches an NXML display_name.
    supp_rows = db.execute(
        text(
            """
            SELECT sp.reference_id, sp.referencefile_id, sp.md5sum
            FROM referencefile sp
            INNER JOIN referencefile nx
              ON nx.reference_id = sp.reference_id
             AND nx.display_name = sp.display_name
            WHERE sp.reference_id = ANY(:ref_ids)
              AND lower(sp.file_class) = 'supplement'
              AND lower(sp.file_extension) = 'pdf'
              AND sp.display_name IS NOT NULL
              AND lower(nx.file_extension) = 'nxml'
            ORDER BY sp.reference_id, sp.referencefile_id
            """
        ),
        {"ref_ids": list(reference_ids)},
    ).fetchall()

    by_ref: Dict[int, List[Tuple[int, str]]] = {}
    for ref_id, rf_id, md5 in supp_rows:
        by_ref.setdefault(int(ref_id), []).append((int(rf_id), str(md5)))

    for ref_id in sorted(reference_ids):
        rows = by_ref.get(ref_id, [])
        if not rows:
            continue

        if ref_id in refs_with_main:
            # main exists -> delete all matching supplements
            for rf_id, _md5 in rows:
                obj = db.get(ReferencefileModel, rf_id)
                if obj is not None:
                    db.delete(obj)
                    removed += 1
                null_mod_cache.discard(rf_id)
            continue

        # no main exists -> promote ONE, delete rest
        first = True
        for rf_id, _md5 in rows:
            if first:
                if promote_referencefile_to_main(db, rf_id):
                    promoted += 1
                if rf_id not in null_mod_cache and not has_null_mod_assoc(db, rf_id):
                    insert_referencefile_mod(db, rf_id)
                    null_mod_cache.add(rf_id)
                first = False
            else:
                obj = db.get(ReferencefileModel, rf_id)
                if obj is not None:
                    db.delete(obj)
                    removed += 1
                null_mod_cache.discard(rf_id)

    return removed, promoted


# -----------------------------
# Worker: ensure local PDF exists
# -----------------------------
def process_single_item(args: Tuple[int, int, str, bool, str]) -> Dict:
    """
    Ensures a local PDF exists:
      - If already on disk: mark downloaded=True, already_downloaded=True
      - Else:
          * dry-run: HEAD check to report availability (no download)
          * non-dry-run: attempt download (GET); availability derived from success
    """
    index, reference_id, pmcid, dry_run, output_dir = args
    pmc_id = normalize_pmcid(pmcid)
    pdf_path = os.path.join(output_dir, f"{pmc_id}.pdf")

    out = {
        "index": index,
        "reference_id": reference_id,
        "pmcid": pmcid,
        "pdf_path": pdf_path,
        "available": False,
        "downloaded": False,
        "already_downloaded": False,
        "md5sum": None,
        "uploaded": None,
        "db_loaded": None,
        "promoted": False,
        "error": "",
    }

    if os.path.exists(pdf_path):
        out["available"] = True
        out["downloaded"] = True
        out["already_downloaded"] = True
        return out

    try:
        if dry_run:
            out["available"] = check_pdf_available(pmcid)
            return out

        # Non-dry-run: avoid extra HEAD; download attempt is availability check
        if download_pdf_by_pmcid(pmcid, pdf_path):
            out["available"] = True
            out["downloaded"] = True
        return out

    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
        return out


# -----------------------------
# Main
# -----------------------------
def main() -> None:  # noqa: C901  # pragma: no cover
    ap = argparse.ArgumentParser(description="Download OA PDFs from Europe PMC, upload to S3, and load DB metadata")
    ap.add_argument("--dry-run", action="store_true", help="Only check OA + availability; do not download/upload/db")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of PMCIDs to process")
    ap.add_argument("--output-dir", type=str, default="pmc_pdfs_oa", help="Directory to save downloaded PDFs")
    ap.add_argument("--workers", type=int, default=20, help="Parallel download workers")
    ap.add_argument("--chunksize", type=int, default=100, help="Thread map chunksize")

    # main-loop commits: still helpful to keep transactions bounded, but safe now due to per-item savepoints
    ap.add_argument("--commit-every", type=int, default=250, help="DB commit batch size (main loop)")

    ap.add_argument("--oa-batch-size", type=int, default=100, help="PMCIDs per OA metadata request")
    ap.add_argument("--oa-cache", type=str, default="europepmc_oa_cache.json", help="JSON cache for OA metadata")
    ap.add_argument("--oa-sleep", type=float, default=0.1, help="Sleep between OA batches")

    ap.add_argument(
        "--require-has-pdf",
        action="store_true",
        default=True,
        help="Require hasPDF=='Y' in addition to OA (default True)",
    )
    ap.add_argument(
        "--no-require-has-pdf",
        dest="require_has_pdf",
        action="store_false",
        help="Do not require hasPDF=='Y' (OA only)",
    )

    # DB-wide repair step toggles
    ap.add_argument(
        "--repair-nxml-supplement-pdfs",
        action="store_true",
        default=True,
        help="Repair DB-wide cases where supplement PDF display_name == NXML display_name (default True)",
    )
    ap.add_argument(
        "--no-repair-nxml-supplement-pdfs",
        dest="repair_nxml_supplement_pdfs",
        action="store_false",
        help="Disable the DB-wide repair step",
    )
    ap.add_argument(
        "--repair-limit",
        type=int,
        default=None,
        help="Optional limit on number of reference_ids to repair in the DB-wide scan",
    )
    ap.add_argument(
        "--repair-batch-size",
        type=int,
        default=1000,
        help="Number of reference_ids per repair batch (prefetch happens per batch)",
    )
    ap.add_argument(
        "--repair-commit-every",
        type=int,
        default=5000,
        help="Commit every N repaired reference_ids (best-effort; independent of batch size)",
    )

    args = ap.parse_args()

    env_state = os.environ.get("ENV_STATE")
    upload_and_db_allowed = (env_state is not None and env_state != "test")

    if not args.dry_run:
        os.makedirs(args.output_dir, exist_ok=True)

    logger.info("Querying DB for PMCIDs without main PDFs...")
    candidates = get_pmcids_without_main_pdf(args.limit)
    if not candidates:
        logger.info("No PMCIDs to process.")
        return

    # OA metadata filter
    cache_path = Path(args.oa_cache)
    cache = load_cache(cache_path)

    pmcids_norm = [normalize_pmcid(pmc) for (_, pmc) in candidates]
    unique_pmcids = sorted(set(pmcids_norm))
    # Include PMCIDs not in cache OR those with fetch_error=True (transient failures)
    missing = [p for p in unique_pmcids if p not in cache or cache.get(p, {}).get("fetch_error")]

    logger.info(
        f"OA cache: {cache_path} (entries={len(cache)}); "
        f"unique pmcids={len(unique_pmcids)}; missing={len(missing)}"
    )

    session = requests.Session()
    session.headers.update({"User-Agent": "agr-europepmc-oa-download-upload-load/1.3"})

    fetched = 0
    try:
        for oa_batch in chunked(missing, args.oa_batch_size):
            try:
                batch_meta = fetch_batch_core(oa_batch, session=session)
            except Exception as e:
                logger.error(f"OA fetch failed for batch (size={len(oa_batch)}): {type(e).__name__}: {e}")
                # Mark with fetch_error=True so next run can retry these PMCIDs.
                # We do NOT cache them as hit=False (which would prevent retry).
                for pmcid in oa_batch:
                    cache[pmcid] = {
                        "hit": False,
                        "is_open_access": None,
                        "license": None,
                        "has_pdf": None,
                        "in_pmc": None,
                        "fetch_error": True,
                    }
                save_cache(cache_path, cache)
                continue

            for pmcid, meta in batch_meta.items():
                cache[pmcid] = {
                    "hit": meta.hit,
                    "is_open_access": meta.is_open_access,
                    "license": meta.license,
                    "has_pdf": meta.has_pdf,
                    "in_pmc": meta.in_pmc,
                }

            returned = set(batch_meta.keys())
            for pmcid in oa_batch:
                if pmcid not in returned:
                    cache[pmcid] = {
                        "hit": False,
                        "is_open_access": None,
                        "license": None,
                        "has_pdf": None,
                        "in_pmc": None,
                    }

            fetched += len(oa_batch)
            save_cache(cache_path, cache)

            if args.oa_sleep > 0:
                time.sleep(args.oa_sleep)
            if fetched and fetched % (args.oa_batch_size * 10) == 0:
                logger.info(f"Fetched OA metadata for {fetched}/{len(missing)} missing PMCIDs...")

    finally:
        # Ensure cache is persisted even if something unexpected aborts the loop
        try:
            save_cache(cache_path, cache)
        except Exception:
            pass

    oa_items: List[Tuple[int, str]] = []
    skipped_non_oa: List[Tuple[int, str]] = []

    for reference_id, pmcid in candidates:
        c = cache.get(normalize_pmcid(pmcid), {})
        if is_oa_from_cache_entry(c, require_has_pdf=args.require_has_pdf):
            oa_items.append((reference_id, pmcid))
        else:
            skipped_non_oa.append((reference_id, pmcid))

    logger.info(
        f"OA filter: OA={len(oa_items)} skipped_non_oa_or_unknown={len(skipped_non_oa)} "
        f"(require_has_pdf={args.require_has_pdf})"
    )

    if not oa_items:
        logger.info("No OA items to process after OA filter.")
        return

    # Ensure local PDFs exist (threads)
    work = [(i, rid, pmc, args.dry_run, args.output_dir) for i, (rid, pmc) in enumerate(oa_items, 1)]
    total = len(work)

    logger.info(f"Ensuring local PDFs (download if missing) with {args.workers} workers...")
    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        completed = 0
        for r in ex.map(process_single_item, work, chunksize=args.chunksize):
            results.append(r)
            completed += 1
            if completed % 100 == 0 or completed == total:
                logger.info(f"Progress: {completed}/{total} ({100 * completed / total:.1f}%)")

    if args.dry_run:
        logger.info("Dry-run complete (no download/upload/db inserts performed).")
        return

    # Upload + DB load (serial)
    db = create_postgres_session(False)

    removed_bad_supp = 0
    promoted_bad_supp = 0
    inserted_ok = 0
    promoted_ok = 0
    uploaded_ok = 0
    skipped_upload_db = 0

    # Small in-run caches to avoid repeated DB hits
    rf_info_cache: Dict[Tuple[int, str], Optional[RfInfo]] = {}
    null_mod_cache: Set[int] = set()

    try:
        script_nm = Path(__file__).stem
        set_global_user_id(db, script_nm)

        committed = 0

        for r in results:
            if not r.get("downloaded"):
                continue

            pdf_path = r["pdf_path"]
            reference_id = int(r["reference_id"])
            pmcid = str(r["pmcid"])
            display_name = normalize_pmcid(pmcid)

            md5sum = get_md5sum(pdf_path)
            r["md5sum"] = md5sum

            gz_path = gzip_file(pdf_path)
            if not gz_path:
                r["error"] = (r.get("error") or "") + " gzip_failed"
                continue

            upload_status = upload_pdf_file_to_s3(gz_path, md5sum)
            r["uploaded"] = upload_status

            try:
                os.remove(gz_path)
            except OSError:
                pass

            if upload_status is True:
                uploaded_ok += 1

            if not upload_and_db_allowed:
                r["db_loaded"] = None
                skipped_upload_db += 1
                continue

            # Per-item SAVEPOINT so a failure won't rollback prior successful items
            sp = db.begin_nested()
            try:
                key = (reference_id, md5sum)
                info = rf_info_cache.get(key)
                if key not in rf_info_cache:
                    info = fetch_rf_info_by_key(db, reference_id, md5sum)
                    rf_info_cache[key] = info

                if info is None:
                    rf_id = insert_referencefile(db, reference_id, display_name, md5sum)
                    rf_info_cache[key] = RfInfo(
                        referencefile_id=rf_id,
                        file_class=FILE_CLASS,
                        file_extension=FILE_EXTENSION,
                        file_publication_status=FILE_PUBLICATION_STATUS,
                        pdf_type=PDF_TYPE,
                        display_name=display_name,
                    )
                    inserted_ok += 1
                    logger.info(f"{pmcid}: inserted referencefile (id={rf_id})")
                else:
                    rf_id = info.referencefile_id
                    if needs_promotion(info):
                        if promote_referencefile_to_main(db, rf_id):
                            # refresh cached info to reflect new state
                            info.file_class = FILE_CLASS
                            info.file_extension = FILE_EXTENSION
                            info.file_publication_status = FILE_PUBLICATION_STATUS
                            info.pdf_type = PDF_TYPE
                            r["promoted"] = True
                            promoted_ok += 1
                            logger.info(f"{pmcid}: promoted existing referencefile to main (id={rf_id})")

                if rf_id not in null_mod_cache:
                    if not has_null_mod_assoc(db, rf_id):
                        insert_referencefile_mod(db, rf_id)
                        logger.info(f"{pmcid}: inserted referencefile_mod (mod_id NULL)")
                    null_mod_cache.add(rf_id)

                r["db_loaded"] = True
                sp.commit()

                committed += 1
                if args.commit_every > 0 and committed % args.commit_every == 0:
                    db.commit()

            except Exception as e:
                r["db_loaded"] = False
                r["error"] = f"{type(e).__name__}: {e}"
                try:
                    sp.rollback()
                except Exception:
                    pass
                # keep going; outer transaction still holds prior successful items
                continue

        # Commit any remaining main-loop work
        db.commit()

        # DB-wide repair step (NOT limited to this run)
        if upload_and_db_allowed and args.repair_nxml_supplement_pdfs:
            logger.info("Scanning DB for supplement PDFs whose display_name matches NXML display_name...")
            all_refs = find_refs_with_nxml_named_supplement_pdfs(db, limit=args.repair_limit)
            logger.info(f"DB-wide repair candidates: {len(all_refs)}")

            processed = 0
            processed_since_commit = 0
            ref_batch: List[int] = []

            for ref_id in all_refs:
                ref_batch.append(ref_id)
                if len(ref_batch) >= args.repair_batch_size:
                    sp = db.begin_nested()
                    try:
                        rm_ct, pr_ct = repair_nxml_named_supplement_pdfs(
                            db,
                            set(ref_batch),
                            null_mod_cache=null_mod_cache,
                        )
                        removed_bad_supp += rm_ct
                        promoted_bad_supp += pr_ct
                        sp.commit()
                    except Exception as e:
                        logger.error(
                            f"Repair batch failed (ref_id range ~{ref_batch[0]}..{ref_batch[-1]}): "
                            f"{type(e).__name__}: {e}"
                        )
                        try:
                            sp.rollback()
                        except Exception:
                            pass
                        ref_batch = []
                        continue

                    processed += len(ref_batch)
                    processed_since_commit += len(ref_batch)
                    ref_batch = []

                    if args.repair_commit_every > 0 and processed_since_commit >= args.repair_commit_every:
                        db.commit()
                        processed_since_commit = 0

                    if args.repair_commit_every > 0 and processed % args.repair_commit_every == 0:
                        logger.info(f"Repair progress: {processed}/{len(all_refs)}")

            if ref_batch:
                sp = db.begin_nested()
                try:
                    rm_ct, pr_ct = repair_nxml_named_supplement_pdfs(
                        db,
                        set(ref_batch),
                        null_mod_cache=null_mod_cache,
                    )
                    removed_bad_supp += rm_ct
                    promoted_bad_supp += pr_ct
                    sp.commit()
                except Exception as e:
                    logger.error(f"Final repair batch failed: {type(e).__name__}: {e}")
                    try:
                        sp.rollback()
                    except Exception:
                        pass

                processed += len(ref_batch)
                processed_since_commit += len(ref_batch)

            # Final commit for any remaining repairs not yet committed
            db.commit()

    finally:
        db.close()

    downloaded = sum(1 for r in results if r.get("downloaded"))
    available = sum(1 for r in results if r.get("available"))
    already = sum(1 for r in results if r.get("already_downloaded"))
    promoted = sum(1 for r in results if r.get("promoted") is True)

    logger.info("============================================================")
    logger.info("SUMMARY")
    logger.info("============================================================")
    logger.info(f"OA items processed: {len(oa_items)}")
    logger.info(f"Downloaded this run: {downloaded - already}")
    logger.info(f"Skipped (already on disk): {already}")
    logger.info(f"Available at PDF endpoint OR already on disk: {available}")
    logger.info(f"Inserted new referencefile rows: {inserted_ok}")
    logger.info(f"Promoted misclassified existing PDFs to main: {promoted}")
    logger.info(f"Removed misnamed supplement PDFs (matched NXML display_name): {removed_bad_supp}")
    logger.info(f"Promoted misnamed supplement PDFs to main (matched NXML display_name): {promoted_bad_supp}")
    logger.info(f"Skipped non-OA/unknown: {len(skipped_non_oa)}")
    logger.info(f"ENV_STATE={os.environ.get('ENV_STATE')} upload/db allowed={upload_and_db_allowed}")
    logger.info("Done.")


if __name__ == "__main__":
    main()
