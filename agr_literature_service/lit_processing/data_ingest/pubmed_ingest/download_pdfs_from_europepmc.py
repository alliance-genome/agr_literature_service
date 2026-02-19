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
- Downloaded PDFs are kept in --output-dir after upload (for re-runs that skip download).
  This means disk usage grows across runs. Monitor or periodically clean up --output-dir.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, List, Optional, Set, Tuple

import gzip
import requests
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

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

# Europe PMC API maximum page size
EUROPEPMC_MAX_PAGE_SIZE = 1000

FILE_CLASS = "main"
FILE_PUBLICATION_STATUS = "final"
FILE_EXTENSION = "pdf"
PDF_TYPE = "pdf"

# Expected EuroPMC URL patterns (for detecting URL changes)
EUROPEPMC_PDF_URL_PATTERN = "europepmc.org/backend/ptpmcrender.fcgi"
EUROPEPMC_API_URL_PATTERN = "ebi.ac.uk/europepmc/webservices/rest/search"


# -----------------------------
# Error Types for EuroPMC requests
# -----------------------------
class EuropePmcErrorType(Enum):
    """Categorized error types for EuroPMC API/PDF requests."""
    SUCCESS = "success"
    HTTP_404_NOT_FOUND = "http_404_not_found"
    HTTP_403_FORBIDDEN = "http_403_forbidden"
    HTTP_401_UNAUTHORIZED = "http_401_unauthorized"
    HTTP_429_RATE_LIMITED = "http_429_rate_limited"
    HTTP_500_SERVER_ERROR = "http_500_server_error"
    HTTP_502_BAD_GATEWAY = "http_502_bad_gateway"
    HTTP_503_UNAVAILABLE = "http_503_unavailable"
    HTTP_OTHER = "http_other"
    UNEXPECTED_REDIRECT = "unexpected_redirect"
    WRONG_CONTENT_TYPE = "wrong_content_type"
    CONNECTION_ERROR = "connection_error"
    TIMEOUT = "timeout"
    SSL_ERROR = "ssl_error"
    INVALID_RESPONSE_FORMAT = "invalid_response_format"
    EMPTY_RESPONSE = "empty_response"
    UNKNOWN = "unknown"


@dataclass
class EuropePmcErrorStats:
    """Thread-safe tracking of EuroPMC request errors for surfacing issues."""
    _lock: Lock = field(default_factory=Lock, repr=False)
    pdf_errors: Counter = field(default_factory=Counter)
    api_errors: Counter = field(default_factory=Counter)
    sample_errors: Dict[str, List[str]] = field(default_factory=dict)
    max_samples_per_type: int = 5

    def record_pdf_error(self, error_type: EuropePmcErrorType, pmcid: str, details: str = "") -> None:
        """Record a PDF download error with optional details."""
        with self._lock:
            self.pdf_errors[error_type.value] += 1
            key = f"pdf_{error_type.value}"
            if key not in self.sample_errors:
                self.sample_errors[key] = []
            if len(self.sample_errors[key]) < self.max_samples_per_type:
                self.sample_errors[key].append(f"{pmcid}: {details}" if details else pmcid)

    def record_api_error(self, error_type: EuropePmcErrorType, details: str = "") -> None:
        """Record an API metadata request error with optional details."""
        with self._lock:
            self.api_errors[error_type.value] += 1
            key = f"api_{error_type.value}"
            if key not in self.sample_errors:
                self.sample_errors[key] = []
            if len(self.sample_errors[key]) < self.max_samples_per_type:
                self.sample_errors[key].append(details)

    def has_errors(self) -> bool:
        """Return True if any errors were recorded."""
        return bool(self.pdf_errors) or bool(self.api_errors)

    def get_summary(self) -> str:
        """Generate a summary of all recorded errors."""
        lines = []
        if self.pdf_errors:
            lines.append("PDF Download Errors:")
            for error_type, count in sorted(self.pdf_errors.items()):
                lines.append(f"  {error_type}: {count}")
                key = f"pdf_{error_type}"
                if key in self.sample_errors and self.sample_errors[key]:
                    lines.append(f"    Samples: {self.sample_errors[key][:3]}")

        if self.api_errors:
            lines.append("API Metadata Errors:")
            for error_type, count in sorted(self.api_errors.items()):
                lines.append(f"  {error_type}: {count}")
                key = f"api_{error_type}"
                if key in self.sample_errors and self.sample_errors[key]:
                    lines.append(f"    Samples: {self.sample_errors[key][:3]}")

        return "\n".join(lines) if lines else "No errors recorded."

    def check_for_systemic_issues(self, total_pdf_attempts: int, total_api_attempts: int) -> List[str]:
        """Check for patterns indicating systemic issues (URL changes, rate limiting, etc.)."""
        warnings = []

        # Check for high 404 rate (possible URL change)
        if total_pdf_attempts > 0:
            not_found_rate = self.pdf_errors.get("http_404_not_found", 0) / total_pdf_attempts
            if not_found_rate > 0.5 and self.pdf_errors.get("http_404_not_found", 0) > 10:
                warnings.append(
                    f"HIGH 404 RATE ({not_found_rate:.1%}): EuroPMC PDF URL pattern may have changed. "
                    f"Current pattern: {EUROPEPMC_PDF_URL_PATTERN}"
                )

        # Check for 403/401 (authentication/access issues)
        auth_errors = (
            self.pdf_errors.get("http_403_forbidden", 0)
            + self.pdf_errors.get("http_401_unauthorized", 0)
            + self.api_errors.get("http_403_forbidden", 0)
            + self.api_errors.get("http_401_unauthorized", 0)
        )
        if auth_errors > 5:
            warnings.append(
                f"ACCESS DENIED ({auth_errors} errors): EuroPMC may have added authentication requirements "
                "or IP restrictions."
            )

        # Check for rate limiting
        rate_limit_errors = (
            self.pdf_errors.get("http_429_rate_limited", 0)
            + self.api_errors.get("http_429_rate_limited", 0)
        )
        if rate_limit_errors > 0:
            warnings.append(
                f"RATE LIMITED ({rate_limit_errors} errors): Consider reducing --workers or adding --download-sleep."
            )

        # Check for unexpected redirects (URL change indicator)
        redirect_errors = self.pdf_errors.get("unexpected_redirect", 0)
        if redirect_errors > 5:
            warnings.append(
                f"UNEXPECTED REDIRECTS ({redirect_errors}): EuroPMC PDF URLs may be redirecting to a new location."
            )

        # Check for wrong content type (API returning HTML error pages)
        wrong_content = (
            self.pdf_errors.get("wrong_content_type", 0)
            + self.api_errors.get("wrong_content_type", 0)
        )
        if wrong_content > 10:
            warnings.append(
                f"WRONG CONTENT TYPE ({wrong_content}): API may be returning error pages instead of expected content."
            )

        # Check for server errors (EuroPMC infrastructure issues)
        server_errors = sum([
            self.pdf_errors.get("http_500_server_error", 0),
            self.pdf_errors.get("http_502_bad_gateway", 0),
            self.pdf_errors.get("http_503_unavailable", 0),
            self.api_errors.get("http_500_server_error", 0),
            self.api_errors.get("http_502_bad_gateway", 0),
            self.api_errors.get("http_503_unavailable", 0),
        ])
        if server_errors > 10:
            warnings.append(
                f"SERVER ERRORS ({server_errors}): EuroPMC may be experiencing infrastructure issues."
            )

        # Check for API response format issues
        if self.api_errors.get("invalid_response_format", 0) > 3:
            warnings.append(
                "API RESPONSE FORMAT CHANGED: EuroPMC API may have changed its response structure."
            )

        return warnings


# Global error stats instance (initialized per run in main())
_error_stats: Optional[EuropePmcErrorStats] = None


def get_error_stats() -> EuropePmcErrorStats:
    """Get the global error stats instance."""
    global _error_stats
    if _error_stats is None:
        _error_stats = EuropePmcErrorStats()
    return _error_stats


def reset_error_stats() -> None:
    """Reset error stats for a new run."""
    global _error_stats
    _error_stats = EuropePmcErrorStats()


def classify_request_exception(e: Exception) -> EuropePmcErrorType:
    """Classify a requests exception into an error type."""
    if isinstance(e, requests.exceptions.Timeout):
        return EuropePmcErrorType.TIMEOUT
    elif isinstance(e, requests.exceptions.SSLError):
        return EuropePmcErrorType.SSL_ERROR
    elif isinstance(e, requests.exceptions.ConnectionError):
        return EuropePmcErrorType.CONNECTION_ERROR
    elif isinstance(e, requests.exceptions.HTTPError):
        response = getattr(e, 'response', None)
        if response is not None:
            return classify_http_status(response.status_code)
        return EuropePmcErrorType.HTTP_OTHER
    elif isinstance(e, requests.exceptions.RequestException):
        return EuropePmcErrorType.UNKNOWN
    return EuropePmcErrorType.UNKNOWN


def classify_http_status(status_code: int) -> EuropePmcErrorType:
    """Classify an HTTP status code into an error type."""
    if status_code == 404:
        return EuropePmcErrorType.HTTP_404_NOT_FOUND
    elif status_code == 403:
        return EuropePmcErrorType.HTTP_403_FORBIDDEN
    elif status_code == 401:
        return EuropePmcErrorType.HTTP_401_UNAUTHORIZED
    elif status_code == 429:
        return EuropePmcErrorType.HTTP_429_RATE_LIMITED
    elif status_code == 500:
        return EuropePmcErrorType.HTTP_500_SERVER_ERROR
    elif status_code == 502:
        return EuropePmcErrorType.HTTP_502_BAD_GATEWAY
    elif status_code == 503:
        return EuropePmcErrorType.HTTP_503_UNAVAILABLE
    elif 400 <= status_code < 500:
        return EuropePmcErrorType.HTTP_OTHER
    elif 500 <= status_code < 600:
        return EuropePmcErrorType.HTTP_500_SERVER_ERROR
    return EuropePmcErrorType.HTTP_OTHER


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
    """HEAD-check for dry-run reporting with explicit error tracking."""
    pmc_id = normalize_pmcid(pmcid)
    url = europepmc_pdf_url(pmc_id)
    stats = get_error_stats()

    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)

        # Check for unexpected redirects to different domains
        if r.history:
            final_url = r.url
            if EUROPEPMC_PDF_URL_PATTERN not in final_url:
                stats.record_pdf_error(
                    EuropePmcErrorType.UNEXPECTED_REDIRECT,
                    pmc_id,
                    f"Redirected to: {final_url}"
                )
                logger.warning(
                    f"{pmc_id}: Unexpected redirect from EuroPMC PDF URL to {final_url}"
                )
                return False

        if r.status_code != 200:
            error_type = classify_http_status(r.status_code)
            stats.record_pdf_error(
                error_type,
                pmc_id,
                f"HTTP {r.status_code}"
            )
            logger.debug(f"{pmc_id}: HEAD check returned HTTP {r.status_code}")
            return False

        ct = r.headers.get("Content-Type", "")
        if "application/pdf" not in ct.lower():
            stats.record_pdf_error(
                EuropePmcErrorType.WRONG_CONTENT_TYPE,
                pmc_id,
                f"Content-Type: {ct}"
            )
            logger.debug(f"{pmc_id}: HEAD check returned unexpected Content-Type: {ct}")
            return False

        return True

    except requests.exceptions.Timeout as e:
        stats.record_pdf_error(EuropePmcErrorType.TIMEOUT, pmc_id, str(e))
        logger.debug(f"{pmc_id}: HEAD check timed out: {e}")
        return False
    except requests.exceptions.SSLError as e:
        stats.record_pdf_error(EuropePmcErrorType.SSL_ERROR, pmc_id, str(e))
        logger.warning(f"{pmc_id}: SSL error during HEAD check: {e}")
        return False
    except requests.exceptions.ConnectionError as e:
        stats.record_pdf_error(EuropePmcErrorType.CONNECTION_ERROR, pmc_id, str(e))
        logger.debug(f"{pmc_id}: Connection error during HEAD check: {e}")
        return False
    except requests.RequestException as e:
        error_type = classify_request_exception(e)
        stats.record_pdf_error(error_type, pmc_id, str(e))
        logger.debug(f"{pmc_id}: Request error during HEAD check: {type(e).__name__}: {e}")
        return False


def download_pdf_by_pmcid(pmcid: str, output_path: str, timeout: int = 60) -> bool:
    """
    Never overwrites output_path. Downloads to .tmp then atomic rename.
    Availability is determined by the GET response.
    Tracks errors for detecting URL changes and API issues.
    """
    if os.path.exists(output_path):
        return False

    pmc_id = normalize_pmcid(pmcid)
    url = europepmc_pdf_url(pmc_id)
    tmp_path = output_path + ".tmp"
    stats = get_error_stats()

    try:
        with requests.get(url, timeout=timeout, stream=True) as r:
            # Check for unexpected redirects
            if r.history:
                final_url = r.url
                if EUROPEPMC_PDF_URL_PATTERN not in final_url:
                    stats.record_pdf_error(
                        EuropePmcErrorType.UNEXPECTED_REDIRECT,
                        pmc_id,
                        f"Redirected to: {final_url}"
                    )
                    logger.warning(
                        f"{pmc_id}: Unexpected redirect from EuroPMC PDF URL to {final_url}"
                    )
                    return False

            # Check HTTP status code
            if r.status_code != 200:
                error_type = classify_http_status(r.status_code)
                stats.record_pdf_error(
                    error_type,
                    pmc_id,
                    f"HTTP {r.status_code}"
                )
                if r.status_code in (401, 403, 404, 429, 500, 502, 503):
                    logger.debug(f"{pmc_id}: Download failed with HTTP {r.status_code}")
                return False

            # Validate Content-Type
            ct = r.headers.get("Content-Type", "")
            if "application/pdf" not in ct.lower():
                stats.record_pdf_error(
                    EuropePmcErrorType.WRONG_CONTENT_TYPE,
                    pmc_id,
                    f"Content-Type: {ct}"
                )
                logger.debug(f"{pmc_id}: Expected PDF but got Content-Type: {ct}")
                return False

            # Download the content
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

            # Verify we got actual content
            if os.path.getsize(tmp_path) == 0:
                stats.record_pdf_error(
                    EuropePmcErrorType.EMPTY_RESPONSE,
                    pmc_id,
                    "Downloaded file is empty"
                )
                logger.warning(f"{pmc_id}: Downloaded PDF is empty")
                return False

        os.replace(tmp_path, output_path)
        return True

    except requests.exceptions.Timeout as e:
        stats.record_pdf_error(EuropePmcErrorType.TIMEOUT, pmc_id, str(e))
        logger.debug(f"{pmc_id}: Download timed out: {e}")
        return False
    except requests.exceptions.SSLError as e:
        stats.record_pdf_error(EuropePmcErrorType.SSL_ERROR, pmc_id, str(e))
        logger.warning(f"{pmc_id}: SSL error during download: {e}")
        return False
    except requests.exceptions.ConnectionError as e:
        stats.record_pdf_error(EuropePmcErrorType.CONNECTION_ERROR, pmc_id, str(e))
        logger.debug(f"{pmc_id}: Connection error during download: {e}")
        return False
    except requests.RequestException as e:
        error_type = classify_request_exception(e)
        stats.record_pdf_error(error_type, pmc_id, str(e))
        logger.debug(f"{pmc_id}: Request error during download: {type(e).__name__}: {e}")
        return False
    except OSError as e:
        logger.error(f"{pmc_id}: File system error during download: {e}")
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


def fetch_batch_core(  # noqa: C901
    pmcids: List[str],
    session: requests.Session,
    timeout: int = 60,
    page_size: int = 1000,
) -> Dict[str, PmcMeta]:
    """
    Fetch OA metadata from EuroPMC API with explicit error handling.
    Validates response structure to detect API changes.
    """
    # Ensure page_size can accommodate all PMCIDs in the batch, but cap at API limit
    effective_page_size = min(max(page_size, len(pmcids)), EUROPEPMC_MAX_PAGE_SIZE)
    or_query = " OR ".join([f"PMCID:{p}" for p in pmcids])
    query = f"({or_query})"

    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    params = {"query": query, "resultType": "core", "format": "json", "pageSize": effective_page_size}
    stats = get_error_stats()

    try:
        r = session.get(url, params=params, timeout=timeout)

        # Check for unexpected redirects
        if r.history:
            final_url = r.url
            if EUROPEPMC_API_URL_PATTERN not in final_url:
                stats.record_api_error(
                    EuropePmcErrorType.UNEXPECTED_REDIRECT,
                    f"Redirected to: {final_url}"
                )
                logger.warning(f"EuroPMC API redirected to unexpected URL: {final_url}")

        # Check HTTP status
        if r.status_code != 200:
            error_type = classify_http_status(r.status_code)
            stats.record_api_error(
                error_type,
                f"HTTP {r.status_code} for batch of {len(pmcids)} PMCIDs"
            )
            r.raise_for_status()

        # Validate Content-Type is JSON
        ct = r.headers.get("Content-Type", "")
        if "application/json" not in ct.lower() and "json" not in ct.lower():
            stats.record_api_error(
                EuropePmcErrorType.WRONG_CONTENT_TYPE,
                f"Expected JSON but got Content-Type: {ct}"
            )
            logger.warning(f"EuroPMC API returned unexpected Content-Type: {ct}")

        # Parse JSON response
        try:
            data = r.json()
        except ValueError as e:
            stats.record_api_error(
                EuropePmcErrorType.INVALID_RESPONSE_FORMAT,
                f"JSON parse error: {e}"
            )
            logger.error(f"EuroPMC API returned invalid JSON: {e}")
            raise

        # Validate expected response structure
        if not isinstance(data, dict):
            stats.record_api_error(
                EuropePmcErrorType.INVALID_RESPONSE_FORMAT,
                f"Expected dict, got {type(data).__name__}"
            )
            logger.error(f"EuroPMC API response is not a dict: {type(data).__name__}")
            raise ValueError(f"Invalid API response format: expected dict, got {type(data).__name__}")

        result_list = data.get("resultList")
        if result_list is None:
            # Check if this is an error response
            if "error" in data or "errorMessage" in data:
                error_msg = data.get("error") or data.get("errorMessage") or "Unknown API error"
                stats.record_api_error(
                    EuropePmcErrorType.HTTP_OTHER,
                    f"API error response: {error_msg}"
                )
                logger.error(f"EuroPMC API returned error: {error_msg}")
                raise ValueError(f"API error: {error_msg}")

            stats.record_api_error(
                EuropePmcErrorType.INVALID_RESPONSE_FORMAT,
                "Missing 'resultList' in response"
            )
            logger.warning("EuroPMC API response missing 'resultList' field - API format may have changed")

        results: List[dict] = ((result_list or {}).get("result", []) or []) if result_list else []

        # Warn if we got fewer results than expected (might indicate API issues)
        if len(results) == 0 and len(pmcids) > 0:
            logger.debug(f"EuroPMC API returned 0 results for {len(pmcids)} PMCIDs (may all be non-OA)")

        out: Dict[str, PmcMeta] = {}

        for res in results:
            if not isinstance(res, dict):
                stats.record_api_error(
                    EuropePmcErrorType.INVALID_RESPONSE_FORMAT,
                    f"Result item is not a dict: {type(res).__name__}"
                )
                continue

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

    except requests.exceptions.Timeout as e:
        stats.record_api_error(EuropePmcErrorType.TIMEOUT, str(e))
        raise
    except requests.exceptions.SSLError as e:
        stats.record_api_error(EuropePmcErrorType.SSL_ERROR, str(e))
        raise
    except requests.exceptions.ConnectionError as e:
        stats.record_api_error(EuropePmcErrorType.CONNECTION_ERROR, str(e))
        raise
    except requests.exceptions.HTTPError:
        # Already recorded above, just re-raise
        raise
    except requests.RequestException as e:
        error_type = classify_request_exception(e)
        stats.record_api_error(error_type, str(e))
        raise


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
def process_single_item(args: Tuple[int, int, str, bool, str, float]) -> Dict:
    """
    Ensures a local PDF exists:
      - If already on disk: mark downloaded=True, already_downloaded=True
      - Else:
          * dry-run: HEAD check to report availability (no download)
          * non-dry-run: attempt download (GET); availability derived from success
    """
    index, reference_id, pmcid, dry_run, output_dir, download_sleep = args
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

        # Rate limiting for PDF downloads
        if download_sleep > 0:
            time.sleep(download_sleep)

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

    ap.add_argument(
        "--oa-batch-size", type=int, default=100,
        help=f"PMCIDs per OA metadata request (max {EUROPEPMC_MAX_PAGE_SIZE})"
    )
    ap.add_argument("--oa-cache", type=str, default="europepmc_oa_cache.json", help="JSON cache for OA metadata")
    ap.add_argument("--oa-sleep", type=float, default=0.1, help="Sleep between OA batches")
    ap.add_argument("--download-sleep", type=float, default=0.0, help="Sleep between PDF downloads (rate limiting)")

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

    # Reset error stats for this run
    reset_error_stats()

    # Validate oa_batch_size doesn't exceed Europe PMC's max page size
    if args.oa_batch_size > EUROPEPMC_MAX_PAGE_SIZE:
        logger.warning(
            f"--oa-batch-size ({args.oa_batch_size}) exceeds Europe PMC max page size "
            f"({EUROPEPMC_MAX_PAGE_SIZE}). Results may be truncated."
        )

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

    # Deduplicate by PMCID for download phase to avoid race conditions:
    # Multiple reference_ids can share the same PMCID, causing concurrent
    # workers to write to the same temp file. We download once per unique PMCID.
    seen_pmcids: Set[str] = set()
    deduped_for_download: List[Tuple[int, str]] = []
    for reference_id, pmcid in oa_items:
        norm = normalize_pmcid(pmcid)
        if norm not in seen_pmcids:
            deduped_for_download.append((reference_id, pmcid))
            seen_pmcids.add(norm)

    logger.info(
        f"Unique PMCIDs for download: {len(deduped_for_download)} "
        f"(total OA items including duplicates: {len(oa_items)})"
    )

    # Ensure local PDFs exist (threads) - uses deduplicated list
    work = [
        (i, rid, pmc, args.dry_run, args.output_dir, args.download_sleep)
        for i, (rid, pmc) in enumerate(deduped_for_download, 1)
    ]
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
        # Report any errors encountered during dry-run availability checks
        stats = get_error_stats()
        if stats.has_errors():
            logger.info("============================================================")
            logger.info("ERROR STATISTICS (dry-run)")
            logger.info("============================================================")
            logger.info(stats.get_summary())

            # Check for systemic issues
            total_pdf_attempts = len(deduped_for_download)
            warnings = stats.check_for_systemic_issues(total_pdf_attempts, 0)
            if warnings:
                logger.info("============================================================")
                logger.warning("POTENTIAL ISSUES DETECTED")
                logger.info("============================================================")
                for warning in warnings:
                    logger.warning(warning)

        logger.info("Dry-run complete (no download/upload/db inserts performed).")
        return

    # Build a map of normalized PMCID -> download result for lookup
    # This allows us to process ALL oa_items (including duplicate PMCIDs) for DB load
    pmcid_to_result: Dict[str, Dict] = {}
    for r in results:
        if r.get("downloaded"):
            norm = normalize_pmcid(r["pmcid"])
            pmcid_to_result[norm] = r

    # Upload phase: gzip and upload each unique PDF once
    # Cache md5sum per PMCID so we don't recompute for duplicate reference_ids
    pmcid_to_md5: Dict[str, str] = {}

    for norm_pmcid, r in pmcid_to_result.items():
        pdf_path = r["pdf_path"]
        md5sum = get_md5sum(pdf_path)
        r["md5sum"] = md5sum
        pmcid_to_md5[norm_pmcid] = md5sum

        gz_path = gzip_file(pdf_path)
        if not gz_path:
            r["error"] = (r.get("error") or "") + " gzip_failed"
            r["upload_failed"] = True
            continue

        upload_status = upload_pdf_file_to_s3(gz_path, md5sum)
        r["uploaded"] = upload_status

        try:
            os.remove(gz_path)
        except OSError:
            pass

    uploaded_ok = sum(1 for r in pmcid_to_result.values() if r.get("uploaded") is True)

    # DB load (serial) - process ALL oa_items including duplicate PMCIDs
    db = create_postgres_session(False)

    removed_bad_supp = 0
    promoted_bad_supp = 0
    inserted_ok = 0
    promoted_ok = 0
    skipped_upload_db = 0

    # Small in-run caches to avoid repeated DB hits
    rf_info_cache: Dict[Tuple[int, str], Optional[RfInfo]] = {}
    null_mod_cache: Set[int] = set()

    try:
        script_nm = Path(__file__).stem
        set_global_user_id(db, script_nm)

        committed = 0

        # Process ALL oa_items for DB load (not just deduplicated download results)
        for reference_id, pmcid in oa_items:
            norm_pmcid = normalize_pmcid(pmcid)
            download_result = pmcid_to_result.get(norm_pmcid)

            if download_result is None or download_result.get("upload_failed"):
                continue

            if not upload_and_db_allowed:
                skipped_upload_db += 1
                continue

            md5sum = pmcid_to_md5.get(norm_pmcid)
            if not md5sum:
                continue

            display_name = norm_pmcid

            # Per-item SAVEPOINT so a failure won't rollback prior successful items
            sp = db.begin_nested()
            try:
                key = (reference_id, md5sum)
                info = rf_info_cache.get(key)
                if key not in rf_info_cache:
                    info = fetch_rf_info_by_key(db, reference_id, md5sum)
                    rf_info_cache[key] = info

                if info is None:
                    try:
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
                        logger.info(f"{pmcid} (ref={reference_id}): inserted referencefile (id={rf_id})")
                    except IntegrityError as ie:
                        # Could be display_name or md5sum constraint - log actual error
                        logger.warning(
                            f"{pmcid} (ref={reference_id}): skipped insert due to IntegrityError: {ie}"
                        )
                        sp.rollback()
                        continue
                else:
                    rf_id = info.referencefile_id
                    if needs_promotion(info):
                        if promote_referencefile_to_main(db, rf_id):
                            # refresh cached info to reflect new state
                            info.file_class = FILE_CLASS
                            info.file_extension = FILE_EXTENSION
                            info.file_publication_status = FILE_PUBLICATION_STATUS
                            info.pdf_type = PDF_TYPE
                            promoted_ok += 1
                            logger.info(
                                f"{pmcid} (ref={reference_id}): promoted existing referencefile to main (id={rf_id})"
                            )

                if rf_id not in null_mod_cache:
                    if not has_null_mod_assoc(db, rf_id):
                        insert_referencefile_mod(db, rf_id)
                        logger.info(f"{pmcid} (ref={reference_id}): inserted referencefile_mod (mod_id NULL)")
                    null_mod_cache.add(rf_id)

                sp.commit()

                committed += 1
                if args.commit_every > 0 and committed % args.commit_every == 0:
                    db.commit()

            except Exception as e:
                logger.error(f"{pmcid} (ref={reference_id}): DB error: {type(e).__name__}: {e}")
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
                            f"Repair batch failed ({len(ref_batch)} ref_ids lost): "
                            f"{type(e).__name__}: {e}; ref_ids={ref_batch}"
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

                    # Log progress every batch for visibility
                    logger.info(f"Repair progress: {processed}/{len(all_refs)}")

                    if args.repair_commit_every > 0 and processed_since_commit >= args.repair_commit_every:
                        db.commit()
                        processed_since_commit = 0

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
                    logger.error(
                        f"Final repair batch failed ({len(ref_batch)} ref_ids lost): "
                        f"{type(e).__name__}: {e}; ref_ids={ref_batch}"
                    )
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

    logger.info("============================================================")
    logger.info("SUMMARY")
    logger.info("============================================================")
    logger.info(f"OA items (reference_id, pmcid pairs): {len(oa_items)}")
    logger.info(f"Unique PMCIDs for download: {len(deduped_for_download)}")
    logger.info(f"Downloaded this run: {downloaded - already}")
    logger.info(f"Skipped (already on disk): {already}")
    logger.info(f"Available at PDF endpoint OR already on disk: {available}")
    logger.info(f"Uploaded to S3: {uploaded_ok}")
    logger.info(f"Skipped upload/DB (ENV_STATE missing or test): {skipped_upload_db}")
    logger.info(f"Inserted new referencefile rows: {inserted_ok}")
    logger.info(f"Promoted misclassified existing PDFs to main: {promoted_ok}")
    logger.info(f"Removed misnamed supplement PDFs (matched NXML display_name): {removed_bad_supp}")
    logger.info(f"Promoted misnamed supplement PDFs to main (matched NXML display_name): {promoted_bad_supp}")
    logger.info(f"Skipped non-OA/unknown: {len(skipped_non_oa)}")
    logger.info(f"ENV_STATE={os.environ.get('ENV_STATE')} upload/db allowed={upload_and_db_allowed}")

    # Report error statistics and check for systemic issues
    stats = get_error_stats()
    if stats.has_errors():
        logger.info("============================================================")
        logger.info("ERROR STATISTICS")
        logger.info("============================================================")
        logger.info(stats.get_summary())

        # Check for systemic issues that might indicate URL/API changes
        total_pdf_attempts = len(deduped_for_download)
        total_api_attempts = len(missing) // args.oa_batch_size + (1 if len(missing) % args.oa_batch_size else 0)
        warnings = stats.check_for_systemic_issues(total_pdf_attempts, total_api_attempts)

        if warnings:
            logger.info("============================================================")
            logger.warning("POTENTIAL ISSUES DETECTED")
            logger.info("============================================================")
            for warning in warnings:
                logger.warning(warning)

    logger.info("Done.")


if __name__ == "__main__":
    main()
