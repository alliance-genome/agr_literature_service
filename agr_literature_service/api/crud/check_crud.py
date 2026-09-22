"""
check_crud.py

This "crud" only allows get, it is not intended to chage any setting via this code.

General checks, that may be useful for debugging etc.
So maybe not useful for general users but it is unlikely they would use the
swagger interface so should be fine.
Also test for Ateam api, but more could be added.
==============
"""
import json
import re
import urllib.request
from glob import glob
from os import environ, path
from collections import defaultdict
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from agr_cognito_py import get_authentication_token

import logging

logger = logging.getLogger(__name__)


def check_ateam_api():
    token = get_authentication_token()
    ateam_api_base_url = environ.get('ATEAM_API_URL')
    ateam_health = ateam_api_base_url.replace('api', 'health')
    try:
        request = urllib.request.Request(url=ateam_health)
        request.add_header("Authorization", f"Bearer {token}")
        request.add_header("Content-type", "application/json")
        request.add_header("Accept", "application/json")
        with urllib.request.urlopen(request) as response:
            resp = response.read().decode("utf8")
            resp_obj = json.loads(resp)
            return resp_obj
    except Exception as e:
        logger.error(f"Exception checking ateam api: {e}")
        # return {}
        return {"status": "SKIPPED until A-team cognito authentication is in place"}


def check_database(db: Session):
    res = {}
    query = "select version_num from alembic_version"
    try:
        rows = db.execute(text(query)).fetchall()
        alembic_version = []
        for row in rows:
            alembic_version.append(row[0])
            print(row[0])
        if len(alembic_version) != 1:
            res['alembic_version'] = f"Problem we do not have 1 value we have: {alembic_version}"
        else:
            res['alembic_version'] = alembic_version[0]
    except Exception as e:
        res['alembic_version'] = f"Unable to query database for alembic version: {e}"

    query = "select count(1) from reference"
    try:
        rows = db.execute(text(query)).fetchall()
        # ref_count = rows[0]
        res['ref_count'] = rows[0][0]
    except Exception as e:
        res['ref_count'] = f"Unable to query database for number of references: {e}"

    return res


# QC report keys this API serves, mapped to the filename stem the generator
# scripts under lit_processing/data_check write into ${LOG_PATH}/QC/. Every run
# writes a stable "<stem>.log" plus a dated copy "<stem>_YYYYMMDD.log"; those
# dated copies are the history this whitelist makes reachable.
QC_REPORTS = {
    "obsolete_entities": "obsolete_entity_report",
    "redacted_references": "redacted_references_with_tags",
    "obsolete_pmids": "obsolete_pmid_report",
    "duplicate_orcids": "duplicate_orcid_report",
}

# Explicit ASCII digits and fullmatch, not r'^\d{8}$': \d is Unicode-aware and
# $ tolerates a trailing newline, so the looser form accepts strings this code
# then describes as "eight digits". Nothing that matches either form can escape
# the report directory, but the strict form is what the callers below claim.
DATESTAMP_RE = re.compile(r'[0-9]{8}')

DATE_PRODUCED_MARKER = 'date-produced:'


def _qc_report_stem(report_key: str) -> str:
    """Filename stem for a report key, refusing anything not whitelisted.

    The key is compared, never used to index, and the stem returned is a literal
    out of QC_REPORTS - so no caller-supplied string is carried into a filename,
    not even through a dict lookup. That is also what keeps the path-injection
    analysis satisfied rather than merely satisfiable by argument.
    """
    for known_key, stem in QC_REPORTS.items():
        if known_key == report_key:
            return stem
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Unknown QC report '{report_key}'. Expected one of: "
                               f"{', '.join(sorted(QC_REPORTS))}.")


def _parse_date_produced(line: str) -> Optional[str]:
    """The datestamp out of a "#!date-produced: YYYYMMDD" header line.

    Split on the marker without its trailing space, so the test and the split
    agree on one token: a hand-written "#!date-produced:20260101" would
    otherwise pass a check for the marker and then raise IndexError on the
    split, which reaches the caller as a 500.
    """
    if DATE_PRODUCED_MARKER not in line:
        return None
    datestamp = line.split(DATE_PRODUCED_MARKER, 1)[1].strip()
    return datestamp if DATESTAMP_RE.fullmatch(datestamp) else None


def _read_date_produced(log_file: str) -> Optional[str]:
    """The date one QC log claims for itself, from its first line."""
    try:
        with open(log_file, 'r') as f:
            first_line = f.readline()
    except OSError:
        return None
    return _parse_date_produced(first_line)


def _qc_log_files(stem: str, log_path: str) -> Dict[Optional[str], str]:
    """Every log on disk for one QC report, keyed by datestamp.

    The ``None`` key is the undated "<stem>.log", which is the current run; the
    rest are its datestamped archives.

    Every path here is either built from constants - the stem is a QC_REPORTS
    literal, log_path is configuration - or handed over by the directory listing
    itself. Nothing from a request is ever interpolated into a filename: a
    caller's datestamp only ever gets compared against keys collected from disk,
    which is why a separator or a ".." segment has nothing to act on.
    """
    files: Dict[Optional[str], str] = {}

    latest_file = path.join(log_path, f"QC/{stem}.log")
    if path.isfile(latest_file):
        files[None] = latest_file

    for archived_file in glob(path.join(log_path, f"QC/{stem}_*.log")):
        suffix = path.basename(archived_file)[len(stem) + 1:-len('.log')]
        if DATESTAMP_RE.fullmatch(suffix):
            files[suffix] = archived_file

    return files


def qc_report_runs(report_key: str) -> Dict[str, Any]:
    """Every run of one QC report on this host, as one snapshot.

    ``dates`` lists the datestamps newest first, covering both the archived
    copies and the current run. The current run is the undated "<stem>.log", so
    listing only the datestamped filenames would leave the newest report out
    whenever its dated copy is absent - and a caller defaulting to the first
    entry would then show stale data.

    ``has_latest`` says whether that current run is present at all and
    ``latest`` names its date when it has a readable header, which is the entry
    of ``dates`` it accounts for. The two differ for a log written by hand: it is
    present and readable but carries no header, so it cannot be dated. Callers
    must tell that apart from there being no current run, because the first is
    still worth offering and the second is not.

    A missing LOG_PATH/QC directory is not an error - it only means this host has
    no reports yet - so this comes back empty rather than 404ing, and the caller
    falls back to reading the latest report directly.
    """
    stem = _qc_report_stem(report_key)
    files = _qc_log_files(stem, environ.get('LOG_PATH', '.'))

    latest_file = files.get(None)
    latest = _read_date_produced(latest_file) if latest_file else None

    datestamps = {datestamp for datestamp in files if datestamp is not None}
    if latest:
        datestamps.add(latest)

    return {
        "dates": sorted(datestamps, reverse=True),
        "latest": latest,
        "has_latest": latest_file is not None
    }


def list_qc_report_dates(report_key: str) -> List[str]:
    """Datestamps of the runs of one QC report, newest first."""
    return qc_report_runs(report_key)["dates"]


def qc_latest_datestamp(report_key: str) -> Optional[str]:
    """The date the current, undated run of one QC report claims for itself."""
    return qc_report_runs(report_key)["latest"]


def qc_latest_exists(report_key: str) -> bool:
    """Whether the current, undated run of one QC report is on disk."""
    return qc_report_runs(report_key)["has_latest"]


def _resolve_qc_log(report_key: str, datestamp: Optional[str] = None) -> str:
    """Path of one QC report file: a dated archive, or the current run.

    The path returned is always one the directory listing produced, picked out
    by comparing the caller's datestamp against the dates found there - the
    request never contributes characters to a filename. The eight-digit check
    stays as well, so a malformed datestamp is answered with a clear 400 rather
    than an indistinguishable 404.

    Omitting the datestamp reads the undated file. A datestamp with no archived
    copy still resolves to that same file when its header reports that date,
    because the undated file may be the only copy of that run left on the host -
    which is why the newest date a caller sees listed can be one that exists
    nowhere in a filename.
    """
    stem = _qc_report_stem(report_key)
    files = _qc_log_files(stem, environ.get('LOG_PATH', '.'))
    latest_file = files.get(None)

    if not datestamp:
        log_file = latest_file
    else:
        if not DATESTAMP_RE.fullmatch(datestamp):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="datestamp must be 8 digits in YYYYMMDD form.")
        log_file = files.get(datestamp)
        if log_file is None and latest_file and _read_date_produced(latest_file) == datestamp:
            log_file = latest_file

    if log_file is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"No {report_key} report for {datestamp or 'the latest run'}.")
    return log_file


def check_obsolete_entities(datestamp: Optional[str] = None):

    log_file = _resolve_qc_log("obsolete_entities", datestamp)
    date_produced = None
    data = defaultdict(list)

    with open(log_file, 'r') as f:
        for line in f:
            if DATE_PRODUCED_MARKER in line:
                # Split on the marker without its trailing space, so a header
                # written by hand as "#!date-produced:20260101" cannot pass this
                # test and then raise IndexError on the split. Left permissive
                # about the value itself, which is only echoed back to the
                # caller - unlike the datestamps offered as selectable runs,
                # which _parse_date_produced holds to eight digits.
                date_produced = line.split(DATE_PRODUCED_MARKER, 1)[1].strip()
            else:
                pieces = line.strip().split('\t')
                if len(pieces) > 6:
                    reference_curies_raw = pieces[6] if len(pieces) > 6 else ''
                    reference_curies_list = [curie.strip() for curie in reference_curies_raw.split(',') if curie.strip()]
                    if len(reference_curies_list) > 5:
                        display_curies = ', '.join(reference_curies_list[:5]) + ', ...'
                    else:
                        display_curies = ', '.join(reference_curies_list)
                    data[pieces[0]].append({
                        "entity_type": pieces[1],
                        "entity_status": pieces[2],
                        "entity_curie": pieces[3],
                        "entity_name": pieces[4] if len(pieces) > 4 else None,
                        "reference_count": pieces[5],
                        "reference_curies": display_curies,
                        "species": pieces[7] if len(pieces) > 7 else ""
                    })

    return {
        "date-produced": date_produced,
        "obsolete_entities": dict(data)
    }


def check_redacted_references_with_tags(datestamp: Optional[str] = None):

    log_file = _resolve_qc_log("redacted_references", datestamp)
    date_produced = None
    data = defaultdict(list)

    with open(log_file, 'r') as f:
        for line in f:
            if DATE_PRODUCED_MARKER in line:
                # Split on the marker without its trailing space, so a header
                # written by hand as "#!date-produced:20260101" cannot pass this
                # test and then raise IndexError on the split. Left permissive
                # about the value itself, which is only echoed back to the
                # caller - unlike the datestamps offered as selectable runs,
                # which _parse_date_produced holds to eight digits.
                date_produced = line.split(DATE_PRODUCED_MARKER, 1)[1].strip()
            else:
                pieces = line.strip().split('\t')
                if len(pieces) >= 3:
                    data[pieces[1]].append({
                        "reference_id": pieces[0]
                    })

    return {
        "date-produced": date_produced,
        "redacted-references": dict(data)
    }


def check_obsolete_pmids(datestamp: Optional[str] = None):

    log_file = _resolve_qc_log("obsolete_pmids", datestamp)
    date_produced = None
    data = defaultdict(list)

    with open(log_file, 'r') as f:
        for line in f:
            if DATE_PRODUCED_MARKER in line:
                # Split on the marker without its trailing space, so a header
                # written by hand as "#!date-produced:20260101" cannot pass this
                # test and then raise IndexError on the split. Left permissive
                # about the value itself, which is only echoed back to the
                # caller - unlike the datestamps offered as selectable runs,
                # which _parse_date_produced holds to eight digits.
                date_produced = line.split(DATE_PRODUCED_MARKER, 1)[1].strip()
            else:
                pieces = line.strip().split('\t')
                if len(pieces) >= 2:
                    data[pieces[0]].append(pieces[1])

    return {
        "date-produced": date_produced,
        "obsolete_pmids": dict(data)
    }


def check_duplicate_orcids(datestamp: Optional[str] = None):
    log_file = _resolve_qc_log("duplicate_orcids", datestamp)
    date_produced = None
    data = defaultdict(list)

    with open(log_file, 'r') as f:
        for line in f:
            if DATE_PRODUCED_MARKER in line:
                # Split on the marker without its trailing space, so a header
                # written by hand as "#!date-produced:20260101" cannot pass this
                # test and then raise IndexError on the split. Left permissive
                # about the value itself, which is only echoed back to the
                # caller - unlike the datestamps offered as selectable runs,
                # which _parse_date_produced holds to eight digits.
                date_produced = line.split(DATE_PRODUCED_MARKER, 1)[1].strip()
            else:
                pieces = line.strip().split('\t')
                if len(pieces) >= 4:
                    data[pieces[0]].append({
                        "reference_curie": pieces[1],
                        "orcid": pieces[2],
                        "author_names": pieces[3]
                    })

    return {
        "date-produced": date_produced,
        "duplicate_orcids": dict(data)
    }


def show_environments():
    """
    But only those that are not sensitive. i.e. NO passwords etc
    """
    res = {}
    for test_env in ['API_PORT', 'API_SERVER', 'XML_PATH', 'ENV_STATE',
                     'PSQL_HOST', 'PSQL_PORT', 'PSQL_DATABASE',
                     'HOST', 'ATEAM_API_URL']:
        res[test_env] = environ.get(test_env)

    return res


def get_debezium_reindex_status():
    """
    Read the Debezium reindex status from the shared status file.
    Returns the current reindexing status including progress and ETA.
    """
    status_file = "/var/lib/debezium_status/reindex_status.json"
    metrics_file = "/var/lib/debezium_status/reindex_metrics.json"

    result = {
        "is_reindexing": False,
        "status": "unknown",
        "message": "Status file not found - Debezium may not have been initialized yet"
    }

    try:
        if path.exists(status_file):
            with open(status_file, 'r') as f:
                status_data = json.load(f)

            result = {
                "is_reindexing": status_data.get("is_reindexing", False),
                "status": "active" if status_data.get("is_reindexing") else "completed",
                "phase": status_data.get("phase", "unknown"),
                "started_at": status_data.get("started_at"),
                "current_phase_started_at": status_data.get("current_phase_started_at"),
                "estimated_completion_at": status_data.get("estimated_completion_at"),
                "progress_percentage": status_data.get("progress_percentage", 0),
                "phase_details": status_data.get("phase_details", {})
            }

            # Add historical metrics if available
            if path.exists(metrics_file):
                try:
                    with open(metrics_file, 'r') as mf:
                        metrics_data = json.load(mf)
                        result["historical_metrics"] = {
                            "average_duration_seconds": metrics_data.get("averages", {}).get("total_duration_seconds"),
                            "average_reindex_duration_seconds": metrics_data.get("averages", {}).get("reindex_duration_seconds"),
                            "completed_runs_count": len(metrics_data.get("completed_runs", []))
                        }
                except Exception as e:
                    logger.warning(f"Error reading metrics file: {e}")

        else:
            result["message"] = "No reindex status available - Debezium has not been started yet"

    except json.JSONDecodeError as e:
        logger.error(f"Error parsing status file: {e}")
        result = {
            "is_reindexing": False,
            "status": "error",
            "message": "Error parsing status file"
        }
    except Exception as e:
        logger.error(f"Error reading Debezium status: {e}")
        result = {
            "is_reindexing": False,
            "status": "error",
            "message": "Error reading status"
        }

    return result
