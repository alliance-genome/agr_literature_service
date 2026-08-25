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
from typing import List, Optional

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

DATESTAMP_RE = re.compile(r'^\d{8}$')


def _qc_report_stem(report_key: str) -> str:
    """Filename stem for a report key, refusing anything not whitelisted.

    The stem is looked up rather than taken from the request, so no
    caller-supplied string ever becomes part of a filename.
    """
    stem = QC_REPORTS.get(report_key)
    if stem is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Unknown QC report '{report_key}'. Expected one of: "
                                   f"{', '.join(sorted(QC_REPORTS))}.")
    return stem


def _qc_latest_datestamp(stem: str, log_path: str) -> Optional[str]:
    """The date the undated "latest" file claims, from its own header.

    A run keeps the plain "<stem>.log" name while it is current and only gains
    a "_YYYYMMDD" copy once it is superseded, so the newest report usually has
    no datestamp in its filename. Its date has to be read out of the
    "#!date-produced:" header instead, which is the first line of the file.
    """
    log_file = path.join(log_path, f"QC/{stem}.log")
    if not path.isfile(log_file):
        return None
    try:
        with open(log_file, 'r') as f:
            first_line = f.readline()
    except OSError:
        return None
    if 'date-produced:' not in first_line:
        return None
    datestamp = first_line.split('date-produced: ')[1].strip()
    return datestamp if DATESTAMP_RE.match(datestamp) else None


def list_qc_report_dates(report_key: str) -> List[str]:
    """Datestamps of the runs of one QC report, newest first.

    Both the datestamped archives and the current run are listed. The current
    run is the undated "<stem>.log", which gains its "_YYYYMMDD" copy only once
    it is superseded, so listing the archives alone would leave the newest
    report out - and a caller defaulting to the first entry would then show
    stale data.

    A missing LOG_PATH/QC directory is not an error - it only means this host
    has no history yet - so the listing comes back empty rather than 404ing,
    and the caller falls back to the latest report.
    """
    stem = _qc_report_stem(report_key)
    log_path = environ.get('LOG_PATH', '.')
    datestamps = set()
    for log_file in glob(path.join(log_path, f"QC/{stem}_*.log")):
        suffix = path.basename(log_file)[len(stem) + 1:-len('.log')]
        if DATESTAMP_RE.match(suffix):
            datestamps.add(suffix)
    latest = _qc_latest_datestamp(stem, log_path)
    if latest:
        datestamps.add(latest)
    return sorted(datestamps, reverse=True)


def qc_latest_datestamp(report_key: str) -> Optional[str]:
    """The date the current, undated run of one QC report reports for itself.

    None when there is no undated file, or when it carries no
    "#!date-produced:" header. Callers use this to tell which entry of
    list_qc_report_dates is the current run, since that run is the one holding
    the plain filename.
    """
    stem = _qc_report_stem(report_key)
    return _qc_latest_datestamp(stem, environ.get('LOG_PATH', '.'))


def qc_latest_exists(report_key: str) -> bool:
    """Whether the current, undated run of one QC report is on disk.

    Deliberately separate from qc_latest_datestamp returning a date. A log
    written by hand, with no "#!date-produced:" header, is still the current run
    and still readable - it just cannot be labelled with a date. Callers need to
    tell that apart from there being no current run at all, because the first
    should still be offered and the second should not.
    """
    stem = _qc_report_stem(report_key)
    log_path = environ.get('LOG_PATH', '.')
    return path.isfile(path.join(log_path, f"QC/{stem}.log"))


def _resolve_qc_log(report_key: str, datestamp: Optional[str] = None) -> str:
    """Path of one QC report file: a dated archive, or the latest run.

    Both halves of the filename are constrained before they reach the
    filesystem - the stem through the QC_REPORTS whitelist, the datestamp
    through an exact eight-digit match - so a path separator or a ".." segment
    cannot get through. Never interpolate a caller's string into a path
    without a check like this.

    Omitting the datestamp reads the undated "latest" file. A datestamp that
    has no archived copy still resolves to that same file when its header
    reports that date, because the current run is not given a datestamped name
    until it is superseded - so the newest date a caller can see listed is
    usually one that exists only inside the undated file.
    """
    stem = _qc_report_stem(report_key)
    log_path = environ.get('LOG_PATH', '.')
    latest_file = path.join(log_path, f"QC/{stem}.log")

    if not datestamp:
        log_file = latest_file
    else:
        if not DATESTAMP_RE.match(datestamp):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="datestamp must be 8 digits in YYYYMMDD form.")
        log_file = path.join(log_path, f"QC/{stem}_{datestamp}.log")
        if not path.isfile(log_file) and _qc_latest_datestamp(stem, log_path) == datestamp:
            log_file = latest_file

    if not path.isfile(log_file):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"No {report_key} report for {datestamp or 'the latest run'}.")
    return log_file


def check_obsolete_entities(datestamp: Optional[str] = None):

    log_file = _resolve_qc_log("obsolete_entities", datestamp)
    date_produced = None
    data = defaultdict(list)

    with open(log_file, 'r') as f:
        for line in f:
            if 'date-produced:' in line:
                date_produced = line.split('date-produced: ')[1].strip()
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
            if 'date-produced:' in line:
                date_produced = line.split('date-produced: ')[1].strip()
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
            if 'date-produced:' in line:
                date_produced = line.split('date-produced: ')[1].strip()
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
            if 'date-produced:' in line:
                date_produced = line.split('date-produced: ')[1].strip()
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
