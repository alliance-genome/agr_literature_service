from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query, Security
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.crud import check_crud
from agr_literature_service.api.schemas import (AteamApiSchemaShow, DatabaseSchemaShow, EnvironmentsSchemaShow)

router = APIRouter(
    prefix="/check",
    tags=['Check']
)

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get('/ateamapi',
            response_model=AteamApiSchemaShow,
            status_code=200)
def check_ateam_api(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    res = check_crud.check_ateam_api()
    return AteamApiSchemaShow(checks=[res])


@router.get('/database',
            response_model=DatabaseSchemaShow,
            status_code=200)
def check_database(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return {"db_details": check_crud.check_database(db)}


@router.get('/check_obsolete_entities',
            status_code=200)
def check_obsolete_entities(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    datestamp: Optional[str] = Query(None, description="Archived run to read, as YYYYMMDD. Omit for the latest report."),
):
    return check_crud.check_obsolete_entities(datestamp)


@router.get('/check_redacted_references_with_tags',
            status_code=200)
def check_redacted_references_with_tags(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    datestamp: Optional[str] = Query(None, description="Archived run to read, as YYYYMMDD. Omit for the latest report."),
):
    return check_crud.check_redacted_references_with_tags(datestamp)


@router.get('/check_obsolete_pmids',
            status_code=200)
def check_obsolete_pmids(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    datestamp: Optional[str] = Query(None, description="Archived run to read, as YYYYMMDD. Omit for the latest report."),
):
    return check_crud.check_obsolete_pmids(datestamp)


@router.get('/check_duplicate_orcids',
            status_code=200)
def check_duplicate_orcids(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    datestamp: Optional[str] = Query(None, description="Archived run to read, as YYYYMMDD. Omit for the latest report."),
):
    return check_crud.check_duplicate_orcids(datestamp)


@router.get('/qc_report_dates/{report_key}',
            status_code=200)
def qc_report_dates(
    report_key: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    """
    Datestamps of the archived runs of one QC report, newest first.

    ``report_key`` is one of obsolete_entities, redacted_references,
    obsolete_pmids or duplicate_orcids. Feed a returned datestamp back to the
    matching /check/check_* endpoint to read that run instead of the latest.
    """
    return {
        "report": report_key,
        "dates": check_crud.list_qc_report_dates(report_key)
    }


@router.get('/environments',
            response_model=EnvironmentsSchemaShow,
            status_code=200)
def show_environments(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    res = check_crud.show_environments()
    return {'envs': res}


@router.get('/debezium_status',
            status_code=200)
def get_debezium_reindex_status(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    """
    Get the current status of Debezium Elasticsearch reindexing.

    Returns:
        - is_reindexing: boolean indicating if reindexing is currently in progress
        - status: current status (active, completed, error, unknown)
        - phase: current phase (setup, data_processing, reindexing, completed)
        - progress_percentage: estimated completion percentage (0-100)
        - estimated_completion_at: ISO 8601 timestamp of estimated completion
        - phase_details: additional details about the current phase
        - historical_metrics: average durations from previous runs (if available)
    """
    return check_crud.get_debezium_reindex_status()
