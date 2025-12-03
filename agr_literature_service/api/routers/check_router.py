from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from agr_literature_service.api import database
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
def check_ateam_api():
    res = check_crud.check_ateam_api()
    return AteamApiSchemaShow(checks=[res])


@router.get('/database',
            response_model=DatabaseSchemaShow,
            status_code=200)
def check_database(db: Session = db_session):
    return {"db_details": check_crud.check_database(db)}


@router.get('/check_obsolete_entities',
            status_code=200)
def check_obsolete_entities():
    return check_crud.check_obsolete_entities()


@router.get('/check_redacted_references_with_tags',
            status_code=200)
def check_redacted_references_with_tags():
    return check_crud.check_redacted_references_with_tags()


@router.get('/check_obsolete_pmids',
            status_code=200)
def check_obsolete_pmids():
    return check_crud.check_obsolete_pmids()


@router.get('/check_duplicate_orcids',
            status_code=200)
def check_duplicate_orcids():
    return check_crud.check_duplicate_orcids()


@router.get('/environments',
            response_model=EnvironmentsSchemaShow,
            status_code=200)
def show_environments():
    res = check_crud.show_environments()
    return {'envs': res}


@router.get('/debezium_status',
            status_code=200)
def get_debezium_reindex_status():
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
