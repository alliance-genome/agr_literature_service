from typing import Any, Dict, Optional

from fastapi import APIRouter, Query, Security

from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.crud import report_crud

router = APIRouter(
    prefix="/report",
    tags=['Report']
)


@router.get('/files',
            status_code=200)
def list_report_files(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    """
    List every report/log file the automated pipelines have written.

    Returns a flat, recursive listing — path, name, directory, size in bytes,
    ISO-8601 modification time, and a public url. The tree is a few hundred
    files, so it is returned whole and the caller filters it.
    """
    return report_crud.list_report_files()


@router.get('/file',
            status_code=200)
def get_report_file(
    path: str = Query(..., description="Report file path, relative to the report directory."),
    tail: Optional[int] = Query(None, ge=1, description="Return only the last N bytes."),
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    """
    Read one report file, optionally only its last ``tail`` bytes.

    Tailing exists for the multi-megabyte logs, which should not be sent whole
    just to show the end of a run.
    """
    return report_crud.get_report_file(path, tail)
