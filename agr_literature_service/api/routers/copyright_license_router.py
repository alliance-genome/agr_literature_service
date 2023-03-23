from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import copyright_license_crud


router = APIRouter(
    prefix="/copyright_license",
    tags=['Copyright License']
)


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get('/all',
            status_code=200)
def show_all(db: Session = db_session):
    return copyright_license_crud.show_all(db)
