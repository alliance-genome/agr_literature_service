from sqlalchemy.orm import Session
from agr_literature_service.api.models import CopyrightLicenseModel


def show_all(db: Session):
    return db.query(CopyrightLicenseModel).all()
