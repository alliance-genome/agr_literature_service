from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from agr_literature_service.api.models import CopyrightLicenseModel
from agr_literature_service.api.schemas import CopyrightLicenseSchemaPost


def create(db: Session, license: CopyrightLicenseSchemaPost):
    """
    :param db:
    :param license:
    :return:
    """

    license_data = jsonable_encoder(license)
    license_db_obj = CopyrightLicenseModel(**license_data)
    db.add(license_db_obj)
    db.commit()

    db.refresh(license_db_obj)

    return license_db_obj.copyright_license_id


def show_all(db: Session):
    return db.query(CopyrightLicenseModel).all()
