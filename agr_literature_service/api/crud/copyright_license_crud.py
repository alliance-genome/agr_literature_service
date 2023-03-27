from fastapi import HTTPException, status
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


def show(db: Session, license_name: str):
    """
    :param db:
    :param copyright_license_id:
    :return:
    """
    license = db.query(CopyrightLicenseModel).filter_by(name=license_name).first()
    if not license:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"License with the license_name {license_name} is not available")
    license_data = jsonable_encoder(license)
    return license_data


def show_all(db: Session):
    return db.query(CopyrightLicenseModel).all()
