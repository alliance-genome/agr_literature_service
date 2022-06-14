from agr_literature_service.api.initialize import update_resource_descriptor
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ResourceDescriptorModel


def update(db: Session):
    return update_resource_descriptor(db)


def show(db: Session):
    return db.query(ResourceDescriptorModel).all()
