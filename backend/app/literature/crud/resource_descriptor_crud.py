from initialize import update_resource_descriptor
from sqlalchemy.orm import Session

from literature.models import ResourceDescriptorModel


def update(db: Session):
    return update_resource_descriptor(db)


def show(db: Session):
    return db.query(ResourceDescriptorModel).all()
