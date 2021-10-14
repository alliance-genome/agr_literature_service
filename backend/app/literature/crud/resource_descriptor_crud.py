from sqlalchemy.orm import Session

from literature.models import ResourceDescriptorModel

from initialize import update_resource_descriptor


def update(db: Session):
    return update_resource_descriptor(db)


def show(db: Session):
    return db.query(ResourceDescriptorModel).all()
