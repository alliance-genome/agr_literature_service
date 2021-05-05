import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from literature import schemas
from literature.models import Reference
from literature.models import Resource
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder


def create_next_curie(curie):
    curie_parts = curie.rsplit('-', 1)
    number_part = curie_parts[1]
    number = int(number_part) + 1
    return "-".join([curie_parts[0], str(number).rjust(10, '0')])

def get_all(db: Session):
    resources = db.query(Resource).all()
    return resources


def create(resource: schemas.ResourceSchemaPost, db: Session):
    resource_data = jsonable_encoder(resource)

    last_curie = db.query(Resource.curie).order_by(sqlalchemy.desc(Resource.curie)).first()

    if last_curie == None:
        last_curie = 'AGR:AGR-Resource-0000000000'
    else:
        last_curie = last_curie[0]

    curie = create_next_curie(last_curie)
    resource_data['curie'] = curie
    resource_db_obj = Resource(**resource_data)
    db.add(resource_db_obj)
    db.commit()

    return db.query(Resource).filter(Resource.curie == curie).first()


def destroy(curie: str, db: Session):
    resource = db.query(Resource).filter(Resource.curie == curie).first()

    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")
    db.delete(resource)
    db.commit()

    return None


def update(curie: str, updated_resource: schemas.ResourceSchemaPost, db: Session):

    resource_db_obj = db.query(Resource).filter(Resource.curie == curie).first()
    if not resource_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")

    for field, value in vars(updated_resource).items():
        setattr(resource_db_obj, field, value)

    resource_db_obj.dateUpdated = datetime.utcnow()
    db.commit()

    return db.query(Resource).filter(Resource.curie == curie).first()


def show(curie: str, db: Session):
    resource = db.query(Resource).filter(Resource.curie == curie).first()
    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with the id {curie} is not available")

    return resource

def show_changesets(curie: str, db: Session):
    resource = db.query(Resource).filter(Resource.curie == curie).first()
    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with the id {curie} is not available")

    changesets = []
    for version in resource.versions:
        changesets.append(version.changeset)

    return changesets
