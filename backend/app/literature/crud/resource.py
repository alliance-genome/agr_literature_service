import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi_sqlalchemy import db

from literature import schemas
from literature.models import Reference
from literature.models import Resource


def create_next_curie(curie):
    curie_parts = curie.rsplit('-', 1)
    number_part = curie_parts[1]
    number = int(number_part) + 1
    return "-".join([curie_parts[0], str(number).rjust(10, '0')])

def get_all():
    resources = db.session.query(Resource).all()
    return resources


def create(resource: schemas.ResourceSchemaPost):
    resource_data = jsonable_encoder(resource)

    last_curie = db.session.query(Resource.curie).order_by(sqlalchemy.desc(Resource.curie)).first()

    if last_curie == None:
        last_curie = 'AGR:AGR-Resource-0000000000'
    else:
        last_curie = last_curie[0]

    curie = create_next_curie(last_curie)
    resource_data['curie'] = curie
    resource_db_obj = Resource(**resource_data)
    db.session.add(resource_db_obj)
    db.session.commit()

    return db.session.query(Resource).filter(Resource.curie == curie).first()


def destroy(curie: str):
    resource = db.session.query(Resource).filter(Resource.curie == curie).first()

    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")
    db.session.delete(resource)
    db.session.commit()

    return None


def update(curie: str, updated_resource: schemas.ResourceSchemaPost):

    resource_db_obj = db.session.query(Resource).filter(Resource.curie == curie).first()
    if not resource_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")

    for field, value in vars(updated_resource).items():
        setattr(resource_db_obj, field, value)

    resource_db_obj.dateUpdated = datetime.utcnow()
    db.session.commit()

    return db.session.query(Resource).filter(Resource.curie == curie).first()


def show(curie: str):
    resource = db.session.query(Resource).filter(Resource.curie == curie).first()
    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with the id {curie} is not available")

    return resource

def show_changesets(curie: str):
    resource = db.session.query(Resource).filter(Resource.curie == curie).first()
    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with the id {curie} is not available")

    changesets = []
    for version in resource.versions:
        changesets.append(version.changeset)

    return changesets
