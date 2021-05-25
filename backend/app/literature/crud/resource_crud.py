import sqlalchemy
from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi_sqlalchemy import db

from literature.schemas import ResourceSchemaPost
from literature.schemas import ResourceSchemaUpdate

from literature.models import Reference
from literature.models import Resource
from literature.models import Author
from literature.models import Editor
from literature.models import CrossReference
from literature.models import MeshDetail


def create_next_curie(curie):
    curie_parts = curie.rsplit('-', 1)
    number_part = curie_parts[1]
    number = int(number_part) + 1
    return "-".join([curie_parts[0], str(number).rjust(10, '0')])


def get_all():
    resources = db.session.query(Resource.curie).all()

    resources_data = []
    for resource in resources:
         resources_data.append(resource[0])

    return resources_data


def create(resource: ResourceSchemaPost):
    resource_data = {}

    if resource.cross_references is not None:
        for cross_reference in resource.cross_references:
            if db.session.query(CrossReference).filter(CrossReference.curie == cross_reference.curie).first():
                raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                    detail=f"CrossReference with curie {cross_reference.curie} already exists")

    last_curie = db.session.query(Resource.curie).order_by(sqlalchemy.desc(Resource.curie)).first()

    if last_curie == None:
        last_curie = 'AGR:AGR-Resource-0000000000'
    else:
        last_curie = last_curie[0]

    curie = create_next_curie(last_curie)
    resource_data['curie'] = curie

    for field, value in vars(resource).items():
        if field in ['authors', 'editors', 'cross_references', 'mesh_terms']:
            db_objs = []
            if value is not None:
                for obj in value:
                    obj_data = jsonable_encoder(obj)
                    db_obj = None
                    if field == 'authors':
                        db_obj = Author(**obj_data)
                    elif field == 'editors':
                        db_obj = Editor(**obj_data)
                    elif field == 'cross_references':
                        db_obj = CrossReference(**obj_data)
                    elif field == 'mesh_terms':
                        db_obj = MeshDetail(**obj_data)
                    db.session.add(db_obj)
                    db_objs.append(db_obj)
                resource_data[field] = db_objs
        else:
            resource_data[field] = value

    resource_db_obj = Resource(**resource_data)
    db.session.add(resource_db_obj)
    db.session.commit()

    return curie


def destroy(curie: str):
    resource = db.session.query(Resource).filter(Resource.curie == curie).first()

    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")
    db.session.delete(resource)
    db.session.commit()

    return None


def update(curie: str, resource_update: ResourceSchemaUpdate):

    resource_db_obj = db.session.query(Resource).filter(Resource.curie == curie).first()
    if not resource_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")

    if resource_update.iso_abbreviation not in [None, ""]:
        iso_abbreviation_resource = db.session.query(Resource).filter(Resource.iso_abbreviation == resource_update.iso_abbreviation).first()

        if iso_abbreviation_resource and iso_abbreviation_resource.curie != curie:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                detail=f"Resource with iso_abbreviation {resource_update.iso_abbreviation} already exists")


    for field, value in vars(resource_update).items():
        if value is not None:
            setattr(resource_db_obj, field, value)

    resource_db_obj.date_updated = datetime.utcnow()
    db.session.commit()

    return "updated"


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

    history = []
    for version in resource.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
