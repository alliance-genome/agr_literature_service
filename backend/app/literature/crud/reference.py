import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi_sqlalchemy import db

from literature.schemas import ReferenceSchemaPost
from literature.schemas import ReferenceSchemaUpdate

from literature.models import Reference
from literature.models import Resource
from literature.models import Author
from literature.models import Editor
from literature.models import CrossReference
from literature.models import ModReferenceType
from literature.models import ReferenceTag
from literature.models import MeshDetail


def create_next_curie(curie):
    curie_parts = curie.rsplit('-', 1)
    number_part = curie_parts[1]
    number = int(number_part) + 1

    return "-".join([curie_parts[0], str(number).rjust(10, '0')])


def get_all():
    references = db.session.query(Reference.curie).all()

    reference_data = []
    for reference in references:
        reference_data.append(reference[0])

    return reference_data


def create(reference: ReferenceSchemaPost):
    reference_data = {}

    if reference.cross_references:
        for cross_reference in reference.cross_references:
            if db.session.query(CrossReference).filter(CrossReference.curie == cross_reference.curie).first():
                raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                    detail=f"CrossReference with id {cross_reference.curie} already exists")

    last_curie = db.session.query(Reference.curie).order_by(sqlalchemy.desc(Reference.curie)).first()

    if last_curie == None:
        last_curie = 'AGR:AGR-Reference-0000000000'
    else:
        last_curie = last_curie[0]

    curie = create_next_curie(last_curie)
    reference_data['curie'] = curie

    for field, value in vars(reference).items():
        if field in ['authors', 'editors', 'mod_reference_types', 'tags', 'mesh_terms', 'cross_references']:
            db_objs = []
            if value is not None:
                for obj in value:
                    obj_data = jsonable_encoder(obj)
                    db_obj = None
                    if field == 'authors':
                        db_obj = Author(**obj_data)
                    elif field == 'editors':
                        db_obj = Editor(**obj_data)
                    elif field == 'mod_reference_types':
                        db_obj = ModReferenceType(**obj_data)
                    elif field == 'tags':
                        db_obj =  ReferenceTag(**obj_data)
                    elif field == 'mesh_terms':
                        db_obj =  MeshDetail(**obj_data)
                    elif field == 'cross_references':
                        db_obj =  CrossReference(**obj_data)

                    db.session.add(db_obj)
                    db_objs.append(db_obj)
            reference_data[field] = db_objs
        else:
            reference_data[field] = value

    if 'resource' in reference_data:
        resource_curie = reference_data['resource']
        resource = db.session.query(Resource).filter(Resource.curie == resource_curie).first()
        if not resource:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Resource with curie {resource_curie} does not exist")
        reference_data['resource'] = resource

    reference_db_obj = Reference(**reference_data)
    db.session.add(reference_db_obj)
    db.session.commit()

    return curie


def destroy(curie: str):
    reference = db.session.query(Reference).filter(Reference.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with curie {curie} not found")
    db.session.delete(reference)
    db.session.commit()

    return None


def update(curie: str, reference_update: ReferenceSchemaUpdate):

    reference_db_obj = db.session.query(Reference).filter(Reference.curie == curie).first()
    if not reference_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with curie {curie} not found")

    for field, value in vars(reference_update).items():
        if field == "resource":
          resource_curie = value
          resource = db.session.query(Resource).filter(Resource.curie == resource_curie).first()
          if not resource:
              raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Resource with curie {resource_curie} does not exist")
          reference_db_obj.resource = resource
        if field == "modReferenceType":
            mod_reference
        else:
            setattr(reference_db_obj, field, value)

    reference_db_obj.dateUpdated = datetime.utcnow()
    db.session.commit()

    return "updated"


def show_files(curie:str):
    reference = db.session.query(Reference).filter(Reference.curie == curie).first()
    files_data = []
    for reference_file in reference.files:
        file_data = jsonable_encoder(reference_file)
        del file_data['reference_id']
        files_data.append(file_data)

    return files_data


def show(curie: str):
    reference = db.session.query(Reference).filter(Reference.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")

    reference_data = jsonable_encoder(reference)

    if reference.resource_id:
        reference_data['resource_curie'] = db.session.query(Resource.curie).filter(Resource.resource_id == reference.resource_id).first()[0]
        reference_data['resource_title'] = db.session.query(Resource.title).filter(Resource.resource_id == reference.resource_id).first()[0]
        del reference_data['reference_id']
    if reference.cross_references:
        for cross_reference in reference_data['cross_references']:
            del cross_reference['reference_id']
            del cross_reference['resource_id']
    if reference.mod_reference_types:
        for mod_reference_type in reference_data['mod_reference_types']:
            del mod_reference_type['reference_id']
    if reference.tags:
        for tag in reference_data['tags']:
            del tag['reference_id']
    if reference.mesh_terms:
        for mesh_term in reference_data['mesh_terms']:
            del mesh_term['reference_id']
    if reference.authors:
        for author in reference_data['authors']:
            del author['resource_id']
            del author['reference_id']
    if reference.editors:
        for editor in reference_data['editors']:
            del editor['resource_id']
            del editor['reference_id']

    del reference_data['files']
    del reference_data['resource_id']

    return reference_data


def show_changesets(curie: str):
    reference = db.session.query(Reference).filter(Reference.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")
    history = []
    for version in reference.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
