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


def create_next_curie(curie):
    curie_parts = curie.rsplit('-', 1)
    number_part = curie_parts[1]
    number = int(number_part) + 1
    return "-".join([curie_parts[0], str(number).rjust(10, '0')])


def get_all():
    references = db.session.query(Reference).all()
    return references


def create(reference: ReferenceSchemaPost):
    reference_data = {}


    for author in reference.authors:
        author_obj = db.session.query(Author).filter(Author.orcid == author.orcid).first()
        if author_obj:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                detail=f"Author with ORCID {author.orcid} already exists: author_id {author_obj.author_id}")



    for cross_reference in reference.crossReferences:
        if db.session.query(CrossReference).filter(CrossReference == cross_reference.curie).first():
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
        if field in ['authors', 'editors', 'modReferenceType']:
            db_objs = []
            for obj in value:
                obj_data = jsonable_encoder(obj)
                db_obj = None
                if field == 'authors':
                    db_obj = Author(**obj_data)
                elif field == 'editors':
                    db_obj = Editor(**obj_data)
                elif field == 'modReferenceType':
                    db_obj = ModReferenceType(**obj_data)
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

    return db.session.query(Reference).filter(Reference.curie == curie).first()


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
          reference_data['resource'] = resource
        else:
            print(field)
            print(value)
            setattr(reference_db_obj, field, value)

    reference_db_obj.dateUpdated = datetime.utcnow()
    db.session.commit()

    return db.session.query(Reference).filter(Reference.curie == curie).first()


def show(curie: str):
    reference = db.session.query(Reference).filter(Reference.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")

    return reference


def show_changesets(curie: str):
    reference = db.session.query(Reference).filter(Reference.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")

    changesets = []
    for version in reference.versions:
        changesets.append(version.changeset)

    return changesets
