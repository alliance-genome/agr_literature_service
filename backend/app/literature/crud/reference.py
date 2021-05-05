import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from literature import schemas
from literature.models import Reference
from literature.models import Resource
from literature.models import Author

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder


def create_next_curie(curie):
    curie_parts = curie.rsplit('-', 1)
    number_part = curie_parts[1]
    number = int(number_part) + 1
    return "-".join([curie_parts[0], str(number).rjust(10, '0')])

def get_all(db: Session):
    references = db.query(Reference).all()
    return references


def create(reference: schemas.ReferenceSchemaPost, db: Session):
    reference_data = {} # jsonable_encoder(reference)

    last_curie = db.query(Reference.curie).order_by(sqlalchemy.desc(Reference.curie)).first()

    if last_curie == None:
        last_curie = 'AGR:AGR-Reference-0000000000'
    else:
        last_curie = last_curie[0]

    curie = create_next_curie(last_curie)
    reference_data['curie'] = curie

    for field, value in vars(reference).items():
        if field == 'authors':
            authors = []
            for author in value:
                author_data = jsonable_encoder(author)
                author_db_obj = Author(**author_data)
                db.add(author_db_obj)
                authors.append(author_db_obj)
            reference_data['authors'] = authors
        else:
            reference_data[field] = value

    if 'resource' in reference_data:
        resource_curie = reference_data['resource']
        resource = db.query(Resource).filter(Resource.curie == resource_curie).first()
        if not resource:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Resource with curie {resource_curie} does not exist")
        reference_data['resource'] = resource

    reference_db_obj = Reference(**reference_data)
    db.add(reference_db_obj)
    db.commit()

    return db.query(Reference).filter(Reference.curie == curie).first()


def destroy(curie: str, db: Session):
    reference = db.query(Reference).filter(Reference.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with curie {curie} not found")
    db.delete(reference)
    db.commit()

    return None


def update(curie: str, updated_reference: schemas.ReferenceSchemaUpdate, db: Session):

    reference_db_obj = db.query(Reference).filter(Reference.curie == curie).first()
    if not reference_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with curie {curie} not found")

    for field, value in vars(updated_reference).items():
        if field == "resource":
          resource_curie = value
          resource = db.query(Resource).filter(Resource.curie == resource_curie).first()
          if not resource:
              raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Resource with curie {resource_curie} does not exist")
          reference_data['resource'] = resource
        else:
            print(field)
            print(value)
            setattr(reference_db_obj, field, value)

    reference_db_obj.dateUpdated = datetime.utcnow()
    db.commit()

    return db.query(Reference).filter(Reference.curie == curie).first()


def show(curie: str, db: Session):
    reference = db.query(Reference).filter(Reference.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")

    return reference

def show_changesets(curie: str, db: Session):
    reference = db.query(Reference).filter(Reference.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")

    changesets = []
    for version in reference.versions:
        changesets.append(version.changeset)

    return changesets
