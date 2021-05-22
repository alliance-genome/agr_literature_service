import sqlalchemy
from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi_sqlalchemy import db

from literature.schemas import ResourceSchemaPost

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
    resources = db.session.query(Resource).all()

    return resources


def create(resource: ResourceSchemaPost):
    resource_data = {}

#    for author in resource.authors:
#        author_obj = db.session.query(Author).filter(Author.orcid == author.orcid).first()
#        if author_obj:
#            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
#                                detail=f"Author with ORCID {author.orcid} already exists: author_id {author_obj.author_id}")

#    for editor in resource.editors:
#        editor_obj = db.session.query(Editor).filter(Editor.orcid == editor.orcid).first()
#        if editor_obj:
#            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
#                                detail=f"Editor with ORCID {editor.orcid} already exists: editor_id {editor_obj.editor_id}")

    for cross_reference in resource.cross_references:
        if db.session.query(CrossReference).filter(CrossReference.curie == cross_reference.curie).first():
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                detail=f"CrossReference with curie {cross_reference.curie} already exists")

    if db.session.query(Resource).filter(Resource.iso_abbreviation == resource.iso_abbreviation).first():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Resource with iso_abbreviation {resource.iso_abbreviation} already exists")

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

    return db.session.query(Resource).filter(Resource.curie == curie).first()


def destroy(curie: str):
    resource = db.session.query(Resource).filter(Resource.curie == curie).first()

    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")
    db.session.delete(resource)
    db.session.commit()

    return None


def update(curie: str, resource_update: ResourceSchemaPost):

    resource_db_obj = db.session.query(Resource).filter(Resource.curie == curie).first()
    if not resource_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")

    iso_abbreviation_resource = db.session.query(Resource).filter(Resource.iso_abbreviation == resource_update.iso_abbreviation).first()

    if iso_abbreviation_resource and iso_abbreviation_resource.curie != curie:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Resource with iso_abbreviation {resource_update.iso_abbreviation} already exists")


    for field, value in vars(resource_update).items():
        setattr(resource_db_obj, field, value)

    resource_db_obj.date_updated = datetime.utcnow()
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

    history = []
    for version in resource.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
