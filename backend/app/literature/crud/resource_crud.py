import sqlalchemy
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder

from sqlalchemy.orm import Session

from literature.schemas import ResourceSchemaPost
from literature.schemas import ResourceSchemaUpdate

from literature.crud import cross_reference_crud

from literature.models import ResourceModel
from literature.models import AuthorModel
from literature.models import EditorModel
from literature.models import CrossReferenceModel
from literature.models import MeshDetailModel


def create_next_curie(curie):
    curie_parts = curie.rsplit('-', 1)
    number_part = curie_parts[1]
    number = int(number_part) + 1
    return "-".join([curie_parts[0], str(number).rjust(10, '0')])


def create(db: Session, resource: ResourceSchemaPost):
    resource_data = {}

    if resource.cross_references is not None:
        for cross_reference in resource.cross_references:
            if db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == cross_reference.curie).first():
                raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                    detail=f"CrossReference with curie {cross_reference.curie} already exists")

    last_curie = db.query(ResourceModel.curie).order_by(sqlalchemy.desc(ResourceModel.curie)).first()

    if not last_curie:
        last_curie = 'AGR:AGR-Resource-0000000000'
    else:
        last_curie = last_curie[0]

    curie = create_next_curie(last_curie)
    resource_data['curie'] = curie

    for field, value in vars(resource).items():
        if field in ['authors', 'editors', 'cross_references', 'mesh_terms']:
            db_objs = []
            if value is None:
                continue
            for obj in value:
                obj_data = jsonable_encoder(obj)
                db_obj = None
                if field in ['authors', 'editors']:
                    if obj_data['orcid']:
                        cross_reference_obj = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == obj_data['orcid']).first()
                        if not cross_reference_obj:
                            cross_reference_obj = CrossReferenceModel(curie=obj_data['orcid'])
                            db.add(cross_reference_obj)

                        obj_data['orcid_cross_reference'] = cross_reference_obj
                    del obj_data['orcid']
                    if field == 'authors':
                        db_obj = AuthorModel(**obj_data)
                    else:
                        db_obj = EditorModel(**obj_data)
                elif field == 'cross_references':
                    db_obj = CrossReferenceModel(**obj_data)
                elif field == 'mesh_terms':
                    db_obj = MeshDetailModel(**obj_data)
                db.add(db_obj)
                db_objs.append(db_obj)
            resource_data[field] = db_objs
        else:
            resource_data[field] = value

    resource_db_obj = ResourceModel(**resource_data)
    db.add(resource_db_obj)
    db.commit()

    return curie


def destroy(db: Session, curie: str):
    resource = db.query(ResourceModel).filter(ResourceModel.curie == curie).first()

    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")
    db.delete(resource)
    db.commit()

    return None


def patch(db: Session, curie: str, resource_update: ResourceSchemaUpdate):
    resource_db_obj = db.query(ResourceModel).filter(ResourceModel.curie == curie).first()
    if not resource_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")

    if 'iso_abbreviation' in resource_update and resource_update['iso_abbreviation']:
        iso_abbreviation_resource = db.query(ResourceModel).filter(ResourceModel.iso_abbreviation == resource_update['iso_abbreviation']).first()

        if iso_abbreviation_resource and iso_abbreviation_resource.curie != curie:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                detail=f"Resource with iso_abbreviation {resource_update.iso_abbreviation} already exists")

    for field, value in resource_update.items():
        setattr(resource_db_obj, field, value)

    resource_db_obj.date_updated = datetime.utcnow()
    db.commit()

    return {"message": "updated"}


def show(db: Session, curie: str):
    resource = db.query(ResourceModel).filter(ResourceModel.curie == curie).first()
    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with the id {curie} is not available")

    resource_data = jsonable_encoder(resource)
    if resource.cross_references:
        cross_references = []
        for cross_reference in resource_data['cross_references']:
            cross_reference_show = jsonable_encoder(cross_reference_crud.show(db, cross_reference['curie']))
            del cross_reference_show['resource_curie']
            cross_references.append(cross_reference_show)
        resource_data['cross_references'] = cross_references

    if resource.authors:
        for author in resource_data['authors']:
            if author['orcid']:
                author['orcid'] = jsonable_encoder(cross_reference_crud.show(db, author['orcid']))
            del author['person_id']
            del author['orcid_cross_reference']
            del author['resource_id']
            del author['reference_id']

    if resource.editors:
        for editor in resource_data['editors']:
            if editor['orcid']:
                editor['orcid'] = jsonable_encoder(cross_reference_crud.show(db, editor['orcid']))
            del editor['person_id']
            del editor['orcid_cross_reference']
            del editor['resource_id']
            del editor['reference_id']

    return resource_data


def show_notes(db: Session, curie: str):
    resource = db.query(ResourceModel).filter(ResourceModel.curie == curie).first()

    notes_data = []
    for resource_note in resource.notes:
        note_data = jsonable_encoder(resource_note)
        del note_data['reference_id']
        del note_data['resource_id']
        note_data['resource_curie'] = curie
        notes_data.append(note_data)

    return notes_data


def show_changesets(db: Session, curie: str):
    resource = db.query(ResourceModel).filter(ResourceModel.curie == curie).first()
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
