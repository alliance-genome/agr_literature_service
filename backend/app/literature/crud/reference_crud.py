import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder

from literature.schemas import ReferenceSchemaPost
from literature.schemas import ReferenceSchemaUpdate

from literature.crud import cross_reference_crud

from literature.models import ReferenceModel
from literature.models import ResourceModel
from literature.models import AuthorModel
from literature.models import EditorModel
from literature.models import CrossReferenceModel
from literature.models import ModReferenceTypeModel
from literature.models import ReferenceTagModel
from literature.models import MeshDetailModel


def create_next_curie(curie):
    curie_parts = curie.rsplit('-', 1)
    number_part = curie_parts[1]
    number = int(number_part) + 1

    return "-".join([curie_parts[0], str(number).rjust(10, '0')])


def create(db: Session, reference: ReferenceSchemaPost):
    reference_data = {}

    if reference.cross_references:
        for cross_reference in reference.cross_references:
            if db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == cross_reference.curie).first():
                raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                    detail=f"CrossReference with id {cross_reference.curie} already exists")

    last_curie = db.query(ReferenceModel.curie).order_by(sqlalchemy.desc(ReferenceModel.curie)).first()

    if last_curie == None:
        last_curie = 'AGR:AGR-Reference-0000000000'
    else:
        last_curie = last_curie[0]

    curie = create_next_curie(last_curie)
    reference_data['curie'] = curie

    for field, value in vars(reference).items():
        if value is None:
            continue
        if field in ['authors', 'editors', 'mod_reference_types', 'tags', 'mesh_terms', 'cross_references']:
            db_objs = []
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
                elif field == 'mod_reference_types':
                    db_obj = ModReferenceTypeModel(**obj_data)
                elif field == 'tags':
                    db_obj =  ReferenceTagModel(**obj_data)
                elif field == 'mesh_terms':
                    db_obj =  MeshDetailModel(**obj_data)
                elif field == 'cross_references':
                    db_obj =  CrossReferenceModel(**obj_data)

                db.add(db_obj)
                db_objs.append(db_obj)
            reference_data[field] = db_objs
        elif field == 'resource':
            resource = db.query(ResourceModel).filter(ResourceModel.curie == value).first()
            if not resource:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Resource with curie {value} does not exist")
            reference_data['resource'] = resource
        else:
            reference_data[field] = value

    reference_db_obj = ReferenceModel(**reference_data)
    db.add(reference_db_obj)
    db.commit()

    return curie


def destroy(db: Session, curie: str):
    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with curie {curie} not found")
    db.delete(reference)
    db.commit()

    return None


def patch(db: Session, curie: str, reference_update: ReferenceSchemaUpdate):
    reference_db_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).first()

    if not reference_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with curie {curie} not found")

    for field, value in reference_update.items():
        if field == "resource":
          resource_curie = value
          resource = db.query(ResourceModel).filter(ResourceModel.curie == resource_curie).first()
          if not resource:
              raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Resource with curie {resource_curie} does not exist")
          reference_db_obj.resource = resource
        else:
            setattr(reference_db_obj, field, value)

    reference_db_obj.dateUpdated = datetime.utcnow()
    db.commit()

    return "updated"


def show_files(db: Session, curie:str):
    reference = db.query(Reference).filter(Reference.curie == curie).first()
    files_data = []
    for reference_file in reference.files:
        file_data = jsonable_encoder(reference_file)
        del file_data['reference_id']
        files_data.append(file_data)

    return files_data


def show(db: Session, curie: str):
    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one_or_none()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")

    reference_data = jsonable_encoder(reference)

    if reference.resource_id:
        reference_data['resource_curie'] = db.query(ResourceModel.curie).filter(ResourceModel.resource_id == reference.resource_id).first()[0]
        reference_data['resource_title'] = db.query(ResourceModel.title).filter(ResourceModel.resource_id == reference.resource_id).first()[0]
        del reference_data['resource_id']

    if reference.cross_references:
        cross_references = []
        for cross_reference in reference_data['cross_references']:
            cross_reference_show = jsonable_encoder(cross_reference_crud.show(db, cross_reference['curie']))
            del cross_reference_show['reference_curie']
            cross_references.append(cross_reference_show)
        reference_data['cross_references'] = cross_references

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
            if author['orcid_id']:
                author['orcid'] = jsonable_encoder(cross_reference_crud.show(db, author['orcid_id']))
            del author['orcid_id']
            del author['orcid_cross_reference']
            del author['resource_id']
            del author['reference_id']

    if reference.editors:
        for editor in reference_data['editors']:
            if editor['orcid_id']:
                editor['orcid'] = jsonable_encoder(cross_reference_crud.show(db, editor['orcid_id']))
            del editor['orcid_id']
            del editor['orcid_cross_reference']
            del editor['resource_id']
            del editor['reference_id']

    del reference_data['files']

    return reference_data


def show_changesets(db: Session, curie: str):
    reference = db.query(Reference).filter(Reference.curie == curie).first()
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
