"""
resource_crud.py
================
"""

from datetime import datetime
from typing import Dict, Union

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import ARRAY, Boolean, String, func
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import cast

from agr_literature_service.api.crud import cross_reference_crud
from agr_literature_service.api.crud.cross_reference_crud import set_curie_prefix
from agr_literature_service.api.crud.reference_resource import create_obj
from agr_literature_service.api.models import (CrossReferenceModel, EditorModel,
                                               MeshDetailModel, ResourceModel)
from agr_literature_service.api.schemas import ResourceSchemaPost, ResourceSchemaUpdate
from agr_literature_service.global_utils import get_next_resource_curie
from agr_literature_service.api.crud.user_utils import map_to_user_id


def create(db: Session, resource: ResourceSchemaPost):
    """
    Creates a new resource.
    :param db:
    :param resource:
    :return:
    """
    remap = {'editors': 'editor',
             'mesh_terms': 'mesh_term',
             'cross_references': 'cross_reference',
             'mod_reference_types': 'mod_reference_type'}
    resource_data = {}

    if resource.cross_references is not None:
        for cross_reference in resource.cross_references:
            if db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == cross_reference.curie).first():
                raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                    detail=f"CrossReference with curie {cross_reference.curie} already exists")

    curie = get_next_resource_curie(db)
    resource_data['curie'] = curie

    for field, value in vars(resource).items():
        if field in ['editors', 'cross_references', 'mesh_terms']:
            db_objs = []
            if value is None:
                continue
            for obj in value:
                obj_data = jsonable_encoder(obj)
                if "created_by" in obj_data and obj_data["created_by"] is not None:
                    obj_data["created_by"] = map_to_user_id(obj_data["created_by"], db)
                if "updated_by" in obj_data and obj_data["updated_by"] is not None:
                    obj_data["updated_by"] = map_to_user_id(obj_data["updated_by"], db)
                db_obj = None
                if field == 'editors':
                    db_obj = create_obj(db, EditorModel, obj_data, non_fatal=True)
                elif field == 'cross_references':
                    db_obj = CrossReferenceModel(**obj_data)
                    set_curie_prefix(db_obj)
                elif field == 'mesh_terms':
                    db_obj = MeshDetailModel(**obj_data)
                db.add(db_obj)
                db_objs.append(db_obj)
            if field in remap:
                resource_data[remap[field]] = db_objs
            else:
                resource_data[field] = db_objs
        else:
            resource_data[field] = value

    resource_db_obj = ResourceModel(**resource_data)
    db.add(resource_db_obj)
    db.commit()

    return curie


def show_all_resources_external_ids(db: Session):
    """
    Returns all resources with external ids.
    :param db:
    :return:
    """

    resources_query = db.query(ResourceModel.curie,
                               cast(func.array_agg(CrossReferenceModel.curie),
                                    ARRAY(String)),
                               cast(func.array_agg(CrossReferenceModel.is_obsolete),
                                    ARRAY(Boolean))) \
        .outerjoin(ResourceModel.cross_reference) \
        .group_by(ResourceModel.curie)

    return [{'curie': resource[0],
             'cross_references': [{'curie': resource[1][idx],
                                   'is_obsolete': resource[2][idx]}
                                  for idx in range(len(resource[1]))]}
            for resource in resources_query.all()]


def destroy(db: Session, curie: str):
    """

    :param db:
    :param curie:
    :return:
    """

    resource = db.query(ResourceModel).filter(ResourceModel.curie == curie).first()

    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")
    db.delete(resource)
    db.commit()

    return None


def patch(db: Session, curie: str, resource_update: Union[ResourceSchemaUpdate, Dict]) -> dict:
    """

    :param db:
    :param curie:
    :param resource_update:
    :return:
    """

    resource_db_obj = db.query(ResourceModel).filter(ResourceModel.curie == curie).first()
    if resource_db_obj is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with curie {curie} not found")

    if isinstance(resource_update, ResourceSchemaUpdate):
        if resource_update.iso_abbreviation is not None:
            iso_abbreviation_resource = db.query(ResourceModel).filter(ResourceModel.iso_abbreviation == resource_update.iso_abbreviation).first()

            if iso_abbreviation_resource and iso_abbreviation_resource.curie != curie:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                    detail=f"Resource with iso_abbreviation {resource_update.iso_abbreviation} already exists")

    update_dict = {}  # type: Dict
    if isinstance(resource_update, ResourceSchemaUpdate):
        update_dict = resource_update.dict()
    elif isinstance(resource_update, Dict):
        update_dict = resource_update
    else:
        update_dict = {}

    if "created_by" in update_dict and update_dict["created_by"] is not None:
        update_dict["created_by"] = map_to_user_id(update_dict["created_by"], db)
    if "updated_by" in update_dict and update_dict["updated_by"] is not None:
        update_dict["updated_by"] = map_to_user_id(update_dict["updated_by"], db)

    for field, value in update_dict.items():
        setattr(resource_db_obj, field, value)

    resource_db_obj.date_updated = datetime.utcnow()
    db.commit()

    return {"message": "updated"}


def show(db: Session, curie: str):
    """

    :param db:
    :param curie:
    :return:
    """

    resource = db.query(ResourceModel).filter(ResourceModel.curie == curie).first()
    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with the id {curie} is not available")

    resource_data = jsonable_encoder(resource)
    if resource.cross_reference:
        cross_references = []
        for cross_reference in resource_data['cross_reference']:
            cross_reference_show = jsonable_encoder(cross_reference_crud.show(db, cross_reference['curie']))
            del cross_reference_show['resource_curie']
            cross_references.append(cross_reference_show)
        resource_data['cross_references'] = cross_references

    if resource.editor:
        editors = []
        for editor in resource_data['editor']:
            del editor['resource_id']
            editors.append(editor)
        resource_data['editors'] = editors
    return resource_data


def show_changesets(db: Session, curie: str):
    """

    :param db:
    :param curie:
    :return:
    """

    resource = db.query(ResourceModel).filter(ResourceModel.curie == curie).first()
    if not resource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Resource with the id {curie} is not available")

    history = []
    for version in resource.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
