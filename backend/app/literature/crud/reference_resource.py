from sqlalchemy.orm import Session
from literature.models import (
    ReferenceModel, ResourceModel
)
from fastapi import (
    HTTPException, status
)
from typing import Any


def stripout(db: Session, file_update: dict) -> dict:
    data_object = {'resource': None,
                   'reference': None}

    resource_curie = None
    if 'resource_curie' in file_update:
        resource_curie = file_update['resource_curie']
        del file_update['resource_curie']

    reference_curie = None
    if 'reference_curie' in file_update:
        reference_curie = file_update['reference_curie']
        del file_update['reference_curie']
    if resource_curie and reference_curie:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Only supply either resource_curie or reference_curie")
    elif resource_curie:
        data_object['resource'] = db.query(ResourceModel).filter(ResourceModel.curie == resource_curie).first()
        if not data_object['resource']:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Resource with curie {resource_curie} does not exist")
    elif reference_curie:
        data_object['reference'] = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
        if not data_object['reference']:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Reference with curie {reference_curie} does not exist")
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Supply one of resource_curie or reference_curie")

    return data_object


def add(ref_res_obj: dict, data_object: Any) -> None:
    """Lookup reference or resource and add to data_object.

    NOTE: The keys for these will be removed from file_update.
    """
    if 'resource' in ref_res_obj:
        data_object.resource = ref_res_obj['resource']
    else:
        data_object.reference = ref_res_obj['reference']


def create_obj(db: Session, obj_type: Any, obj_data):
    res_ref = stripout(db, obj_data)
    db_obj = obj_type(**obj_data)
    add(res_ref, db_obj)
    return db_obj


def add_reference_resource(db: Session, db_obj: Any, obj_data: dict) -> None:
    res_ref = stripout(db, obj_data)
    add(res_ref, db_obj)
