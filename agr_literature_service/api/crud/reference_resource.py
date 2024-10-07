"""
reference_resource.py
=====================
"""

from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferenceModel, ResourceModel


def stripout(db: Session, file_update: dict, non_fatal: bool = False) -> dict:
    """
    Lookup reference or resource and add to data_object.
    :param db:
    :param file_update:
    :param non_fatal:
    :return:
    """

    data_object = {"resource": None,
                   "reference": None}

    resource_curie = None
    if "resource_curie" in file_update:
        resource_curie = file_update["resource_curie"]
        del file_update["resource_curie"]

    reference_curie = None
    if "reference_curie" in file_update:
        reference_curie = file_update["reference_curie"]
        del file_update["reference_curie"]
    if resource_curie and reference_curie:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Only supply either resource_curie or reference_curie")
    elif resource_curie:
        data_object["resource"] = db.query(ResourceModel).filter(ResourceModel.curie == resource_curie).first()
        if not data_object["resource"]:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Resource with curie {resource_curie} does not exist")
    elif reference_curie:
        data_object["reference"] = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
        if not data_object["reference"]:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Reference with curie {reference_curie} does not exist")
    else:
        if not non_fatal:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="Supply one of resource_curie or reference_curie")

    return data_object


def add(ref_res_obj: dict, data_object: Any) -> None:
    """
    Lookup reference or resource and add to data_object.

    NOTE: The keys for these will be removed from file_update.

    :param ref_res_obj:
    :param data_object:
    :return:
    """

    if ref_res_obj["resource"]:
        data_object.resource = ref_res_obj["resource"]
    elif ref_res_obj["reference"]:
        data_object.reference = ref_res_obj["reference"]


def create_obj(db: Session, obj_type: Any, obj_data, non_fatal=False):
    """
    Create a new object of type obj_type.
    :param db:
    :param obj_type:
    :param obj_data:
    :param non_fatal:
    :return:
    """

    res_ref = stripout(db, obj_data, non_fatal)
    db_obj = obj_type(**obj_data)
    add(res_ref, db_obj)
    db.add(db_obj)
    return db_obj


def add_reference_resource(db: Session, db_obj: Any, obj_data: dict, non_fatal: bool = False) -> None:
    """
    Add a reference or resource to a literature object.
    :param db:
    :param db_obj:
    :param obj_data:
    :param non_fatal:
    :return:
    """

    res_ref = stripout(db, obj_data, non_fatal)
    add(res_ref, db_obj)
