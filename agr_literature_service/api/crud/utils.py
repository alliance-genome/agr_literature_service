from datetime import datetime
from fastapi import HTTPException, status

from agr_literature_service.api.user import get_global_user_id
from agr_literature_service.api.models import UserModel


def add_default_update_keys(db, schema_dict):
    if "updated_by" not in schema_dict or not schema_dict["updated_by"]:
        id = get_global_user_id()
        if not id:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"User with id {id} does not exist")
        # sanity check for now
        try:
            user = db.query(UserModel).filter(UserModel.id == id).first()
            if not user:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"User with id {id} does not exist")
        except BaseException:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"User with id {id} does not exist")
        schema_dict["updated_by"] = id

    if "date_updated" not in schema_dict or not schema_dict["date_updated"]:
        schema_dict["date_updated"] = datetime.utcnow()


def add_default_create_keys(db, schema_dict):
    if "created_by" not in schema_dict or not schema_dict["created_by"] :
        id = get_global_user_id()
        if not id:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"User with id {id} does not exist")
        # sanity check for now
        try:
            user = db.query(UserModel).filter(UserModel.id == id).first()
            if not user:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"User with id {id} does not exist")
        except BaseException:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"User with id {id} does not exist")

        schema_dict["created_by"] = id

    if "date_created" not in schema_dict or not schema_dict["date_created"]:
        schema_dict["date_created"] = datetime.utcnow()
