from datetime import datetime
from fastapi import HTTPException, status

from agr_literature_service.api.user import get_global_user_id
from agr_literature_service.api.models import UserModel


def add_default_update_keys(schema_dict):
    if "updated_by" not in schema_dict:
        schema_dict["updated_by"] = get_global_user_id()
    if "updated_date" not in schema_dict:
        schema_dict["date_updated"] = datetime.utcnow()


def add_default_create_keys(db, schema_dict):
    if "created_by" not in schema_dict:
        id = get_global_user_id()
        # sanity check for now
        try:
            user = db.query(UserModel).filter(UserModel.user_id == id).first()
            if not user:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"User with id {id} does not exist")
        except BaseException:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"User with id {id} does not exist")
        schema_dict["created_by"] = get_global_user_id()
    if "created_date" not in schema_dict:
        schema_dict["date_created"] = datetime.utcnow()
