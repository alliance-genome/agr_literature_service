from datetime import datetime

from agr_literature_service.api.user import get_global_user_id


def add_default_update_keys(schema_dict):
    if "updated_by" not in schema_dict:
        schema_dict["updated_by"] = get_global_user_id()
    if "updated_date" not in schema_dict:
        schema_dict["date_updated"] = datetime.utcnow()


def add_default_create_keys(schema_dict):
    if "created_by" not in schema_dict:
        schema_dict["created_by"] = get_global_user_id()
    if "created_date" not in schema_dict:
        schema_dict["date_created"] = datetime.utcnow()
