from typing import Optional
# from pydantic import BaseModel
from fastapi_okta import OktaUser

from sqlalchemy.orm import Session

from agr_literature_service.api.crud import user_crud
from agr_literature_service.api.models.user_model import UserModel

user_id: Optional[str] = None


def set_global_user_id(db: Session, id: str):
    """
    :param db:
    :param id:
    :return:
    """

    global user_id
    user_id = id

    if not db.query(UserModel).filter(UserModel.id == user_id).first():
        user_crud.create(db, user_id, None)


def set_global_user_from_okta(db: Session, user: OktaUser):
    """

    :param db:
    :param user:
    :return:
    """

    global user_id
    user_id = user.id
    user_email = None
    if user.email != user.id and '@' in user.email:
        user_email = user.email

    x = db.query(UserModel).filter_by(id=user_id).one_or_none()
    if x is None:
        user_crud.create(db, user_id, user_email)
        # should we delete old entry with email in the ID column for this okta user?
    elif x.email != user_email:
        x.email = user_email
        db.add(x)
        db.commit()
        db.refresh(x)


def get_global_user_id():
    """

    :return:
    """

    return user_id
