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

    add_user_if_not_exists(db, user_id)


def add_user_if_not_exists(db: Session, user_id: str):
    if not db.query(UserModel).filter(UserModel.id == user_id).first():
        user_crud.create(db, user_id)


def set_global_user_from_okta(db: Session, user: OktaUser):
    """

    :param db:
    :param user:
    :return:
    """

    global user_id
    user_id = user.cid
    if user.uid:
        user_id = user.uid
    user_email = None
    if user.email != user_id and '@' in user.email:
        user_email = user.email

    x = db.query(UserModel).filter_by(id=user_id).one_or_none()
    if x is None:
        if user_email is not None:
            user_crud.create(db, user_id, user_email)
        else:
            user_crud.create(db, user_id)
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
