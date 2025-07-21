from typing import Optional
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api.crud import user_crud
from agr_literature_service.api.models.user_model import UserModel

# still Optional here, since we may not have set it yet
user_id: Optional[str] = None


def set_global_user_id(db: Session, id: str):
    """
    Manually set the global user_id (e.g. from a path parameter).
    """
    global user_id
    user_id = id
    add_user_if_not_exists(db, id)


def add_user_if_not_exists(db: Session, user_id: str):
    """
    Create the user record if it doesn't already exist.
    """
    if not db.query(UserModel).filter(UserModel.id == user_id).first():
        user_crud.create(db, user_id)


def set_global_user_from_okta(db: Session, user: OktaUser):
    """
    Pull the user ID/email from Okta and ensure our DB has a matching UserModel.
    """
    global user_id
    # pick a concrete string ID
    uid: str = user.uid if user.uid else user.cid
    user_id = uid

    # only treat this as an “email” if it’s different from the uid and looks like one
    user_email: Optional[str] = None
    if user.email and user.email != uid and "@" in user.email:
        user_email = user.email

    existing = db.query(UserModel).filter_by(id=uid).one_or_none()
    if existing is None:
        # now `uid` is definitely a str, never Optional[str]
        if user_email is not None:
            # pass both uid and email (email is narrowed to str here)
            user_crud.create(db, uid, user_email)
        else:
            # only pass the uid
            user_crud.create(db, uid)
    elif existing.email != user_email:
        existing.email = user_email
        db.add(existing)
        db.commit()
        db.refresh(existing)


def get_global_user_id() -> Optional[str]:
    return user_id
