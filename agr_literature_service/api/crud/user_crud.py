from sqlalchemy.orm import Session
from typing import Optional

from agr_literature_service.api.models.user_model import UserModel


def create(db: Session, user_id: str, user_email: Optional[str]):
    user_obj = UserModel(id=user_id, email=user_email)
    db.add(user_obj)
    db.commit()
    db.refresh(user_obj)

    return user_obj
