from sqlalchemy.orm import Session

from agr_literature_service.api.models.user_model import UserModel


def create(db: Session, user_id: str):
    user_obj = UserModel(id=user_id)
    db.add(user_obj)
    db.commit()
    db.refresh(user_obj)

    return user_obj
